#!/usr/bin/env python3
"""
state_opcode_breakdown.py

Opcode-level gas breakdown analysis for transactions executed on different blockchain states.

Analyzes how transaction gas consumption changes when executed on states at different lookback points.
Uses debug_traceCall with structLogs to extract per-opcode gas consumption.

Lookback N means: execute on state from (original_block - 1 - N)
Default lookback points: [0, 5, 10, 20]
"""

import sys
import os
import json
import re
import time
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import csv
from dotenv import load_dotenv

load_dotenv()

# Resolve sibling package path at import time so the script works regardless of CWD.
_OPCODE_BREAKDOWN_DIR = Path(__file__).resolve().parent.parent / 'opcode_breakdown'
if str(_OPCODE_BREAKDOWN_DIR) not in sys.path:
    sys.path.insert(0, str(_OPCODE_BREAKDOWN_DIR))

_UTILS_DIR = Path(__file__).resolve().parent.parent / 'utils'
if str(_UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(_UTILS_DIR))

# The one shared call-object builder. Copies type/chainId/nonce/accessList/
# authorizationList faithfully — omitting authorizationList made every type-4
# (EIP-7702) replay execute without its delegation (wrong gas AND wrong slots).
from tx_call_object import build_call_object

from op_code_breakdown import (
    BlockNotWritable,
    calculate_intrinsic_gas,
    calculate_access_list_gas,
    calculate_authorization_list_gas,
    eip3860_initcode_cost_for_creation_tx,
    get_session,
    CHAIN_CONFIGS,
    _find_return_index,
    _sum_direct_children_frame_net,
    _sum_non_call_opcodes_in_frame,
    extract_refund_from_trace,
    apply_refund_cap_eip3529,
    is_precompile,
    CALL_FAM,
    ACCOUNT_ACCESS_OPS,
    STORAGE_ACCESS_OPS,
    COLD_ACCOUNT_ACCESS_COST,
    COLD_SLOAD_COST,
    _effective_op_gas
)


# ──────────────────────────────────────────────────────────────────────────────
# Helper functions (avoiding circular imports)
# ──────────────────────────────────────────────────────────────────────────────

# This pipeline reads its own rpc_config.json (per the top-level README each
# pipeline directory carries one); the imported CHAIN_CONFIGS from
# opcode_breakdown/ is only a fallback for chains not configured here.
_LOCAL_RPC_CONFIG_PATH = Path(__file__).resolve().parent / 'rpc_config.json'
try:
    with open(_LOCAL_RPC_CONFIG_PATH) as _f:
        _LOCAL_CHAIN_CONFIGS = json.load(_f)
except (FileNotFoundError, json.JSONDecodeError):
    _LOCAL_CHAIN_CONFIGS = {}


def get_rpc_url(chain: str) -> str:
    """Return the tracing RPC URL for a chain, preferring this directory's rpc_config.json."""
    chain_lower = chain.lower()
    cfg = _LOCAL_CHAIN_CONFIGS.get(chain_lower) or CHAIN_CONFIGS.get(chain_lower)
    if cfg:
        return cfg.get('rpc_tracing') or cfg['rpc_url']
    raise ValueError(
        f"No RPC URL configured for chain '{chain}'. "
        f"Add it to {_LOCAL_RPC_CONFIG_PATH}.")


TERMINAL_RPC_SUBSTRINGS = (
    "less than block base fee",
    "fee cap less than",
    "insufficient funds",
    "nonce too low",
    "nonce too high",
    "intrinsic gas too low",
    "exceeds block gas limit",
    "gas limit reached",
)


def is_terminal_trace_rejection(success, gas_used, error, structlogs) -> bool:
    if success or gas_used is not None or structlogs:
        return False
    message = str(error or "").lower()
    return any(part in message for part in TERMINAL_RPC_SUBSTRINGS)


def get_transaction(tx_hash: str, rpc_url: str) -> Optional[Dict]:
    """Fetch transaction details"""
    session = get_session()
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_hash],
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=(30, 120))
        result = response.json()
        return result.get("result")
    except Exception as e:
        print(f"Warning: eth_getTransactionByHash failed for {tx_hash}: {e}")
        return None


def get_block_transactions(block_number: int, rpc_url: str) -> List[str]:
    """Get all transaction hashes from a block"""
    session = get_session()
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_number), False],  # False = only tx hashes
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=(30, 120))
        result = response.json()
        block = result.get("result")
        if block and "transactions" in block:
            return block["transactions"]
        return []
    except Exception as e:
        print(f"Warning: eth_getBlockByNumber failed for block {block_number}: {e}")
        return []


def decode_revert_reason(hex_data: str) -> str:
    """
    Decode a hex-encoded revert reason.

    Error signatures:
    - 0x08c379a0 = Error(string)
    - Others may be custom errors
    """
    if not hex_data or not isinstance(hex_data, str):
        return str(hex_data) if hex_data else ""

    # Plain text errors (not hex) pass through unchanged
    if not hex_data.startswith("0x") and not re.fullmatch(r'[0-9a-fA-F]+', hex_data):
        return hex_data

    if hex_data.startswith("0x"):
        hex_data = hex_data[2:]

    # Check if it's an Error(string) - signature 0x08c379a0
    if hex_data.startswith("08c379a0"):
        try:
            length_hex = hex_data[8+64:8+64+64]
            length = int(length_hex, 16)
            string_hex = hex_data[8+64+64:8+64+64+length*2]
            decoded = bytes.fromhex(string_hex).decode('utf-8', errors='ignore')
            return decoded
        except:
            return hex_data[:20] + "..."

    # For other errors, return as hex (truncated if too long)
    if len(hex_data) > 20:
        return "0x" + hex_data[:20] + "..."
    return "0x" + hex_data if hex_data else ""


def trace_transaction_at_block_with_opcodes_cached(
    tx_hash: str,
    state_block: int,
    rpc_url: str,
    cached_tx: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Optional[int], Optional[str], Optional[List[Dict[str, Any]]]]:
    """
    Trace a transaction at a specific block state WITH opcode-level details (with caching).

    Uses debug_traceCall with structLogs enabled to capture opcode execution.

    Args:
        tx_hash: Transaction hash
        state_block: Block number for state
        rpc_url: RPC endpoint
        cached_tx: Pre-fetched transaction data

    Returns:
        (success, gas_used, error_message, structLogs)
    """
    session = get_session()

    if cached_tx:
        tx = cached_tx
    else:
        tx = get_transaction(tx_hash, rpc_url)

    if not tx:
        return False, None, "Could not fetch transaction", None

    call_obj = build_call_object(tx)

    # Use debug_traceCall at the specified block
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceCall",
        "params": [
            call_obj,
            hex(state_block),
            {
                "disableMemory": True,
                "disableStack": True,
                "disableStorage": True,
            }
        ],
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=(30, 300))
        result = response.json()

        if "error" in result:
            error_msg = result["error"].get("message", str(result["error"]))
            return False, None, error_msg, None

        if "result" not in result:
            return False, None, "No result in response", None

        trace_result = result["result"]
        gas_used = int(trace_result.get("gas", 0))
        structlogs = trace_result.get("structLogs", [])

        if "failed" in trace_result and trace_result["failed"]:
            error_msg = trace_result.get("returnValue", "execution failed")
            error_msg = decode_revert_reason(error_msg)
            return False, gas_used, error_msg, structlogs

        return True, gas_used, None, structlogs

    except Exception as e:
        return False, None, str(e), None


def trace_transaction_at_block_with_opcodes(
    tx_hash: str,
    state_block: int,
    rpc_url: str
) -> Tuple[bool, Optional[int], Optional[str], Optional[List[Dict]]]:
    """
    Trace a transaction at a specific block state WITH opcode-level details.

    Uses debug_traceCall with structLogs enabled to capture opcode execution.

    Returns:
        (success, gas_used, error_message, structLogs)
        - success: bool indicating if execution succeeded
        - gas_used: total gas used (if available)
        - error_message: error message if failed
        - structLogs: list of opcode execution logs
    """
    return trace_transaction_at_block_with_opcodes_cached(tx_hash, state_block, rpc_url, None)


# ──────────────────────────────────────────────────────────────────────────────
# State diff parsing helpers (ported from op_code_breakdown.py)
# ──────────────────────────────────────────────────────────────────────────────

def _as_hex(v):
    """Convert value to hex string"""
    if v is None:
        return "0x0"
    if isinstance(v, int):
        return hex(v)
    s = str(v)
    return s if s else "0x0"


def _hex_to_int(v) -> int:
    """Convert hex string to int"""
    s = _as_hex(v)
    try:
        return int(s, 16) if s.startswith("0x") else int(s)
    except Exception:
        return 0


def _is_zero(v) -> bool:
    """Check if hex value is zero"""
    return _hex_to_int(v) == 0


def _equal_hex_numeric(a, b) -> bool:
    """Check if two hex values are numerically equal"""
    return _hex_to_int(a) == _hex_to_int(b)


def _norm_pair(obj: dict[str, Any]):
    """
    Normalize state diff pair to (old_value, new_value).
    Handles various formats: {"from": x, "to": y}, {"-": x, "+": y}, {"*": x, "+": y}, etc.
    """
    if not isinstance(obj, dict):
        return ("0x0", "0x0")
    if "from" in obj or "to" in obj:
        return (_as_hex(obj.get("from", "0x0")), _as_hex(obj.get("to", "0x0")))
    if "-" in obj or "+" in obj:
        oldv = obj.get("-", obj.get("*", "0x0"))
        newv = obj.get("+", "0x0")
        return (_as_hex(oldv), _as_hex(newv))
    if "*" in obj:
        star = obj["*"]
        if isinstance(star, dict) and ("from" in star or "to" in star):
            return (_as_hex(star.get("from", "0x0")), _as_hex(star.get("to", "0x0")))
        return (_as_hex(star), _as_hex(obj.get("+", "0x0")))
    if "+" in obj:
        return ("0x0", _as_hex(obj["+"]))
    return ("0x0", "0x0")


def _parse_prestate_diff(prestate_diff: dict[str, Any]) -> Dict[str, Any]:
    """
    Parse prestateTracer result to extract storage and account changes.

    Handles two formats:
    1. {pre: {...}, post: {...}} format (from diffMode: true)
    2. Direct diff format with {from, to} pairs (from other tracers)

    Returns dict with:
        - zero_to_nonzero: Storage slots created (0 → non-zero)
        - nonzero_to_zero: Storage slots deleted (non-zero → 0)
        - nonzero_to_nonzero: Storage slots updated (non-zero → different non-zero)
        - net_slots_written: Net storage slot growth
        - accounts_created: Number of accounts created
        - accounts_deleted: Number of accounts deleted (SELFDESTRUCT)
        - bytecode_bytes_allocated: Bytes of bytecode created
        - bytecode_bytes_freed: Bytes of bytecode freed
        - net_bytecode_bytes: Net bytecode size change
        - storage_by_address: Per-address storage changes
    """
    out = {
        'zero_to_nonzero': 0,
        'nonzero_to_zero': 0,
        'nonzero_to_nonzero': 0,
        'net_slots_written': 0,
        'accounts_created': 0,
        'accounts_deleted': 0,
        'bytecode_bytes_allocated': 0,
        'bytecode_bytes_freed': 0,
        'net_bytecode_bytes': 0,
        'storage_by_address': {},
    }

    if not isinstance(prestate_diff, dict):
        return out

    # Handle {pre, post} format from diffMode: true
    if 'pre' in prestate_diff and 'post' in prestate_diff:
        pre = prestate_diff['pre']
        post = prestate_diff['post']

        # Get all affected addresses
        all_addrs = set(pre.keys()) | set(post.keys())

        for addr in all_addrs:
            pre_state = pre.get(addr, {})
            post_state = post.get(addr, {})

            a = {'zero_to_nonzero': 0, 'nonzero_to_zero': 0, 'nonzero_to_nonzero': 0}

            # Compare storage slots
            pre_storage = pre_state.get('storage', {})
            post_storage = post_state.get('storage', {})
            all_slots = set(pre_storage.keys()) | set(post_storage.keys())

            for slot in all_slots:
                oldv = pre_storage.get(slot, '0x0')
                newv = post_storage.get(slot, '0x0')
                oz, nz = _is_zero(oldv), _is_zero(newv)

                if oz and not nz:
                    a['zero_to_nonzero'] += 1
                elif (not oz) and nz:
                    a['nonzero_to_zero'] += 1
                elif (not oz) and (not nz) and (not _equal_hex_numeric(oldv, newv)):
                    a['nonzero_to_nonzero'] += 1

            if any(a.values()):
                out['storage_by_address'][addr] = a
                out['zero_to_nonzero'] += a['zero_to_nonzero']
                out['nonzero_to_zero'] += a['nonzero_to_zero']
                out['nonzero_to_nonzero'] += a['nonzero_to_nonzero']

            # Compare bytecode for bytecode allocation/deallocation tracking
            old_code = pre_state.get('code', None)
            new_code = post_state.get('code', None)

            # Track bytecode allocation (contract creation)
            if new_code is not None and not _is_zero(new_code):
                if old_code is None or _is_zero(old_code):
                    if new_code.startswith('0x'):
                        try:
                            code_bytes = bytes.fromhex(new_code[2:])
                            out['bytecode_bytes_allocated'] += len(code_bytes)
                        except:
                            pass

            # Track bytecode deallocation (SELFDESTRUCT)
            if new_code is not None and _is_zero(new_code):
                if old_code is not None and not _is_zero(old_code):
                    if old_code.startswith('0x'):
                        try:
                            destroyed_bytes = len(bytes.fromhex(old_code[2:]))
                            out['bytecode_bytes_freed'] += destroyed_bytes
                        except:
                            pass

            # Account creation: check if code, balance, or nonce went from zero to non-zero
            created = False
            for field in ('code', 'balance', 'nonce'):
                old_val = pre_state.get(field, None)
                new_val = post_state.get(field, None)
                # Only consider if the field is explicitly present in post
                if new_val is not None:
                    # Check if it went from zero/missing to non-zero
                    if (old_val is None or _is_zero(old_val)) and not _is_zero(new_val):
                        created = True
                        break
            if created:
                out['accounts_created'] += 1

            # Account deletion: check if code or balance went from non-zero to zero
            deleted = False
            for field in ('code', 'balance'):
                old_val = pre_state.get(field, None)
                new_val = post_state.get(field, None)
                # Only consider if the field is explicitly present in post
                if new_val is not None:
                    # Check if it went from non-zero to zero
                    if (old_val is not None and not _is_zero(old_val)) and _is_zero(new_val):
                        deleted = True
                        break
            if deleted:
                out['accounts_deleted'] += 1

        out['net_slots_written'] = out['zero_to_nonzero'] - out['nonzero_to_zero']
        out['net_bytecode_bytes'] = out['bytecode_bytes_allocated'] - out['bytecode_bytes_freed']
        return out

    # Handle direct diff format (legacy path for other tracers)
    root = prestate_diff
    for addr, changes in root.items():
        if not isinstance(changes, dict):
            continue

        a = {'zero_to_nonzero': 0, 'nonzero_to_zero': 0, 'nonzero_to_nonzero': 0}
        storage = changes.get('storage') or {}

        if isinstance(storage, dict):
            for slot, raw in storage.items():
                oldv, newv = _norm_pair(raw)
                oz, nz = _is_zero(oldv), _is_zero(newv)
                if oz and not nz:
                    a['zero_to_nonzero'] += 1
                elif (not oz) and nz:
                    a['nonzero_to_zero'] += 1
                elif (not oz) and (not nz) and (not _equal_hex_numeric(oldv, newv)):
                    a['nonzero_to_nonzero'] += 1

        if any(a.values()):
            out['storage_by_address'][addr] = a
            out['zero_to_nonzero'] += a['zero_to_nonzero']
            out['nonzero_to_zero'] += a['nonzero_to_zero']
            out['nonzero_to_nonzero'] += a['nonzero_to_nonzero']

        # Handle bytecode creation/destruction
        code_change = changes.get('code')
        if code_change:
            old_code, new_code = _norm_pair(code_change)
            if (not old_code or old_code == '0x' or old_code == '0x0' or _is_zero(old_code)) and new_code and new_code != '0x' and new_code != '0x0':
                if new_code.startswith('0x'):
                    try:
                        code_bytes = bytes.fromhex(new_code[2:])
                        out['bytecode_bytes_allocated'] += len(code_bytes)
                    except:
                        pass
            if old_code and old_code != '0x' and old_code != '0x0' and (not new_code or new_code == '0x' or new_code == '0x0'):
                if old_code.startswith('0x'):
                    try:
                        destroyed_bytes = len(bytes.fromhex(old_code[2:]))
                        out['bytecode_bytes_freed'] += destroyed_bytes
                    except:
                        pass

        # Account creation detection
        created = False
        for fld in ("code", "balance", "nonce"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if _is_zero(ov) and (not _is_zero(nv)):
                    created = True
                    break
        if created:
            out['accounts_created'] += 1

        # Account deletion detection (SELFDESTRUCT)
        deleted = False
        for fld in ("code", "balance"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if (not _is_zero(ov)) and _is_zero(nv):
                    deleted = True
                    break
        if deleted:
            out['accounts_deleted'] += 1

    out['net_slots_written'] = out['zero_to_nonzero'] - out['nonzero_to_zero']
    out['net_bytecode_bytes'] = out['bytecode_bytes_allocated'] - out['bytecode_bytes_freed']
    return out


def trace_transaction_state_diff_at_block(
    tx_hash: str,
    state_block: int,
    rpc_url: str,
    cached_tx: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Trace a transaction at a specific block state to get state diff (storage/account changes).

    Uses debug_traceCall with prestateTracer to capture before/after state.

    Args:
        tx_hash: Transaction hash
        state_block: Block number for state
        rpc_url: RPC endpoint
        cached_tx: Pre-fetched transaction data

    Returns:
        Dict with storage and account changes, or None if failed
    """
    session = get_session()

    if cached_tx:
        tx = cached_tx
    else:
        tx = get_transaction(tx_hash, rpc_url)

    if not tx:
        return None

    call_obj = build_call_object(tx)

    # Use debug_traceCall with prestateTracer at the specified block
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceCall",
        "params": [
            call_obj,
            hex(state_block),
            {
                "tracer": "prestateTracer",
                "tracerConfig": {
                    "diffMode": True
                }
            }
        ],
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=(30, 300))
        result = response.json()

        if "error" in result:
            print(f"Warning: prestateTracer error for {tx_hash} at block {state_block}: {result['error']}")
            return None

        if "result" not in result:
            print(f"Warning: prestateTracer returned no result for {tx_hash} at block {state_block}")
            return None

        prestate_diff = result["result"]
        return _parse_prestate_diff(prestate_diff)

    except Exception as e:
        print(f"Warning: prestateTracer request failed for {tx_hash} at block {state_block}: {e}")
        return None


def analyze_opcode_breakdown_from_structlogs(
    structlogs: List[Dict[str, Any]],
    tx: Dict[str, Any],
    intrinsic_gas: int,
    gas_used: int = 0,
    tx_success: bool = True,
    debug: bool = False,
    extra_implicit_gas: int = 0,
    authorization_list_gas: int = 0
) -> Dict[str, Any]:
    """
    Analyze opcode-level gas consumption from structLogs using depth-direct mode.

    Depth-direct mode logic (from op_code_breakdown.py):
    - Non-CALL opcodes: sum gasCost at all depths
    - CALL/CREATE: net(frame) - sum(net(direct children)) - non-CALL opcodes in frame
    This gives pure CALL/CREATE overhead without double-counting internal execution.

    Args:
        structlogs: List of opcode execution logs from debug_traceCall
        tx: Original transaction dict (for context)
        intrinsic_gas: Pre-calculated intrinsic gas
        gas_used: Gas used from trace (if available)
        tx_success: Whether transaction succeeded
        debug: Enable debug output

    Returns:
        Dict with opcode breakdown analysis
    """
    per_op_noncall = defaultdict(int)
    per_op_call = defaultdict(int)
    used_root_fallback = False

    # Track access counts (total and cold)
    account_access_counts = defaultdict(int)
    account_cold_counts = defaultdict(int)
    storage_cold_counts = defaultdict(int)

    # For trace calls, we use gas_used as the actual gas used
    actual_gas_used = gas_used
    gas_limit = _hex_to_int(tx.get('gas', 0))
    is_out_of_gas = (actual_gas_used == gas_limit) and not tx_success

    # Handle empty structlogs (EOA transfers, precompile calls, immediate failures)
    if not structlogs or len(structlogs) == 0:
        # Subtract extra_implicit_gas (eip3860 initcode + contract_creation): it's part of
        # actual_gas_used but accounted separately. Without this, an immediate failed
        # creation (empty trace, OOG) double-counts the initcode gas here too.
        execution_gas = max(0, actual_gas_used - intrinsic_gas - extra_implicit_gas)

        # Check if this is a precompile call
        to_address = tx.get('to')
        precompile_name = is_precompile(to_address) if to_address else None

        tx_failed = not tx_success
        if execution_gas > 0:
            if is_out_of_gas:
                per_op_noncall['OUT_OF_GAS'] = execution_gas
                non_call_gas = execution_gas
                call_overhead = 0
            elif precompile_name:
                per_op_call[f'PRECOMPILE_{precompile_name}'] = execution_gas
                non_call_gas = 0
                call_overhead = execution_gas
            elif tx_failed:
                # Preserve otherwise unattributable gas from a failed empty trace.
                per_op_noncall['EXCEPTIONAL_HALT_IMPLICIT'] = execution_gas
                non_call_gas = execution_gas
                call_overhead = 0
            else:
                non_call_gas = 0
                call_overhead = 0
            used_root_fallback = True
        else:
            non_call_gas = 0
            call_overhead = 0
        create_overhead = 0
        exec_total = non_call_gas + call_overhead

        # Simplified result for empty structlogs
        return {
            'per_op_noncall': dict(per_op_noncall),
            'per_op_call': dict(per_op_call),
            'total_opcode_gas': exec_total,
            'uncapped_refund': None if authorization_list_gas else 0,
            'refunds_effective': None if authorization_list_gas else 0,
            'net_gas': None if authorization_list_gas else intrinsic_gas + exec_total,
            'storage_reads': 0,
            'storage_writes': 0,
            'account_accesses': {},
            'account_cold_accesses': {},
            'storage_cold_accesses': {},
            'opcode_count': 0,
            'used_root_fallback': used_root_fallback
        }

    # ──────────────── DEPTH_DIRECT MODE ────────────────
    n = len(structlogs)

    # First: count all non-CALL opcodes at all depths + track accesses
    storage_reads = 0
    storage_writes = 0

    for i, log in enumerate(structlogs):
        op = (log.get('op') or '').upper()
        if op not in CALL_FAM:
            try:
                per_op_noncall[op] += _effective_op_gas(log)
            except Exception:
                pass

        # Track account accesses
        if op in ACCOUNT_ACCESS_OPS:
            account_access_counts[op] += 1
            gas_cost = int(log.get('gasCost', 0) or 0)
            # For CALL-family ops gasCost includes forwarded gas, so this exact
            # match rarely fires: CALL-family cold counts are a lower bound.
            if gas_cost == COLD_ACCOUNT_ACCESS_COST:
                account_cold_counts[op] += 1

        # Track storage accesses
        if op in STORAGE_ACCESS_OPS:
            gas_cost = int(log.get('gasCost', 0) or 0)

            if op == 'SLOAD':
                storage_reads += 1
                if gas_cost == COLD_SLOAD_COST:  # 2100
                    storage_cold_counts[op] += 1
            elif op == 'SSTORE':
                storage_writes += 1
                # Post-Berlin cold SSTORE gasCosts are exactly 2200 (no-op),
                # 5000 (reset) or 22100 (set); warm costs are 100/2900/20000.
                if gas_cost in (2200, 5000, 22100):
                    storage_cold_counts[op] += 1

    # Second: For each CALL/CREATE at any depth, calculate overhead
    # CALL overhead = net(frame) - net(direct children) - non-CALL opcodes at child depth
    for i, log in enumerate(structlogs):
        op = (log.get('op') or '').upper()
        if op not in CALL_FAM:
            continue
        depth = int(log.get('depth', 0) or 0)
        g_before = int(log.get('gas', 0) or 0)

        j = _find_return_index(structlogs, i, depth)
        g_after = int(structlogs[j].get('gas', 0) or 0) if j != -1 else int(structlogs[-1].get('gas', 0) or 0)
        parent_net = max(0, g_before - g_after)

        # Sum net of direct child CALL/CREATE frames
        child_frames_sum = _sum_direct_children_frame_net(structlogs, i, j if j != -1 else n, depth)

        # Sum gasCost of non-CALL opcodes inside this frame (excluding child frames)
        non_call_in_frame = _sum_non_call_opcodes_in_frame(structlogs, i, j if j != -1 else n, depth)

        # CALL overhead = total frame - child frames - non-call opcodes
        self_only = max(0, parent_net - child_frames_sum - non_call_in_frame)

        per_op_call[op] += self_only

        if debug:
            print(f"  {op}@{i} d={depth}: net={parent_net:,} children={child_frames_sum:,} non_call={non_call_in_frame:,} self={self_only:,}")

    non_call_gas = sum(per_op_noncall.values())
    call_overhead = sum(v for k, v in per_op_call.items() if k in {'CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL'})
    create_overhead = sum(v for k, v in per_op_call.items() if k in {'CREATE', 'CREATE2'})
    exec_total = non_call_gas + call_overhead + create_overhead

    # ─────────── Calculate refunds ───────────
    # Create minimal receipt dict for refund extraction
    # extract_refund_from_trace returns 0 for failed txs, so refund_uncapped is the
    # traced (execution/SSTORE) refund on success and 0 on revert — matching the
    # reference's effective_refund_pre_cap (zeroed on failure).
    minimal_receipt = {'status': 1 if tx_success else 0}
    refund_uncapped = extract_refund_from_trace(structlogs, minimal_receipt)

    # Authorization refunds are protocol-level and absent from opcode traces.
    # Do not infer them from gas_used; type-4 difference retains that residual.
    refund = apply_refund_cap_eip3529(refund_uncapped, intrinsic_gas + exec_total)

    # Handle implicit costs for REVERT/RETURN/STOP/SELFDESTRUCT
    if structlogs and actual_gas_used > 0:
        last_op = structlogs[-1].get('op', '').upper()
        tx_failed = not tx_success

        is_terminal_op = last_op in ('REVERT', 'RETURN', 'STOP', 'SELFDESTRUCT')
        is_exceptional_halt = tx_failed and not is_terminal_op

        if is_terminal_op or is_exceptional_halt:
            # +extra_implicit_gas: eip3860/contract_creation gas is in actual_gas_used but
            # accounted separately; exclude it from the implicit-cost residual so it is not
            # double-counted.
            expected_gas = intrinsic_gas + exec_total - refund + extra_implicit_gas
            implicit_cost = max(0, actual_gas_used - expected_gas)

            is_contract_creation = (tx.get('to') is None)

            if implicit_cost > 0 and (not is_contract_creation or tx_failed):
                op_label = last_op if is_terminal_op else 'EXCEPTIONAL_HALT'
                per_op_noncall[f'{op_label}_IMPLICIT'] = implicit_cost
                non_call_gas += implicit_cost
                exec_total += implicit_cost

                if debug:
                    print(f"     {op_label} implicit cost:  {implicit_cost:,}")

    return {
        'per_op_noncall': dict(per_op_noncall),
        'per_op_call': dict(per_op_call),
        'total_opcode_gas': exec_total,
        # Lookback replay lacks the authority state needed for type-4 refunds.
        # Store NULL so consumers cannot mistake an unknown refund for zero.
        'uncapped_refund': None if authorization_list_gas else refund_uncapped,
        'refunds_effective': None if authorization_list_gas else refund,
        'net_gas': None if authorization_list_gas else intrinsic_gas + exec_total - refund,
        'storage_reads': storage_reads,
        'storage_writes': storage_writes,
        'account_accesses': dict(account_access_counts),
        'account_cold_accesses': dict(account_cold_counts),
        'storage_cold_accesses': dict(storage_cold_counts),
        'opcode_count': len(structlogs),
        'used_root_fallback': used_root_fallback
    }



def _zero_state_changes() -> Dict[str, int]:
    return {
        'zero_to_nonzero': 0,
        'nonzero_to_zero': 0,
        'nonzero_to_nonzero': 0,
        'net_slots_written': 0,
        'accounts_created': 0,
        'accounts_deleted': 0,
        'bytecode_bytes_allocated': 0,
        'bytecode_bytes_freed': 0,
        'net_bytecode_bytes': 0,
    }


def _result_base(tx_hash, original_block, state_block, success, error, gas_used,
                 intrinsic_gas_total, calldata_zero_gas, calldata_nonzero_gas,
                 creation_gas, access_list_gas, authorization_list_gas,
                 eip3860_init_gas, state_changes) -> Dict[str, Any]:
    """The COMMON base row every result path starts from — empty-trace or full.

    Every result row must contain all schema fields; full paths update() this dict.
    """
    return {
        'tx_hash': tx_hash,
        'original_block': original_block,
        'state_block': state_block,
        'lookback': original_block - state_block - 1,
        'success': success,
        'error': error or '',
        'gas_used': gas_used,
        'intrinsic_gas': intrinsic_gas_total,
        'calldata_zero_gas': calldata_zero_gas,
        'calldata_nonzero_gas': calldata_nonzero_gas,
        'creation_gas': creation_gas,
        'access_list_gas': access_list_gas,
        'authorization_list_gas': authorization_list_gas,
        'eip3860_init_gas': eip3860_init_gas,
        'total_opcode_gas': 0,
        'opcode_count': 0,
        'storage_slots_modified': (state_changes['zero_to_nonzero']
                                   + state_changes['nonzero_to_zero']
                                   + state_changes['nonzero_to_nonzero']),
        'storage_slots_created': state_changes['zero_to_nonzero'],
        'storage_slots_deleted': state_changes['nonzero_to_zero'],
        'storage_slots_updated': state_changes['nonzero_to_nonzero'],
        'net_storage_slots_written': state_changes['net_slots_written'],
        'accounts_created': state_changes['accounts_created'],
        'accounts_deleted': state_changes['accounts_deleted'],
        'bytecode_bytes_allocated': state_changes['bytecode_bytes_allocated'],
        'bytecode_bytes_freed': state_changes['bytecode_bytes_freed'],
        'net_bytecode_bytes': state_changes['net_bytecode_bytes'],
        'has_opcode_breakdown': False,
    }


def _opcode_fields(opcode_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """The opcode-derived half of a full result row. Paired with _result_base so
    every full-trace path (single, cached-batch, worker-batch) emits an
    IDENTICAL schema."""
    return {
        'total_opcode_gas': opcode_analysis['total_opcode_gas'],
        'uncapped_refund': opcode_analysis['uncapped_refund'],
        'refunds_effective': opcode_analysis['refunds_effective'],
        'net_gas': opcode_analysis['net_gas'],
        'opcode_count': opcode_analysis['opcode_count'],
        'storage_reads': opcode_analysis['storage_reads'],
        'storage_writes': opcode_analysis['storage_writes'],
        'per_op_noncall': opcode_analysis['per_op_noncall'],
        'per_op_call': opcode_analysis['per_op_call'],
        'account_accesses': opcode_analysis['account_accesses'],
        'account_cold_accesses': opcode_analysis.get('account_cold_accesses', {}),
        'storage_cold_accesses': opcode_analysis.get('storage_cold_accesses', {}),
        'has_opcode_breakdown': True,
    }


def analyze_transaction_opcode_breakdown_on_state(
    tx_hash: str,
    original_block: int,
    state_block: int,
    rpc_url: str,
    debug: bool = False,
    state_changes_override: Optional[Dict[str, int]] = None
) -> Optional[Dict]:
    """
    Get detailed opcode breakdown for a transaction executed on a specific state.

    Args:
        tx_hash: Transaction hash to analyze
        original_block: Original block where transaction was included
        state_block: Block whose state to use for execution
        rpc_url: RPC endpoint
        state_changes_override: pre-computed _parse_prestate_diff aggregates for
            this state. When provided, the internal prestateTracer call is
            SKIPPED (callers that already ran a diffMode trace for slot rows
            reuse it instead of paying a duplicate RPC trace).

    Returns:
        Dict with complete analysis including intrinsic gas, opcode breakdown, etc.
    """
    tx = get_transaction(tx_hash, rpc_url)
    if not tx:
        return None

    intrinsic_gas, calldata_zero_gas, calldata_nonzero_gas, creation_gas = calculate_intrinsic_gas(tx)
    access_list_gas, _, _, _ = calculate_access_list_gas(tx)
    authorization_list_gas, _, _ = calculate_authorization_list_gas(tx)
    eip3860_init_gas = eip3860_initcode_cost_for_creation_tx(tx)

    intrinsic_gas_total = intrinsic_gas + access_list_gas + authorization_list_gas

    success, gas_used, error, structlogs = trace_transaction_at_block_with_opcodes(
        tx_hash, state_block, rpc_url
    )

    if state_changes_override is not None:
        state_changes = state_changes_override
    else:
        state_changes = trace_transaction_state_diff_at_block(
            tx_hash, state_block, rpc_url, cached_tx=tx
        )

    if state_changes is None:
        if is_terminal_trace_rejection(success, gas_used, error, structlogs):
            state_changes = _zero_state_changes()
        else:
            raise BlockNotWritable(
                f"{tx_hash}: state diff unavailable at state block {state_block}")

    base = _result_base(
        tx_hash, original_block, state_block, success, error, gas_used,
        intrinsic_gas_total, calldata_zero_gas, calldata_nonzero_gas,
        creation_gas, access_list_gas, authorization_list_gas,
        eip3860_init_gas, state_changes)

    if not structlogs:
        # Empty traces still carry their own gas and the complete base schema.
        return base

    opcode_analysis = analyze_opcode_breakdown_from_structlogs(
        structlogs, tx, intrinsic_gas_total,
        gas_used=gas_used,
        tx_success=success,
        debug=debug,
        extra_implicit_gas=eip3860_init_gas,
        authorization_list_gas=authorization_list_gas
    )

    result = base
    result.update(_opcode_fields(opcode_analysis))
    return result


def batch_trace_at_multiple_states(
    tx_hash: str,
    state_blocks: List[int],
    rpc_url: str,
    cached_tx: Optional[Dict[str, Any]] = None
) -> Dict[int, Tuple[bool, Optional[int], Optional[str], Optional[List[Dict[str, Any]]]]]:
    """
    Batch trace a transaction at multiple state blocks in one RPC call.

    Args:
        tx_hash: Transaction hash
        state_blocks: List of block numbers for states
        rpc_url: RPC endpoint
        cached_tx: Pre-fetched transaction data

    Returns:
        Dict mapping state_block -> (success, gas_used, error, structlogs)
    """
    session = get_session()

    if cached_tx:
        tx = cached_tx
    else:
        tx = get_transaction(tx_hash, rpc_url)

    if not tx:
        return {}

    call_obj = build_call_object(tx)

    # Build batch request for all state blocks
    batch_payload = []
    for i, state_block in enumerate(state_blocks):
        batch_payload.append({
            "jsonrpc": "2.0",
            "method": "debug_traceCall",
            "params": [
                call_obj,
                hex(state_block),
                {
                    "disableMemory": True,
                    "disableStack": True,
                    "disableStorage": True,
                }
            ],
            "id": i
        })

    id_to_state_block = {i: state_block for i, state_block in enumerate(state_blocks)}

    try:
        response = session.post(rpc_url, json=batch_payload, timeout=(240, 900))
        results = response.json()

        # Match responses to requests by JSON-RPC id (order is not guaranteed)
        traces = {}
        if isinstance(results, list):
            for result in results:
                state_block = id_to_state_block.get(result.get("id")) if isinstance(result, dict) else None
                if state_block is None:
                    print(f"Warning: batch trace response with unknown id for {tx_hash}: {str(result)[:200]}")
                    continue

                if "error" in result:
                    error_msg = result["error"].get("message", str(result["error"]))
                    traces[state_block] = (False, None, error_msg, None)
                elif "result" in result:
                    trace_result = result["result"]
                    gas_used = int(trace_result.get("gas", 0))
                    structlogs = trace_result.get("structLogs", [])

                    if "failed" in trace_result and trace_result["failed"]:
                        error_msg = trace_result.get("returnValue", "execution failed")
                        error_msg = decode_revert_reason(error_msg)
                        traces[state_block] = (False, gas_used, error_msg, structlogs)
                    else:
                        traces[state_block] = (True, gas_used, None, structlogs)
                else:
                    print(f"Warning: batch trace response for {tx_hash} at block {state_block} "
                          f"has neither result nor error")

        return traces

    except Exception as e:
        print(f"Warning: batch trace request failed for {tx_hash}: {e}")
        return {}


def compare_opcode_breakdown_across_states_cached(
    tx_hash: str,
    original_block: int,
    rpc_url: str,
    lookback_points: List[int] = None,
    quiet: bool = False,
    cached_tx: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Compare opcode breakdown across states with cached transaction data.

    Args:
        tx_hash: Transaction to analyze
        original_block: Original block where transaction was included
        rpc_url: RPC endpoint
        lookback_points: Specific lookback points to test
        quiet: If True, suppress per-lookback output
        cached_tx: Pre-fetched transaction data (avoids RPC call)

    Returns:
        List of analysis results, one per lookback level
    """
    if lookback_points is None:
        lookback_points = [0, 5, 10, 20]

    # Calculate state blocks for all lookback points
    state_blocks = []
    lookback_to_state = {}
    for lookback in lookback_points:
        state_block = original_block - 1 - lookback
        if state_block >= 0:
            state_blocks.append(state_block)
            lookback_to_state[lookback] = state_block

    if not state_blocks:
        return []

    # BATCH: Get traces for all state blocks in one RPC call
    traces = batch_trace_at_multiple_states(tx_hash, state_blocks, rpc_url, cached_tx)
    missing_states = [state_block for state_block in state_blocks
                      if state_block not in traces]
    if missing_states:
        raise BlockNotWritable(
            f"{tx_hash}: missing trace results for state blocks {missing_states}")

    # Get transaction for intrinsic gas calculation
    if cached_tx:
        tx = cached_tx
    else:
        tx = get_transaction(tx_hash, rpc_url)

    if not tx:
        raise BlockNotWritable(f"{tx_hash}: transaction data unavailable")

    # Calculate intrinsic gas (same for all lookbacks)
    intrinsic_gas, calldata_zero_gas, calldata_nonzero_gas, creation_gas = calculate_intrinsic_gas(tx)
    access_list_gas, _, _, _ = calculate_access_list_gas(tx)
    authorization_list_gas, _, _ = calculate_authorization_list_gas(tx)
    eip3860_init_gas = eip3860_initcode_cost_for_creation_tx(tx)
    intrinsic_gas_total = intrinsic_gas + access_list_gas + authorization_list_gas

    # Process results for each lookback
    results = []
    for lookback in lookback_points:
        if lookback not in lookback_to_state:
            continue

        state_block = lookback_to_state[lookback]
        trace_data = traces.get(state_block)

        if not trace_data:
            raise BlockNotWritable(f"{tx_hash}: empty trace result at state block {state_block}")

        success, gas_used, error, structlogs = trace_data
        opcode_analysis = None

        if not structlogs:
            # Empty trace: full base row (all schema fields).
            # This cached path never measures state diffs, so the state-diff
            # columns are zeroed here; its full rows simply omit them.
            result = _result_base(
                tx_hash, original_block, state_block, success, error, gas_used,
                intrinsic_gas_total, calldata_zero_gas, calldata_nonzero_gas,
                creation_gas, access_list_gas, authorization_list_gas,
                eip3860_init_gas, _zero_state_changes())
        else:
            opcode_analysis = analyze_opcode_breakdown_from_structlogs(
                structlogs, tx, intrinsic_gas_total,
                gas_used=gas_used or 0,
                tx_success=success,
                extra_implicit_gas=eip3860_init_gas,
                authorization_list_gas=authorization_list_gas
            )

            # Same base + opcode-fields pair as every other result path.
            # This cached path does not measure state diffs -> zeroed columns.
            result = _result_base(
                tx_hash, original_block, state_block, success, error, gas_used,
                intrinsic_gas_total, calldata_zero_gas, calldata_nonzero_gas,
                creation_gas, access_list_gas, authorization_list_gas,
                eip3860_init_gas, _zero_state_changes())
            result.update(_opcode_fields(opcode_analysis))

        results.append(result)

        if not quiet:
            if not success:
                print(f"  Lookback {lookback}: Failed - {error}")
            elif opcode_analysis is not None:
                print(f"  Lookback {lookback}: Success - {opcode_analysis['total_opcode_gas']:,} gas, {opcode_analysis['opcode_count']:,} opcodes")
            else:
                print(f"  Lookback {lookback}: Success - no opcode trace (empty structLogs)")

    return results


def compare_opcode_breakdown_across_states(
    tx_hash: str,
    original_block: int,
    rpc_url: str,
    lookback_points: List[int] = None,
    quiet: bool = False,
    state_changes_by_lookback: Optional[Dict[int, Dict[str, int]]] = None
) -> List[Dict]:
    """
    Compare opcode breakdown for a transaction across multiple state contexts.

    Args:
        tx_hash: Transaction to analyze
        original_block: Original block where transaction was included
        rpc_url: RPC endpoint
        lookback_points: Specific lookback points to test (default: [0, 5, 10, 20])
        quiet: If True, suppress per-lookback output
        state_changes_by_lookback: optional {lookback: _parse_prestate_diff
            aggregates}. When a lookback has an entry, the engine SKIPS its
            internal prestateTracer call for that state (callers that already
            ran the diff trace for slot rows reuse it — one trace, not two).

    Returns:
        List of analysis results, one per lookback level
    """
    if lookback_points is None:
        lookback_points = [0, 5, 10, 20]

    results = []

    for lookback in lookback_points:
        # State block: lookback 0 = parent block, lookback 1 = grandparent, etc.
        state_block = original_block - 1 - lookback

        if state_block < 0:
            if not quiet:
                print(f"  Skipping lookback {lookback} (state block {state_block} is negative)")
            continue

        if not quiet:
            print(f"  Analyzing lookback {lookback} (state block {state_block})...")

        override = None
        if state_changes_by_lookback is not None:
            override = state_changes_by_lookback.get(lookback)

        result = analyze_transaction_opcode_breakdown_on_state(
            tx_hash, original_block, state_block, rpc_url,
            state_changes_override=override
        )

        if result:
            results.append(result)

            if not quiet:
                if not result['success']:
                    print(f"    Failed: {result['error']}")
                else:
                    print(f"    Success: {result['total_opcode_gas']:,} opcode gas, {result['opcode_count']} opcodes")

    return results


def load_gas_usage_from_csv(csv_path: Path) -> Dict[str, int]:
    """
    Load gas usage from existing opcode breakdown CSV for load balancing.

    Args:
        csv_path: Path to block_*_opcode_gas.csv file

    Returns:
        Dict mapping tx_hash -> gas_used
    """
    gas_map = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tx_hash = row.get('tx_hash', '')
            gas_used = int(row.get('gas_used', 0) or row.get('actual_gas', 0) or 21000)
            if tx_hash:
                gas_map[tx_hash] = gas_used

    return gas_map


def batch_get_transactions(tx_hashes: List[str], rpc_url: str) -> Dict[str, Dict]:
    """
    Batch fetch transaction details for multiple hashes.

    Returns:
        Dict mapping tx_hash -> transaction dict
    """
    session = get_session()

    batch_payload = []
    for i, tx_hash in enumerate(tx_hashes):
        batch_payload.append({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": i
        })

    try:
        response = session.post(rpc_url, json=batch_payload, timeout=(240, 900))
        results = response.json()

        # Match responses to requests by JSON-RPC id (order is not guaranteed)
        by_id = {}
        if isinstance(results, list):
            for result in results:
                if isinstance(result, dict):
                    by_id[result.get("id")] = result

        tx_map = {}
        for i, tx_hash in enumerate(tx_hashes):
            result = by_id.get(i)
            if result is None:
                print(f"Warning: no batch response for transaction {tx_hash}")
            elif "error" in result:
                print(f"Warning: batch transaction fetch failed for {tx_hash}: {result['error']}")
            elif result.get("result"):
                tx_map[tx_hash] = result["result"]
            else:
                print(f"Warning: batch transaction fetch returned no result for {tx_hash}")

        return tx_map
    except Exception as e:
        print(f"Warning: Batch transaction fetch failed: {e}")
        return {}


def analyze_block_opcode_breakdown_on_states(
    block_num: int,
    rpc_url: str,
    lookback_points: List[int] = None,
    max_txs: int = None,
    num_workers: int = 10,
    quiet: bool = False,
    source_csv: str = None
) -> List[Dict]:
    """
    Analyze all transactions in a block at different state lookback points.

    Args:
        block_num: Block number to analyze
        rpc_url: RPC endpoint
        lookback_points: Specific lookback points to test (default: [0, 5, 10, 20])
        max_txs: Maximum transactions to analyze (None = all)
        num_workers: Number of parallel workers
        quiet: If True, only print block-level summary (not per-transaction)
        source_csv: Path to existing block_*_opcode_gas.csv for load balancing

    Returns:
        List of all analysis results for all transactions
    """
    if lookback_points is None:
        lookback_points = [0, 5, 10, 20]

    if not quiet:
        print(f"\n{'='*80}")
        print(f"Processing block {block_num:,}")
        print(f"Lookback points: {lookback_points}")
        print(f"{'='*80}")

    start_time = time.time()

    # Get all transactions in block
    tx_hashes = get_block_transactions(block_num, rpc_url)
    if not quiet:
        print(f"Block has {len(tx_hashes)} transactions")

    # Load gas usage from existing CSV for load balancing
    gas_map = {}
    if source_csv and Path(source_csv).exists():
        gas_map = load_gas_usage_from_csv(Path(source_csv))
        if not quiet and gas_map:
            print(f"  Loaded gas usage for {len(gas_map)} transactions from CSV")

    # Sort transactions by gas usage (descending) for better load balancing
    # Workers pick up big jobs first, then fill in with smaller ones
    if gas_map:
        tx_hashes_sorted = sorted(tx_hashes, key=lambda tx: gas_map.get(tx, 21000), reverse=True)
    else:
        tx_hashes_sorted = tx_hashes

    if max_txs and len(tx_hashes_sorted) > max_txs:
        tx_hashes_sorted = random.sample(tx_hashes_sorted, max_txs)
        if not quiet:
            print(f"Sampling {max_txs} transactions")

    # OPTIMIZATION: Batch fetch all transaction details upfront
    if not quiet:
        print(f"  Batch fetching {len(tx_hashes_sorted)} transaction details...")
    tx_cache = batch_get_transactions(tx_hashes_sorted, rpc_url)
    if not quiet:
        print(f"  Cached {len(tx_cache)} transactions (saved {len(tx_hashes_sorted)} RPC calls)")

    all_results = []
    failed_transactions = []

    # Process transactions in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_tx = {
            executor.submit(
                compare_opcode_breakdown_across_states_cached,
                tx_hash,
                block_num,
                rpc_url,
                lookback_points,
                quiet,
                tx_cache.get(tx_hash)  # Pass cached tx data
            ): tx_hash for tx_hash in tx_hashes_sorted
        }

        # Collect results as they complete
        for i, future in enumerate(as_completed(future_to_tx), 1):
            tx_hash = future_to_tx[future]

            try:
                results = future.result()
                all_results.extend(results)

                if not quiet:
                    if results:
                        success_count = sum(1 for r in results if r['success'])
                        print(f"  [{i}/{len(tx_hashes_sorted)}] {tx_hash[:10]}... "
                              f"{success_count}/{len(results)} lookbacks succeeded")
                    else:
                        print(f"  [{i}/{len(tx_hashes_sorted)}] {tx_hash[:10]}... no results")

                if not results:
                    failed_transactions.append(tx_hash)

            except Exception as e:
                failed_transactions.append(tx_hash)
                if not quiet:
                    print(f"  [{i}/{len(tx_hashes_sorted)}] {tx_hash[:10]}... exception: {e}")

    if failed_transactions:
        raise BlockNotWritable(
            f"block {block_num}: {len(failed_transactions)} transactions failed; output not written")
    elapsed = time.time() - start_time
    # Always print block completion summary
    print(f"Block {block_num:,} complete: {len(all_results)} total results from {len(tx_hashes_sorted)} transactions ({elapsed:.1f}s)")
    return all_results


def get_blocks_from_existing_analysis(
    source_dir: str,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None
) -> List[int]:
    """
    Extract block numbers from existing opcode analysis files.

    Args:
        source_dir: Directory containing block_*_opcode_gas.csv files
        start_block: Optional filter for minimum block number
        end_block: Optional filter for maximum block number

    Returns:
        Sorted list of block numbers
    """
    blocks = []
    pattern = re.compile(r'block_(\d+)_opcode_gas\.csv')

    for filename in os.listdir(source_dir):
        match = pattern.match(filename)
        if match:
            block_num = int(match.group(1))
            # Apply filters
            if start_block is not None and block_num < start_block:
                continue
            if end_block is not None and block_num > end_block:
                continue
            blocks.append(block_num)

    return sorted(blocks)


def process_single_block_worker(
    block_num: int,
    rpc_url: str,
    lookback_points: List[int],
    max_txs: int,
    source_csv: str,
    output_file: Path,
    gas_chunk_size: int = 20_000_000
) -> Tuple[int, int, float]:
    """
    Worker function to process a single block (runs in parallel).

    Chunks transactions by gas usage to avoid overwhelming RPC calls.

    Args:
        block_num: Block number
        rpc_url: RPC endpoint
        lookback_points: Lookback points
        max_txs: Max transactions
        source_csv: Source CSV path for load balancing
        output_file: Output file path
        gas_chunk_size: Target gas per batch (default: 20M)

    Returns:
        (num_results, num_txs, elapsed_time)
    """
    start = time.time()

    # Get all transactions in block
    tx_hashes = get_block_transactions(block_num, rpc_url)

    # Load gas usage from existing CSV
    gas_map = {}
    if source_csv and Path(source_csv).exists():
        gas_map = load_gas_usage_from_csv(Path(source_csv))

    # Apply max_txs limit if specified
    if max_txs and len(tx_hashes) > max_txs:
        tx_hashes = random.sample(tx_hashes, max_txs)

    # Sort by gas usage (descending)
    if gas_map:
        tx_hashes_sorted = sorted(tx_hashes, key=lambda tx: gas_map.get(tx, 21000), reverse=True)
    else:
        tx_hashes_sorted = tx_hashes

    # Chunk transactions by gas usage
    chunks = []
    current_chunk = []
    current_gas = 0

    for tx_hash in tx_hashes_sorted:
        tx_gas = gas_map.get(tx_hash, 21000)

        # If adding this tx would exceed chunk size AND we already have some txs, start new chunk
        if current_gas + tx_gas > gas_chunk_size and current_chunk:
            chunks.append(current_chunk)
            current_chunk = [tx_hash]
            current_gas = tx_gas
        else:
            current_chunk.append(tx_hash)
            current_gas += tx_gas

    if current_chunk:
        chunks.append(current_chunk)

    print(f"Block {block_num}: {len(tx_hashes)} txs split into {len(chunks)} chunks by gas (~{gas_chunk_size/1e6:.1f}M gas each)")

    # Batch fetch ALL transaction details upfront (one call for entire block)
    tx_cache = batch_get_transactions(tx_hashes_sorted, rpc_url)

    # Process each chunk sequentially
    all_results = []
    failed_txs = 0
    for chunk_idx, chunk in enumerate(chunks, 1):
        chunk_gas = sum(gas_map.get(tx, 21000) for tx in chunk)
        print(f"  Block {block_num} chunk {chunk_idx}/{len(chunks)}: {len(chunk)} txs, {chunk_gas/1e6:.1f}M gas")

        # Build batch trace calls for all transactions in this chunk across all lookback points
        chunk_results = []
        for tx_hash in chunk:
            # Calculate state blocks for all lookback points
            state_blocks = []
            for lookback in lookback_points:
                state_block = block_num - 1 - lookback
                if state_block >= 0:
                    state_blocks.append(state_block)

            if not state_blocks:
                continue

            # Get cached transaction
            cached_tx = tx_cache.get(tx_hash)
            if not cached_tx:
                print(f"  Block {block_num}: no transaction data for {tx_hash}, counting as failed")
                failed_txs += 1
                continue

            # Batch trace at all lookback points
            traces = batch_trace_at_multiple_states(tx_hash, state_blocks, rpc_url, cached_tx)
            missing_states = [b for b in state_blocks if b not in traces]
            if missing_states:
                print(f"  Block {block_num}: no trace for {tx_hash} at state block(s) "
                      f"{missing_states}, counting as failed")
                failed_txs += 1
                continue

            # Process each trace result
            for state_block, (success, gas_used, error, structlogs) in traces.items():
                lookback = block_num - state_block - 1

                # Calculate intrinsic gas
                intrinsic_gas, calldata_zero_gas, calldata_nonzero_gas, creation_gas = calculate_intrinsic_gas(cached_tx)
                access_list_gas, _, _, _ = calculate_access_list_gas(cached_tx)
                authorization_list_gas, _, _ = calculate_authorization_list_gas(cached_tx)
                eip3860_init_gas = eip3860_initcode_cost_for_creation_tx(cached_tx)
                intrinsic_gas_total = intrinsic_gas + access_list_gas + authorization_list_gas

                # Deterministic transaction-validation rejections have no
                # state diff by definition; transient tracer failures still fail the block.
                if is_terminal_trace_rejection(success, gas_used, error, structlogs):
                    state_changes = _zero_state_changes()
                else:
                    state_changes = trace_transaction_state_diff_at_block(
                        tx_hash, state_block, rpc_url, cached_tx=cached_tx
                    )
                    if state_changes is None:
                        print(f"  Block {block_num}: no state diff for {tx_hash} at state block "
                              f"{state_block}, counting as failed")
                        failed_txs += 1
                        continue

                if not structlogs:
                    # Empty trace: full base row (all schema fields).
                    result = _result_base(
                        tx_hash, block_num, state_block, success, error,
                        gas_used, intrinsic_gas_total, calldata_zero_gas,
                        calldata_nonzero_gas, creation_gas, access_list_gas,
                        authorization_list_gas, eip3860_init_gas, state_changes)
                else:
                    opcode_analysis = analyze_opcode_breakdown_from_structlogs(
                        structlogs, cached_tx, intrinsic_gas_total,
                        gas_used=gas_used or 0,
                        tx_success=success,
                        extra_implicit_gas=eip3860_init_gas,
                        authorization_list_gas=authorization_list_gas
                    )

                    result = _result_base(
                        tx_hash, block_num, state_block, success, error,
                        gas_used, intrinsic_gas_total, calldata_zero_gas,
                        calldata_nonzero_gas, creation_gas, access_list_gas,
                        authorization_list_gas, eip3860_init_gas, state_changes)
                    result.update(_opcode_fields(opcode_analysis))

                chunk_results.append(result)

        all_results.extend(chunk_results)

    if failed_txs:
        raise BlockNotWritable(
            f"block {block_num}: {failed_txs}/{len(tx_hashes_sorted)} transactions failed to "
            f"analyse; CSV not written so a re-run retries the block")

    if all_results:
        write_opcode_comparison_to_csv(all_results, output_file)
        unique_txs = len(set(r['tx_hash'] for r in all_results))
        elapsed = time.time() - start
        print(f"Block {block_num} complete: {len(all_results)} results from {unique_txs} txs ({elapsed:.1f}s)")
        return len(all_results), unique_txs, elapsed
    else:
        return 0, 0, time.time() - start


def analyze_blocks_from_list(
    block_list: List[int],
    chain: str,
    rpc_url: str,
    output_dir: Path,
    lookback_points: List[int] = None,
    max_txs: int = None,
    num_workers: int = 10,
    quiet: bool = True,
    source_dir: str = None,
    gas_chunk_size: int = 20_000_000,
    executor_kind: str = "threads",
):
    """
    Analyze multiple blocks from a list (PARALLELIZED AT BLOCK LEVEL).

    Each worker processes an entire block, making this much faster for batch processing.
    Transactions within each block are chunked by gas usage to avoid overwhelming RPC calls.

    Args:
        block_list: List of block numbers to analyze
        chain: Chain name
        rpc_url: RPC endpoint
        output_dir: Output directory for results
        lookback_points: Lookback points to test
        max_txs: Max transactions per block
        num_workers: Number of parallel workers (blocks processed simultaneously)
        quiet: Suppress per-transaction output
        source_dir: Source directory with existing block_*_opcode_gas.csv files (for load balancing)
        gas_chunk_size: Target gas per batch within each block (default: 20M)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    if lookback_points is None:
        lookback_points = [0, 5, 10, 20]

    print(f"\n{'='*80}")
    print(f"BATCH ANALYSIS - BLOCK-LEVEL PARALLELISM")
    print(f"{'='*80}")
    print(f"Chain: {chain}")
    print(f"Blocks to analyze: {len(block_list):,}")
    print(f"Lookback points: {lookback_points}")
    print(f"Output directory: {output_dir}")
    print(f"Workers: {num_workers} (blocks processed in parallel)")
    if max_txs:
        print(f"Max txs per block: {max_txs}")
    if source_dir:
        print(f"Source CSV dir: {source_dir}")
    print(f"{'='*80}\n")

    total_results = 0
    total_txs = 0
    total_time = 0
    blocks_processed = 0
    failed_blocks: List[Tuple[int, str]] = []
    start_time = time.time()

    # Filter out already-processed blocks
    blocks_to_process = []
    for block_num in block_list:
        output_file = output_dir / f"block_{block_num}_opcode_breakdown.csv"
        if output_file.exists():
            print(f"Skipping block {block_num:,} (already exists)")
        else:
            blocks_to_process.append(block_num)

    if not blocks_to_process:
        print("All blocks already processed!")
        return

    print(f"\n{len(blocks_to_process):,} blocks to process\n")

    # Process blocks in parallel
    pool_class = ProcessPoolExecutor if executor_kind == "processes" else ThreadPoolExecutor
    with pool_class(max_workers=num_workers) as executor:
        future_to_block = {}
        for block_num in blocks_to_process:
            output_file = output_dir / f"block_{block_num}_opcode_breakdown.csv"

            # Find source CSV
            source_csv = None
            if source_dir:
                source_csv_path = Path(source_dir) / f"block_{block_num}_opcode_gas.csv"
                if source_csv_path.exists():
                    source_csv = str(source_csv_path)

            future = executor.submit(
                process_single_block_worker,
                block_num,
                rpc_url,
                lookback_points,
                max_txs,
                source_csv,
                output_file,
                gas_chunk_size
            )
            future_to_block[future] = block_num

        # Collect results as they complete
        for i, future in enumerate(as_completed(future_to_block), 1):
            block_num = future_to_block[future]

            try:
                num_results, num_txs, elapsed = future.result()
                total_results += num_results
                total_txs += num_txs
                total_time += elapsed
                blocks_processed += 1

                print(f"[{i}/{len(blocks_to_process)}] Block {block_num:,}: "
                      f"{num_results} results, {num_txs} txs, {elapsed:.1f}s")

            except Exception as e:
                failed_blocks.append((block_num, str(e)))
                print(f"[{i}/{len(blocks_to_process)}] Block {block_num:,}: ERROR - {e}")

    wall_time = time.time() - start_time

    print(f"\n{'='*80}")
    print(f"BATCH COMPLETE")
    print(f"{'='*80}")
    print(f"Blocks processed: {blocks_processed:,}")
    if failed_blocks:
        print(f"Blocks failed: {len(failed_blocks):,}")
        for block_num, error in failed_blocks:
            print(f"  {block_num}: {error}")
    print(f"Total transactions: {total_txs:,}")
    print(f"Total results: {total_results:,}")
    print(f"Wall time: {wall_time:.1f}s")
    print(f"Total compute time: {total_time:.1f}s (parallelism: {total_time/wall_time:.1f}x)")
    print(f"Output directory: {output_dir}")
    if failed_blocks:
        raise RuntimeError(
            f"{len(failed_blocks)} blocks failed; rerun to collect them")


def write_opcode_comparison_to_csv(
    results: List[Dict],
    output_path: Path
):
    """
    Write opcode breakdown comparison to CSV.

    Args:
        results: List of analysis results from compare_opcode_breakdown_across_states()
        output_path: Output CSV file path
    """
    if not results:
        return

    # Collect all unique opcodes across all results
    all_opcodes = set()
    all_account_access_ops = set()
    all_storage_access_ops = set()

    for r in results:
        if r.get('per_op_noncall'):
            all_opcodes.update(r['per_op_noncall'].keys())
        if r.get('per_op_call'):
            all_opcodes.update(r['per_op_call'].keys())
        if r.get('account_cold_accesses'):
            all_account_access_ops.update(r['account_cold_accesses'].keys())
        if r.get('storage_cold_accesses'):
            all_storage_access_ops.update(r['storage_cold_accesses'].keys())

    sorted_opcodes = sorted(all_opcodes)
    # Always include common account and storage ops for cold access tracking
    all_account_access_ops.update(['BALANCE', 'CALL', 'DELEGATECALL', 'STATICCALL', 'EXTCODESIZE', 'EXTCODECOPY'])
    all_storage_access_ops.update(['SLOAD', 'SSTORE'])
    sorted_account_ops = sorted(all_account_access_ops)
    sorted_storage_ops = sorted(all_storage_access_ops)

    # Build field names
    base_fields = [
        'tx_hash', 'original_block', 'state_block', 'lookback',
        'success', 'error', 'gas_used',
        # Intrinsic gas breakdown
        'intrinsic_gas', 'calldata_zero_gas', 'calldata_nonzero_gas', 'creation_gas',
        'access_list_gas', 'authorization_list_gas', 'eip3860_init_gas',
        # Opcode analysis
        'total_opcode_gas', 'uncapped_refund', 'refunds_effective', 'net_gas',
        'opcode_count', 'storage_reads', 'storage_writes', 'storage_slots_modified',
        # Storage slot changes
        'storage_slots_created', 'storage_slots_deleted', 'storage_slots_updated', 'net_storage_slots_written',
        # Account changes
        'accounts_created', 'accounts_deleted',
        # Bytecode changes
        'bytecode_bytes_allocated', 'bytecode_bytes_freed', 'net_bytecode_bytes'
    ]

    opcode_fields = [f'{op}_gas' for op in sorted_opcodes]
    cold_account_fields = [f'{op}_cold_access_count' for op in sorted_account_ops]
    cold_storage_fields = [f'{op}_cold_access_count' for op in sorted_storage_ops]

    fieldnames = base_fields + opcode_fields + cold_account_fields + cold_storage_fields

    # Write to a temp file in the same directory, then rename atomically so a
    # truncated CSV can never be mistaken for a complete block.
    output_path = Path(output_path)
    tmp_path = output_path.with_name(output_path.name + '.tmp')
    with open(tmp_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore',
                               quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()

        for result in results:
            row = {k: result.get(k, '') for k in base_fields}
            # Strip control bytes that older CSV writers reject. Error text is
            # diagnostic only, so retaining printable characters is sufficient.
            if isinstance(row.get('error'), str) and row['error']:
                cleaned = ''.join(ch for ch in row['error']
                                  if ch == '\t' or ch >= ' ')
                row['error'] = cleaned.strip()

            # Add per-opcode gas
            per_op_noncall = result.get('per_op_noncall', {})
            per_op_call = result.get('per_op_call', {})
            account_cold = result.get('account_cold_accesses', {})
            storage_cold = result.get('storage_cold_accesses', {})

            for op in sorted_opcodes:
                row[f'{op}_gas'] = per_op_noncall.get(op, 0) + per_op_call.get(op, 0)

            # Add cold access counts
            for op in sorted_account_ops:
                row[f'{op}_cold_access_count'] = account_cold.get(op, 0)

            for op in sorted_storage_ops:
                row[f'{op}_cold_access_count'] = storage_cold.get(op, 0)

            writer.writerow(row)

    os.replace(tmp_path, output_path)
    print(f"Wrote opcode comparison to: {output_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Analyze opcode breakdown for transactions on different states"
    )

    # Mode selection: single tx, single block, batch from source, or block range
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--tx-hash", help="Single transaction hash to analyze")
    mode_group.add_argument("--block", type=int, help="Block number to analyze all transactions")
    mode_group.add_argument("--from-source", help="Directory with existing block_*_opcode_gas.csv files to process")
    mode_group.add_argument("--block-range", action="store_true",
                           help="Process all blocks in range [start-block, end-block]")

    parser.add_argument("--chain", default="ethereum", help="Chain name")
    parser.add_argument("--rpc-url", default=None, help="Custom RPC URL")
    parser.add_argument("--lookback-points", type=int, nargs='+', default=[0, 5, 10, 20],
                       help="Specific lookback points to test (default: 0 5 10 20)")
    parser.add_argument("--max-txs", type=int, default=None,
                       help="Maximum transactions to analyze per block (None = all)")
    parser.add_argument("--num-workers", type=int, default=10,
                       help="Number of parallel workers for block mode")
    parser.add_argument("--executor", choices=("threads", "processes"), default="threads",
                       help="Block-level executor for batch modes (default: threads)")
    parser.add_argument("--gas-chunk-size", type=int, default=20_000_000,
                       help="Target gas per batch within each block (default: 20000000 = 20M gas)")
    parser.add_argument("--output-dir", default=None,
                       help="Output directory (default: RAW_BASE_DIR/opcode_breakdown_sensitive)")
    parser.add_argument("--quiet", action="store_true",
                       help="Reduce output verbosity (only show block-level summaries)")

    # Batch mode: block range specification
    parser.add_argument("--start-block", type=int, default=None,
                       help="Start block number (required for --block-range, optional for --from-source)")
    parser.add_argument("--end-block", type=int, default=None,
                       help="End block number (required for --block-range, optional for --from-source)")

    args = parser.parse_args()

    # Setup
    rpc_url = args.rpc_url if args.rpc_url else get_rpc_url(args.chain)

    # Set output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        raw_base = os.getenv('RAW_BASE_DIR')
        if not raw_base:
            print("ERROR: RAW_BASE_DIR not set in .env file and --output-dir not provided")
            sys.exit(1)
        output_dir = Path(raw_base) / 'opcode_breakdown_sensitive'

    output_dir.mkdir(exist_ok=True, parents=True)

    print(f"Chain: {args.chain}")
    print(f"RPC: {rpc_url}")
    print(f"Lookback points: {args.lookback_points}")
    print(f"Output directory: {output_dir}")
    print()

    # Validate mode combinations
    if args.block_range and (args.start_block is None or args.end_block is None):
        print("Error: --block-range requires both --start-block and --end-block")
        return

    # Batch modes (--block-range / --from-source) write CSVs per block inside
    # analyze_blocks_from_list and do not collect results in memory.
    results = None

    # Run analysis based on mode
    if args.tx_hash:
        # Single transaction mode
        # Need to get the block number from the transaction
        tx = get_transaction(args.tx_hash, rpc_url)
        if not tx:
            print(f"Error: Could not fetch transaction {args.tx_hash}")
            return

        original_block = int(tx['blockNumber'], 16) if isinstance(tx['blockNumber'], str) else tx['blockNumber']

        print(f"Analyzing transaction: {args.tx_hash}")
        print(f"Original block: {original_block:,}")
        print()

        results = compare_opcode_breakdown_across_states(
            tx_hash=args.tx_hash,
            original_block=original_block,
            rpc_url=rpc_url,
            lookback_points=args.lookback_points,
            quiet=args.quiet
        )

        if not results:
            print("No results obtained")
            return

        # Write to CSV
        output_file = output_dir / f"{args.tx_hash}_opcode_breakdown.csv"
        write_opcode_comparison_to_csv(results, output_file)

    elif args.block:
        # Single block mode
        print(f"Analyzing block: {args.block:,}")
        if args.max_txs:
            print(f"Max transactions: {args.max_txs}")
        print(f"Workers: {args.num_workers}")
        print()

        results = analyze_block_opcode_breakdown_on_states(
            block_num=args.block,
            rpc_url=rpc_url,
            lookback_points=args.lookback_points,
            max_txs=args.max_txs,
            num_workers=args.num_workers,
            quiet=args.quiet
        )

        if not results:
            print("No results obtained")
            return

        # Write to CSV
        output_file = output_dir / f"block_{args.block}_opcode_breakdown.csv"
        write_opcode_comparison_to_csv(results, output_file)

    elif args.block_range:
        # Block range mode: analyze all blocks in range
        print(f"Block range mode: {args.start_block:,} to {args.end_block:,}")
        print()

        # Generate list of all blocks in range
        block_list = list(range(args.start_block, args.end_block + 1))
        print(f"Generated {len(block_list):,} blocks to analyze")

        # Run batch analysis
        analyze_blocks_from_list(
            block_list=block_list,
            chain=args.chain,
            rpc_url=rpc_url,
            output_dir=output_dir,
            lookback_points=args.lookback_points,
            max_txs=args.max_txs,
            num_workers=args.num_workers,
            quiet=args.quiet,
            gas_chunk_size=args.gas_chunk_size,
            executor_kind=args.executor
        )

    elif args.from_source:
        # Batch mode from source directory
        print(f"Batch mode: analyzing blocks from {args.from_source}")
        print(f"Block range filter: {args.start_block or 'none'} to {args.end_block or 'none'}")
        print()

        # Get list of blocks from source directory
        block_list = get_blocks_from_existing_analysis(
            source_dir=args.from_source,
            start_block=args.start_block,
            end_block=args.end_block
        )

        if not block_list:
            print("No blocks found matching criteria")
            return

        print(f"Found {len(block_list):,} blocks to analyze")

        # Run batch analysis
        analyze_blocks_from_list(
            block_list=block_list,
            chain=args.chain,
            rpc_url=rpc_url,
            output_dir=output_dir,
            lookback_points=args.lookback_points,
            max_txs=args.max_txs,
            num_workers=args.num_workers,
            quiet=args.quiet,
            source_dir=args.from_source,  # Use source CSVs for load balancing
            gas_chunk_size=args.gas_chunk_size,
            executor_kind=args.executor
        )

    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    if args.tx_hash:
        print(f"Transaction: {args.tx_hash}")
        tx = get_transaction(args.tx_hash, rpc_url)
        if tx:
            original_block = int(tx['blockNumber'], 16) if isinstance(tx['blockNumber'], str) else tx['blockNumber']
            print(f"Original block: {original_block:,}")
    elif args.block:
        print(f"Block: {args.block:,}")
    elif args.from_source:
        print(f"Source directory: {args.from_source}")
    elif args.block_range:
        print(f"Block range: {args.start_block:,} - {args.end_block:,}")

    if results is None:
        print("Per-block results written to CSV files in the output directory")
    else:
        print(f"Results collected: {len(results)}")

    if results:
        # Group by transaction for block mode
        if args.block:
            tx_groups = {}
            for r in results:
                tx_hash = r['tx_hash']
                if tx_hash not in tx_groups:
                    tx_groups[tx_hash] = []
                tx_groups[tx_hash].append(r)

            print(f"Transactions analyzed: {len(tx_groups)}")
            print(f"\nPer-transaction summary:")
            for tx_hash, tx_results in list(tx_groups.items())[:10]:  # Show first 10
                success_count = sum(1 for r in tx_results if r['success'])
                print(f"  {tx_hash[:10]}... {success_count}/{len(tx_results)} lookbacks succeeded")
            if len(tx_groups) > 10:
                print(f"  ... and {len(tx_groups) - 10} more transactions")

        else:
            # Single transaction mode - show detailed breakdown
            print(f"\nOpcode gas by lookback:")
            for r in results:
                status = "ok" if r['success'] else "FAILED"
                print(f"  Lookback {str(r.get('lookback', '?')):>2} (block {r['state_block']:,}): "
                      f"{status} {r.get('total_opcode_gas', 0):>10,} gas, "
                      f"{r.get('opcode_count', 0):>6,} opcodes")

            # Show which opcodes changed the most
            if len(results) > 1:
                first = results[0]
                last = results[-1]

                if first.get('per_op_noncall') and last.get('per_op_noncall'):
                    print(f"\nOpcode gas changes (lookback 0 → {last['lookback']}):")

                    changes = {}
                    all_ops = set(first.get('per_op_noncall', {}).keys()) | set(first.get('per_op_call', {}).keys()) | \
                             set(last.get('per_op_noncall', {}).keys()) | set(last.get('per_op_call', {}).keys())

                    for op in all_ops:
                        gas_first = first.get('per_op_noncall', {}).get(op, 0) + first.get('per_op_call', {}).get(op, 0)
                        gas_last = last.get('per_op_noncall', {}).get(op, 0) + last.get('per_op_call', {}).get(op, 0)
                        delta = gas_last - gas_first
                        if delta != 0:
                            changes[op] = delta

                    # Show top 10 changes
                    top_changes = sorted(changes.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
                    for op, delta in top_changes:
                        sign = "+" if delta > 0 else ""
                        print(f"  {op:20s}: {sign}{delta:>10,} gas")


if __name__ == "__main__":
    main()
