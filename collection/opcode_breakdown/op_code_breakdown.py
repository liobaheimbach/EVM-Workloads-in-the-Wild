#!/usr/bin/env python3
# op_code_breakdown.py - per-transaction opcode-level gas breakdown for EVM blocks
# All-depth per-op accounting; CALL/CREATE = net(frame) - sum(net(direct children))
from typing import Any, Dict, List, Set, Tuple, Optional
from web3 import Web3
from collections import defaultdict
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import random
from pathlib import Path
import requests
import os
import re
import time
import gc
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / '.env')

# Resolve the sibling utils package at import time so the script works from any CWD.
import sys as _sys
_UTILS_DIR = Path(__file__).resolve().parent.parent / 'utils'
if str(_UTILS_DIR) not in _sys.path:
    _sys.path.insert(0, str(_UTILS_DIR))

from eip7702_authorization import (authority_state_from_parity_statediff,
                                   balances_before_each_tx, recover_authority,
                                   resolve_existence,
                                   authorization_refund, combine_refunds)
from account_births import account_transition_counts_from_trace

# ──────────────────────────────────────────────────────────────────────────────
# PRECOMPILE ADDRESSES
# ──────────────────────────────────────────────────────────────────────────────
PRECOMPILE_ADDRESSES = {
    '0x0000000000000000000000000000000000000001': 'ECRECOVER',
    '0x0000000000000000000000000000000000000002': 'SHA256',
    '0x0000000000000000000000000000000000000003': 'RIPEMD160',
    '0x0000000000000000000000000000000000000004': 'IDENTITY',
    '0x0000000000000000000000000000000000000005': 'MODEXP',
    '0x0000000000000000000000000000000000000006': 'ECADD',
    '0x0000000000000000000000000000000000000007': 'ECMUL',
    '0x0000000000000000000000000000000000000008': 'ECPAIRING',
    '0x0000000000000000000000000000000000000009': 'BLAKE2F',
    '0x000000000000000000000000000000000000000a': 'POINT_EVALUATION',  # Cancun: KZG point evaluation
    '0x000000000000000000000000000000000000000b': 'BLS12_G1ADD',  # Pectra: BLS12-381 G1 addition
    '0x000000000000000000000000000000000000000c': 'BLS12_G1MSM',  # Pectra: BLS12-381 G1 multi-scalar multiplication
    '0x000000000000000000000000000000000000000d': 'BLS12_G2ADD',  # Pectra: BLS12-381 G2 addition
    '0x000000000000000000000000000000000000000e': 'BLS12_G2MSM',  # Pectra: BLS12-381 G2 multi-scalar multiplication
    '0x000000000000000000000000000000000000000f': 'BLS12_PAIRING',  # Pectra: BLS12-381 pairing check
    '0x0000000000000000000000000000000000000010': 'BLS12_MAP_FP_TO_G1',  # Pectra: BLS12-381 map Fp to G1
    '0x0000000000000000000000000000000000000011': 'BLS12_MAP_FP2_TO_G2',  # Pectra: BLS12-381 map Fp2 to G2
    '0x0000000000000000000000000000000000000100': 'P256VERIFY',  # RIP-7212: secp256r1 (P-256) signature verification
}

def is_precompile(address: str) -> Optional[str]:
    """Check if address is a precompile and return its name."""
    if address:
        normalized = address.lower()
        return PRECOMPILE_ADDRESSES.get(normalized)
    return None

# ──────────────────────────────────────────────────────────────────────────────
# FORK ACTIVATION BLOCKS
# ──────────────────────────────────────────────────────────────────────────────
# Pectra (EIP-7623 calldata floor pricing) activation blocks
PECTRA_FORK_BLOCKS = {
    'ethereum': 22_431_084,  # May 7, 2025, 10:05:11 UTC
    # Base (OP Stack): Pectra via Superchain Isthmus hardfork (timestamp-based)
    # Activation: Fri, May 9, 2025, 16:00:01 UTC (UNIX 1746806401)
    'base': 30_008_527,
}

# ──────────────────────────────────────────────────────────────────────────────
# MULTI-CHAIN CONFIG
# ──────────────────────────────────────────────────────────────────────────────
# Load RPC configuration from external file
with open(Path(__file__).parent / 'rpc_config.json', 'r') as f:
    CHAIN_CONFIGS = json.load(f)

# Default chain (can be overridden)
CURRENT_CHAIN = 'ethereum'

RPC_URL = CHAIN_CONFIGS[CURRENT_CHAIN]['rpc_url']
RPC_FOR_TRACING = CHAIN_CONFIGS[CURRENT_CHAIN]['rpc_tracing']
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# Thread pool configuration
# 4 was the empirical sweet spot on our Reth node — beyond that the node
# saturates and per-block latency rises sharply without throughput gain.
# Bump up with --workers if your node has more capacity.
NUM_WORKERS = 4

# Batch size configuration for RPC requests
# Set to None for no limit, or specify max transactions per batch (e.g., 20-25 for 100MB RPC limit)
BATCH_CHUNK_SIZE = None  # Can be overridden via command line --batch-size

# HTTP Session pool for connection reuse
_session_pool = {}

def normalize_chain_name(chain: str) -> str:
    """Normalize chain name: base1/base2/base3 -> base, ethereum1/ethereum2 -> ethereum, etc."""
    chain = chain.lower().strip()
    # Remove numeric suffixes (base1 -> base, ethereum2 -> ethereum)
    return re.sub(r'\d+$', '', chain)

def check_tracing_client(rpc_tracing: str, strict: bool = False) -> str:
    """Verify the tracing endpoint is reth, which this collector's gas accounting assumes.

    reth reports actually-burned gas in structLog `gasCost`; erigon/geth report the
    nominal cost (see _effective_op_gas). The clamp there compensates, but other
    reth-specific trace semantics are not compensated, so a non-reth endpoint is
    reported rather than silently accepted.
    """
    try:
        r = requests.post(rpc_tracing,
                          json={"jsonrpc": "2.0", "id": 1, "method": "web3_clientVersion", "params": []},
                          timeout=30)
        version = (r.json() or {}).get('result') or ''
    except Exception as e:
        msg = f"could not determine tracing client version: {e}"
        if strict:
            raise RuntimeError(msg)
        print(f"WARNING: {msg}")
        return ''
    if 'reth' not in version.lower():
        msg = (f"tracing endpoint reports '{version}', not reth; gas accounting in this "
               f"collector assumes reth trace semantics")
        if strict:
            raise RuntimeError(msg)
        print(f"WARNING: {msg}")
    return version

def get_session():
    """Get or create a requests.Session for the current thread/process"""
    pid = os.getpid()
    if pid not in _session_pool:
        session = requests.Session()
        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _session_pool[pid] = session
    return _session_pool[pid]

# Output directory configuration
RAW_BASE_DIR = os.getenv('RAW_BASE_DIR')
if not RAW_BASE_DIR:
    raise ValueError("RAW_BASE_DIR must be set in .env file")

# ──────────────────────────────────────────────────────────────────────────────
# Date-to-Block Conversion (Binary Search)
# ──────────────────────────────────────────────────────────────────────────────
def timestamp_to_block(target_timestamp: int, w3_instance: Web3,
                       start_block: Optional[int] = None,
                       end_block: Optional[int] = None) -> int:
    """
    Binary search to find the block number closest to the given timestamp.

    Args:
        target_timestamp: Unix timestamp to search for
        w3_instance: Web3 instance for the chain
        start_block: Starting block for search (default: 0)
        end_block: Ending block for search (default: latest)

    Returns:
        Block number closest to the target timestamp
    """
    if end_block is None:
        end_block = w3_instance.eth.block_number
    if start_block is None:
        start_block = 0

    # Binary search
    while start_block < end_block:
        mid_block = (start_block + end_block) // 2
        mid_timestamp = w3_instance.eth.get_block(mid_block)['timestamp']

        if mid_timestamp < target_timestamp:
            start_block = mid_block + 1
        elif mid_timestamp > target_timestamp:
            end_block = mid_block
        else:
            return mid_block

    return start_block

def sample_blocks_for_date_range(start_date: str, end_date: str,
                                  blocks_per_day: int = 100,
                                  chain: str = 'ethereum') -> Dict[str, List[int]]:
    """
    Sample random blocks for each day in the date range.
    If blocks already exist in the output folder, exclude them and sample additional blocks.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        blocks_per_day: Number of blocks to sample per day
        chain: Chain name

    Returns:
        Dict mapping date strings to lists of block numbers
    """
    import csv

    config = CHAIN_CONFIGS[chain]
    w3_instance = Web3(Web3.HTTPProvider(config['rpc_url']))

    start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    sampled_blocks = {}
    current_date = start_dt

    # Load existing blocks from summary CSV
    all_existing_blocks = set()
    base_name = normalize_chain_name(chain)  # base1, base2, base3 -> base
    summary_file = Path(RAW_BASE_DIR) / "opcode_breakdown" / f"{base_name}_blocks_summary.csv"

    print(f"Loading existing blocks from {summary_file}...")
    if summary_file.exists():
        with open(summary_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Normalize source chain name for comparison (base1/base2/base3 all match 'base')
                if normalize_chain_name(row['source']) == base_name:
                    all_existing_blocks.add(int(row['block_number']))
    else:
        print(f"  Summary file does not exist yet, will create it.")
    print(f"Loaded {len(all_existing_blocks)} existing blocks for {chain}")

    prev_day_end_block = None  # Track previous day's end block to avoid overlap

    while current_date <= end_dt:
        date_str = current_date.strftime('%Y-%m-%d')
        next_date = current_date + timedelta(days=1)

        # Get block range for this day (UTC)
        day_start_ts = int(current_date.timestamp())
        day_end_ts = int(next_date.timestamp()) - 1

        day_start_block = timestamp_to_block(day_start_ts, w3_instance)
        day_end_block = timestamp_to_block(day_end_ts, w3_instance)

        # Prevent overlap with previous day: if same block, increment start
        if prev_day_end_block is not None and day_start_block <= prev_day_end_block:
            day_start_block = prev_day_end_block + 1

        # Find existing blocks in this range from preloaded set
        existing_blocks = {b for b in all_existing_blocks if day_start_block <= b <= day_end_block}

        # Calculate how many new blocks we need to sample
        existing_count = len(existing_blocks)
        needed_count = max(0, blocks_per_day - existing_count)

        # Get available blocks (exclude existing ones)
        available_blocks = [b for b in range(day_start_block, day_end_block + 1)
                           if b not in existing_blocks]

        # Sample additional blocks if needed
        if needed_count > 0 and available_blocks:
            sample_count = min(needed_count, len(available_blocks))
            newly_sampled = random.sample(available_blocks, sample_count)
            sampled_blocks[date_str] = sorted(newly_sampled)

            print(f"{chain} {date_str}: Found {existing_count} existing blocks, sampling {sample_count} additional blocks from {day_start_block}-{day_end_block}")
        else:
            sampled_blocks[date_str] = []
            print(f"{chain} {date_str}: Already have {existing_count} blocks (target: {blocks_per_day}), no additional sampling needed")

        # Track this day's end block to prevent overlap with next day
        prev_day_end_block = day_end_block
        current_date = next_date

    return sampled_blocks

# ──────────────────────────────────────────────────────────────────────────────
# CSV writer (updated for multi-chain)
# ──────────────────────────────────────────────────────────────────────────────
class BlockNotWritable(RuntimeError):
    """A block contains transactions that failed to analyse.

    Writing it would persist them as zero-gas rows with status=1 and
    difference=0, indistinguishable from real successful transactions.
    """


def write_block_opcode_breakdown_to_file(block_num: int, block_data: List[Dict], chain: str = None):
    import csv

    # Every field below is written with .get(field, 0), so a result dict carrying
    # 'error' would become a clean-looking zero-gas row that the mismatch counter
    # cannot see. Refuse the whole block instead: partial output is worse than none.
    failed = [r for r in block_data if r.get('error')]
    if failed:
        raise BlockNotWritable(
            f"block {block_num}: {len(failed)}/{len(block_data)} transactions failed "
            f"to analyse, e.g. {failed[0].get('tx_hash','?')}: {failed[0]['error']}")

    missing_exact = [
        r for r in block_data
        if r.get('account_births') is None or r.get('account_deaths') is None
    ]
    if missing_exact:
        raise BlockNotWritable(
            f"block {block_num}: {len(missing_exact)}/{len(block_data)} transactions "
            "lack exact account-transition counts"
        )

    # Use chain-specific folder
    base_dir = Path(RAW_BASE_DIR)
    if not chain:
        raise ValueError("chain parameter is required")
    # Use base folder name (base1, base2, base3 -> base)
    base_name = normalize_chain_name(chain)
    output_dir = base_dir / "opcode_breakdown" / base_name

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"block_{block_num}_opcode_gas.csv"

    all_opcodes = set()
    all_account_access_ops = set()
    all_storage_access_ops = set()
    for tx_data in block_data:
        all_opcodes.update((tx_data.get('per_op_noncall') or {}).keys())
        all_opcodes.update((tx_data.get('per_op_call') or {}).keys())
        all_account_access_ops.update((tx_data.get('account_access_counts') or {}).keys())
        all_storage_access_ops.update((tx_data.get('storage_access_counts') or {}).keys())
    sorted_ops = sorted(all_opcodes)
    sorted_account_ops = sorted(all_account_access_ops)
    sorted_storage_ops = sorted(all_storage_access_ops)

    headers = [
        'tx_hash','gas_used','gas_limit',
        'intrinsic_gas','calldata_zero_gas','calldata_nonzero_gas',
        'creation_gas','eip3860_init_gas','access_list_gas','authorization_list_gas','contract_creation_gas',
        'execution_gas_total','uncapped_refund','refunds_effective','calculated_total','difference',
        'storage_slots_created','storage_slots_deleted','storage_slots_updated','net_storage_slots_written',
        'accounts_created','accounts_deleted',
        'account_births','account_deaths',
        'bytecode_bytes_allocated','bytecode_bytes_freed','net_bytecode_bytes',
        # Validate against the receipt instead.
        'status','calculated_refunds_pre_cap'
    ] + [f"{op}_total_gas" for op in sorted_ops] \
      + [f"{op}_cold_access_count" for op in sorted_account_ops] \
      + [f"{op}_cold_access_count" for op in sorted_storage_ops]

    with open(filename, 'w', newline='') as f:
        wr = csv.writer(f); wr.writerow(headers)
        for r in block_data:
            row = [
                r.get('tx_hash',''), r.get('actual_gas',0), r.get('gas_limit',0),
                r.get('intrinsic_gas',0), r.get('calldata_zero_gas',0), r.get('calldata_nonzero_gas',0),
                r.get('creation_gas',0), r.get('eip3860_init_gas',0), r.get('access_list_gas',0),
                r.get('authorization_list_gas',0), r.get('contract_creation_gas',0),
                r.get('opcode_gas_total',0), r.get('uncapped_refund',0), r.get('final_refunds',0), r.get('calculated_total',0), r.get('difference',0),
                r.get('storage_slots_created',0), r.get('storage_slots_deleted',0), r.get('storage_slots_updated',0),
                r.get('net_storage_slots_written',0), r.get('accounts_created',0), r.get('accounts_deleted',0),
                r.get('account_births'), r.get('account_deaths'),
                r.get('bytecode_bytes_allocated',0), r.get('bytecode_bytes_freed',0), r.get('net_bytecode_bytes',0),
                r.get('status',1), r.get('calculated_refunds_pre_cap',0)
            ]
            per_op_noncall = r.get('per_op_noncall',{}) or {}
            per_op_call = r.get('per_op_call',{}) or {}
            account_access_counts = r.get('account_access_counts',{}) or {}
            storage_access_counts = r.get('storage_access_counts',{}) or {}
            account_cold_counts = r.get('account_cold_counts',{}) or {}
            storage_cold_counts = r.get('storage_cold_counts',{}) or {}

            for op in sorted_ops:
                row.append(per_op_noncall.get(op,0)+per_op_call.get(op,0))
            for op in sorted_account_ops:
                row.append(account_cold_counts.get(op,0))
            for op in sorted_storage_ops:
                row.append(storage_cold_counts.get(op,0))
            wr.writerow(row)

    # Append to summary CSV
    summary_file = base_dir / "opcode_breakdown" / f"{base_name}_blocks_summary.csv"

    # Create summary file with header if it doesn't exist
    if not summary_file.exists():
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['block_number', 'source'])

    # Append block entry
    with open(summary_file, 'a', newline='') as f:
        writer = csv.writer(f)
        # Format: block_number, source (normalized chain name)
        writer.writerow([block_num, base_name])

    print(f"\nBlock opcode breakdown written to: {filename}")
    return filename

def print_mismatches(mismatches: List[Dict], chain: str = None, date: str = None):
    """
    Print transactions with mismatches to console.

    Args:
        mismatches: List of transaction data with non-zero difference
        chain: Chain name (for context)
        date: Date string (for context)
    """
    if not mismatches:
        return

    prefix = f"{chain} {date}" if chain and date else (chain or date or "")
    print(f"\nWARNING: {len(mismatches)} mismatches found" + (f" for {prefix}" if prefix else ""))

    for m in mismatches:
        print(f"  TX {m.get('tx_hash', 'N/A')[:16]}... block={m.get('block_number', 'N/A')} "
              f"diff={m.get('difference', 0):,} actual={m.get('actual_gas', 0):,} "
              f"calculated={m.get('calculated_total', 0):,}")

# ──────────────────────────────────────────────────────────────────────────────
# Tracers & utilities
# ──────────────────────────────────────────────────────────────────────────────
DFT_TRACER_JSON='{"disableMemory":true,"disableStorage":true,"disableStack":true,"limit":0,"timeout":"120s"}'
ACCOUNT_TRACER_CONFIG = {
    "tracer": "prestateTracer",
    "tracerConfig": {
        "diffMode": True,
        "disableCode": False,
        "disableStorage": True,
    },
}


def _exact_account_counts(result: Any) -> Dict[str, Optional[int]]:
    births, deaths = account_transition_counts_from_trace(result)
    return {"account_births": births, "account_deaths": deaths}


def trace_block_account_changes(
    block_num: int, rpc_url=RPC_FOR_TRACING
) -> Dict[str, Dict[str, Optional[int]]]:
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_num), ACCOUNT_TRACER_CONFIG],
        "id": 1,
    }
    response = get_session().post(
        rpc_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=(30, 600),
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"account trace failed: {response.status_code} {response.text}"
        )
    body = response.json()
    if "error" in body:
        raise RuntimeError(f"account trace error: {body['error']}")
    rows = body.get("result")
    if not isinstance(rows, list):
        raise RuntimeError("account block trace result is not a list")

    counts = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not row.get("txHash"):
            raise RuntimeError(f"account trace row {index} lacks txHash")
        tx_hash = str(row["txHash"]).lower()
        values = _exact_account_counts(row.get("result"))
        for key in {tx_hash, tx_hash.removeprefix("0x")}:
            if key in counts:
                raise RuntimeError(f"duplicate account trace transaction: {tx_hash}")
            counts[key] = values
    return counts
CALL_SET = {'CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL'}

def cast_rpc_dbg_tx(tx_hash, tracer=DFT_TRACER_JSON, rpc_url=RPC_FOR_TRACING):
    """Fetch debug trace using direct RPC call instead of cast command."""
    # Parse tracer if it's a JSON string
    if isinstance(tracer, str):
        tracer_params = json.loads(tracer)
    else:
        tracer_params = tracer

    # Make RPC request
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, tracer_params],
        "id": 1
    }

    # Use longer timeout for trace requests (connect timeout=30s, read timeout=600s)
    session = get_session()
    response = session.post(rpc_url, json=payload, headers={'Content-Type': 'application/json'},
                           timeout=(30, 600))

    if response.status_code != 200:
        raise RuntimeError(f"RPC request failed: {response.text}")

    result = response.json()

    if "error" in result:
        # Check if this is an "internal eth error" - try alternative RPC for ethereum
        error_msg = result['error'].get('message', '') if isinstance(result['error'], dict) else str(result['error'])

        if 'internal eth error' in error_msg and CURRENT_CHAIN == 'ethereum':
            # Try alternative RPC for problematic EIP-7702 transactions
            alternative_rpc = CHAIN_CONFIGS[CURRENT_CHAIN].get('rpc_alternative')
            try:
                alt_response = session.post(alternative_rpc, json=payload, headers={'Content-Type': 'application/json'},
                                           timeout=(30, 600))
                if alt_response.status_code == 200:
                    alt_result = alt_response.json()
                    if "error" not in alt_result:
                        return alt_result.get("result", {})
            except Exception:
                pass  # Fall through to original error

        raise RuntimeError(f"RPC error: {result['error']}")

    return result.get("result", {})

def batch_trace_transactions(tx_hashes: List[str], rpc_url=RPC_FOR_TRACING, chunk_size: int = None) -> Dict[str, Dict]:
    """
    Batch multiple debug_traceTransaction calls into a single JSON-RPC batch request.
    Returns dict mapping tx_hash -> {struct_trace}

    Args:
        tx_hashes: List of transaction hashes to trace
        rpc_url: RPC endpoint URL
        chunk_size: Max transactions per batch (None = no limit). Use to avoid RPC response size limits.
                   Recommended: 20-25 for nodes with 100MB response limit.
    """
    if not tx_hashes:
        return {}

    # If chunk_size specified and we have more transactions, split into chunks
    # Process sequentially to minimize memory usage
    if chunk_size and len(tx_hashes) > chunk_size:
        all_results = {}
        num_chunks = (len(tx_hashes) + chunk_size - 1) // chunk_size
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(tx_hashes))
            chunk = tx_hashes[start_idx:end_idx]

            # Process this chunk directly (not recursive to save stack)
            chunk_results = _batch_trace_single_chunk(chunk, rpc_url)
            all_results.update(chunk_results)
            del chunk_results
        return all_results

    # Single batch (no chunking needed)
    return _batch_trace_single_chunk(tx_hashes, rpc_url)

def _batch_trace_single_chunk(tx_hashes: List[str], rpc_url: str) -> Dict[str, Dict]:
    """Internal function to batch trace a single chunk of transactions."""
    batch_request = []
    request_id = 0
    request_map = {}

    for tx_hash in tx_hashes:
        # Fetch structLog trace
        batch_request.append({
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [tx_hash, json.loads(DFT_TRACER_JSON)],
            "id": request_id
        })
        request_map[request_id] = (tx_hash, 'struct')
        request_id += 1

        # Fetch state diff (for storage slot changes)
        batch_request.append({
            "jsonrpc": "2.0",
            "method": "debug_traceTransaction",
            "params": [tx_hash, {
                "tracer": "prestateTracer",
                "tracerConfig": {"diffMode": True, "disableCode": False},
            }],
            "id": request_id
        })
        request_map[request_id] = (tx_hash, 'state')
        request_id += 1

    # Use longer timeout for batch trace requests (connect timeout=30s, read timeout=600s)
    session = get_session()
    response = session.post(
        rpc_url,
        json=batch_request,
        headers={"Content-Type": "application/json"},
        timeout=(30, 600)
    )
    if response.status_code != 200:
        raise RuntimeError(f"Batch RPC request failed: {response.status_code} {response.text}")

    responses = response.json()
    results = {tx_hash: {} for tx_hash in tx_hashes}

    # Track transactions that had "internal eth error" for fallback retry
    failed_txs = []

    for resp in responses:
        if "error" in resp:
            req_id = resp.get("id")
            if req_id in request_map:
                tx_hash, tracer_type = request_map[req_id]
                results[tx_hash][f'{tracer_type}_error'] = resp["error"]

                # Check if this is an "internal eth error" on ethereum
                error_msg = resp['error'].get('message', '') if isinstance(resp['error'], dict) else str(resp['error'])
                if 'internal eth error' in error_msg and CURRENT_CHAIN == 'ethereum':
                    failed_txs.append((tx_hash, tracer_type))
            continue

        req_id = resp.get("id")
        tx_hash, tracer_type = request_map[req_id]
        if tracer_type == 'struct':
            results[tx_hash]['struct_trace'] = resp
        elif tracer_type == 'state':
            results[tx_hash]['state_diff'] = resp

    # Retry failed transactions with alternative RPC
    if failed_txs:
        alternative_rpc = CHAIN_CONFIGS[CURRENT_CHAIN].get('rpc_alternative')
        try:
            retry_batch = []
            retry_map = {}
            retry_id = 1

            for tx_hash, tracer_type in failed_txs:
                if tracer_type == 'struct':
                    retry_batch.append({
                        "jsonrpc": "2.0",
                        "method": "debug_traceTransaction",
                        "params": [tx_hash, json.loads(DFT_TRACER_JSON)],
                        "id": retry_id
                    })
                    retry_map[retry_id] = (tx_hash, 'struct')
                    retry_id += 1
                elif tracer_type == 'state':
                    retry_batch.append({
                        "jsonrpc": "2.0",
                        "method": "debug_traceTransaction",
                        "params": [tx_hash, {
                            "tracer": "prestateTracer",
                            "tracerConfig": {"diffMode": True, "disableCode": False},
                        }],
                        "id": retry_id
                    })
                    retry_map[retry_id] = (tx_hash, 'state')
                    retry_id += 1

            if retry_batch:
                retry_response = session.post(alternative_rpc, json=retry_batch,
                                             headers={"Content-Type": "application/json"},
                                             timeout=(30, 600))
                if retry_response.status_code == 200:
                    retry_responses = retry_response.json()
                    for resp in retry_responses:
                        if "error" not in resp:
                            req_id = resp.get("id")
                            if req_id in retry_map:
                                tx_hash, tracer_type = retry_map[req_id]
                                # Remove error and add successful result
                                if f'{tracer_type}_error' in results[tx_hash]:
                                    del results[tx_hash][f'{tracer_type}_error']
                                if tracer_type == 'struct':
                                    results[tx_hash]['struct_trace'] = resp
                                elif tracer_type == 'state':
                                    results[tx_hash]['state_diff'] = resp
        except Exception:
            pass  # Keep original errors if fallback fails

    return results

def batch_get_transactions_and_receipts(tx_hashes: List[str], rpc_url=RPC_URL, chunk_size: int = None) -> Dict[str, Dict]:
    """
    Batch fetch transactions and receipts for multiple tx hashes.
    Returns dict mapping tx_hash -> {tx, receipt}

    Args:
        tx_hashes: List of transaction hashes
        rpc_url: RPC endpoint URL
        chunk_size: Max transactions per batch (None = no limit)
    """
    if not tx_hashes:
        return {}

    # If chunk_size specified and we have more transactions, split into chunks
    # Process sequentially to minimize memory usage
    if chunk_size and len(tx_hashes) > chunk_size:
        all_results = {}
        num_chunks = (len(tx_hashes) + chunk_size - 1) // chunk_size
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(tx_hashes))
            chunk = tx_hashes[start_idx:end_idx]

            # Process this chunk directly
            chunk_results = _batch_get_tx_receipts_single_chunk(chunk, rpc_url)
            all_results.update(chunk_results)
            del chunk_results
        return all_results

    # Single batch (no chunking needed)
    return _batch_get_tx_receipts_single_chunk(tx_hashes, rpc_url)

def _batch_get_tx_receipts_single_chunk(tx_hashes: List[str], rpc_url: str) -> Dict[str, Dict]:
    """Internal function to batch fetch tx/receipts for a single chunk."""
    session = get_session()
    batch_request = []
    request_id = 1
    request_map = {}

    # Add all getTransaction requests
    for tx_hash in tx_hashes:
        batch_request.append({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": request_id
        })
        request_map[request_id] = (tx_hash, 'tx')
        request_id += 1

    # Add all getTransactionReceipt requests
    for tx_hash in tx_hashes:
        batch_request.append({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": request_id
        })
        request_map[request_id] = (tx_hash, 'receipt')
        request_id += 1

    response = session.post(
        rpc_url,
        json=batch_request,
        headers={"Content-Type": "application/json"},
        timeout=(30, 600)
    )

    if response.status_code != 200:
        raise RuntimeError(f"Batch tx/receipt request failed: {response.status_code}")

    responses = response.json()
    results = {tx_hash: {} for tx_hash in tx_hashes}

    for resp in responses:
        if "error" in resp:
            continue

        req_id = resp.get("id")
        if req_id not in request_map:
            continue

        tx_hash, req_type = request_map[req_id]
        result = resp.get("result")

        if result is None:
            continue

        # Convert raw RPC response (hex strings) to Web3 format (integers)
        # This ensures compatibility with analyze_transaction which expects Web3-formatted data

        # Fields that should remain as hex strings (hashes, addresses, data)
        HEX_STRING_FIELDS = {
            'hash', 'blockHash', 'from', 'to', 'input', 'data',
            'transactionHash', 'logsBloom', 'contractAddress',
            'sourceHash', 'r', 's'
        }

        if req_type == 'tx':
            # Convert hex strings to integers for numeric fields only
            formatted = {}
            for key, value in result.items():
                if key in HEX_STRING_FIELDS:
                    formatted[key] = value  # Keep hashes/addresses as hex strings
                elif isinstance(value, str) and value.startswith('0x'):
                    try:
                        formatted[key] = int(value, 16)
                    except (ValueError, TypeError):
                        formatted[key] = value  # Keep as-is if conversion fails
                else:
                    formatted[key] = value
            results[tx_hash]['tx'] = formatted
        elif req_type == 'receipt':
            # Convert hex strings to integers for numeric fields only
            formatted = {}
            for key, value in result.items():
                if key in HEX_STRING_FIELDS:
                    formatted[key] = value  # Keep hashes/addresses as hex strings
                elif isinstance(value, str) and value.startswith('0x'):
                    try:
                        formatted[key] = int(value, 16)
                    except (ValueError, TypeError):
                        formatted[key] = value  # Keep as-is if conversion fails
                else:
                    formatted[key] = value
            results[tx_hash]['receipt'] = formatted

    return results

def trace_block_by_number(block_num: int, rpc_url=RPC_FOR_TRACING) -> Dict[str, Dict]:
    """
    Trace an entire block with debug_traceBlockByNumber using the default tracer.
    This is MUCH faster than tracing each transaction individually because it avoids
    re-executing the block multiple times.

    Uses default tracer only (struct logs for opcode-level execution).
    Note: prestateTracer is fetched separately per-transaction when needed for storage analysis.

    Returns dict mapping tx_hash -> {struct_trace}
    """
    # Convert block number to hex
    block_hex = hex(block_num)

    session = get_session()

    # Disable memory/storage/stack — analyze_transaction only reads
    # op/depth/gas/gasCost/refund/pc. Empirically 4× faster wall time,
    # 9× smaller response (2.6 GB → 280 MB on Base block 34947733).
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [block_hex, {
            "disableMemory": True,
            "disableStorage": True,
            "disableStack": True,
        }],
        "id": 1
    }

    response = session.post(
        rpc_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=(30, 600)  # Longer timeout for block tracing
    )

    if response.status_code != 200:
        raise RuntimeError(f"Block trace request failed: {response.status_code} {response.text}")

    resp = response.json()

    if "error" in resp:
        raise RuntimeError(f"Block trace error: {resp['error']}")

    struct_results = resp.get("result", [])
    if not isinstance(struct_results, list):
        struct_results = []

    # Map results by transaction hash, indexed under both "0x..." and the bare
    # hex form so callers using either representation get a hit (web3 returns
    # the bare form; eth_getBlockByNumber returns the "0x..." form).
    results = {}

    for trace_result in struct_results:
        if not isinstance(trace_result, dict):
            continue

        tx_hash = trace_result.get('txHash')
        if not tx_hash:
            continue

        entry = {'struct_trace': trace_result.get('result', {})}
        h_lower = tx_hash.lower()
        results[h_lower] = entry
        results[h_lower.removeprefix("0x")] = entry

    return results


def trace_replay_block_state_diffs(block_num: int, rpc_url=RPC_FOR_TRACING) -> Dict[str, Dict]:
    """Fetch state diffs for every transaction in a block in a single RPC call.

    Uses trace_replayBlockTransactions(['stateDiff']) — supported by Reth/Erigon.
    Returns {tx_hash: stateDiff_dict} in parity format (consumed by _parse_trace_replay).

    Benchmark on Base (block 34947733, 267 txs):
      - Per-tx threaded x16:  ~4s
      - JSON-RPC batch (2N):  ~14s
      - This (single call):   ~0.3s
    """
    session = get_session()
    payload = {
        "jsonrpc": "2.0",
        "method": "trace_replayBlockTransactions",
        "params": [hex(block_num), ["stateDiff"]],
        "id": 1,
    }
    response = session.post(rpc_url, json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=(30, 600))
    if response.status_code != 200:
        raise RuntimeError(f"trace_replayBlockTransactions failed: {response.status_code}")
    resp = response.json()
    if "error" in resp:
        raise RuntimeError(f"trace_replayBlockTransactions error: {resp['error']}")
    # Accept both Web3's prefix-less hashes and RPC's 0x-prefixed hashes.
    results: Dict[str, Dict] = {}
    for item in resp.get("result", []) or []:
        tx_hash = item.get("transactionHash")
        if not tx_hash:
            continue
        sd = item.get("stateDiff") or {}
        h_lower = tx_hash.lower()
        results[h_lower] = sd
        results[h_lower.removeprefix("0x")] = sd
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Robust storage/account change extraction
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_call(method: str, params: list[Any], rpc_url: str = RPC_FOR_TRACING):
    resp = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    out = resp.json()
    if "error" in out:
        raise RuntimeError(out["error"])
    return out.get("result")

def _trace_replay_state_diff(tx_hash: str, rpc_url: str = RPC_FOR_TRACING):
    try:
        res = _rpc_call("trace_replayTransaction", [tx_hash, ["stateDiff"]], rpc_url)
        if isinstance(res, dict) and isinstance(res.get("stateDiff"), dict):
            return res["stateDiff"]
    except Exception:
        pass
    return None

def _cast_prestate_diff(tx_hash: str, rpc_url: str = RPC_FOR_TRACING):
    try:
        cmd = [
            "cast","rpc","debug_traceTransaction", tx_hash,
            '{"tracer":"prestateTracer","tracerConfig":{"diffMode":true}}', "--rpc-url", rpc_url
        ]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if raw.returncode != 0:
            return None
        out = json.loads(raw.stdout)
        res = out.get("result", out)
        if isinstance(res, dict):
            return res
    except Exception:
        pass
    return None

def _as_hex(v):
    if v is None: return "0x0"
    if isinstance(v, int): return hex(v)
    s = str(v)
    return s if s else "0x0"

def _hex_to_int(v) -> int:
    s = _as_hex(v)
    try:
        return int(s, 16) if s.startswith("0x") else int(s)
    except Exception:
        return 0

def _is_zero(v) -> bool:
    return _hex_to_int(v) == 0

def _equal_hex_numeric(a, b) -> bool:
    return _hex_to_int(a) == _hex_to_int(b)

def _norm_pair(obj: dict[str, Any]) -> tuple[str, str]:
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
    if "-" in obj:
        return (_as_hex(obj["-"]), "0x0")
    return ("0x0", "0x0")

def _parse_trace_replay(sd: dict[str, Any], debug: bool=False) -> Dict[str, Any]:
    out = {
        'zero_to_nonzero': 0,
        'nonzero_to_zero': 0,
        'nonzero_to_nonzero': 0,
        'net_slots_written': 0,
        'accounts_created': 0,
        'accounts_deleted': 0,
        # StateDiff markers are not exhaustive existence signals on Reth.
        # Exact counts require code-enabled prestate core-state reconstruction.
        'account_births': None,
        'account_deaths': None,
        'bytecode_bytes_allocated': 0,
        'bytecode_bytes_freed': 0,
        'net_bytecode_bytes': 0,
        'storage_by_address': {},
    }
    for addr, changes in sd.items():
        a = {'zero_to_nonzero': 0, 'nonzero_to_zero': 0, 'nonzero_to_nonzero': 0}
        storage = changes.get('storage') or changes.get('storageDiff') or {}
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
            out['zero_to_nonzero']    += a['zero_to_nonzero']
            out['nonzero_to_zero']    += a['nonzero_to_zero']
            out['nonzero_to_nonzero'] += a['nonzero_to_nonzero']

        # Handle bytecode creation/destruction
        code_change = changes.get('code')
        if code_change:
            old_code, new_code = _norm_pair(code_change)
            # Contract creation: code goes from none/0x/empty to bytecode
            if (not old_code or old_code == '0x' or old_code == '0x0' or _is_zero(old_code)) and new_code and new_code != '0x' and new_code != '0x0':
                if new_code.startswith('0x'):
                    try:
                        code_bytes = bytes.fromhex(new_code[2:])
                        out['bytecode_bytes_allocated'] += len(code_bytes)
                    except:
                        pass
            # Contract destruction: code goes from nonzero to zero/empty/None
            if old_code and old_code != '0x' and old_code != '0x0' and (not new_code or new_code == '0x' or new_code == '0x0'):
                if old_code.startswith('0x'):
                    try:
                        destroyed_bytes = len(bytes.fromhex(old_code[2:]))
                        out['bytecode_bytes_freed'] += destroyed_bytes
                    except:
                        pass

        created = False
        for fld in ("code", "balance", "nonce"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if _is_zero(ov) and (not _is_zero(nv)):
                    created = True; break
        if created:
            out['accounts_created'] += 1
        deleted = False
        for fld in ("code", "balance"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if (not _is_zero(ov)) and _is_zero(nv):
                    deleted = True; break
        if deleted:
            out['accounts_deleted'] += 1

    out['net_slots_written'] = out['zero_to_nonzero'] - out['nonzero_to_zero']  # Net growth
    out['net_bytecode_bytes'] = out['bytecode_bytes_allocated'] - out['bytecode_bytes_freed']
    return out

def _parse_prestate(pre: dict[str, Any], debug: bool=False) -> Dict[str, Any]:
    births, deaths = account_transition_counts_from_trace(pre)
    out = {
        'zero_to_nonzero': 0,
        'nonzero_to_zero': 0,
        'nonzero_to_nonzero': 0,
        'net_slots_written': 0,
        'accounts_created': 0,
        'accounts_deleted': 0,
        'account_births': births,
        'account_deaths': deaths,
        'bytecode_bytes_allocated': 0,
        'bytecode_bytes_freed': 0,
        'net_bytecode_bytes': 0,
        'storage_by_address': {},
    }
    root = pre.get('accounts') or pre.get('state') or pre
    if not isinstance(root, dict): return out
    for addr, changes in root.items():
        if not isinstance(changes, dict): continue
        a = {'zero_to_nonzero': 0, 'nonzero_to_zero': 0, 'nonzero_to_nonzero': 0}
        storage = changes.get('storage') or changes.get('storageDiff') or {}
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
            out['zero_to_nonzero']    += a['zero_to_nonzero']
            out['nonzero_to_zero']    += a['nonzero_to_zero']
            out['nonzero_to_nonzero'] += a['nonzero_to_nonzero']

        # Handle bytecode creation/destruction
        code_change = changes.get('code')
        if code_change:
            old_code, new_code = _norm_pair(code_change)
            # Contract creation: code goes from none/0x/empty to bytecode
            if (not old_code or old_code == '0x' or old_code == '0x0' or _is_zero(old_code)) and new_code and new_code != '0x' and new_code != '0x0':
                if new_code.startswith('0x'):
                    try:
                        code_bytes = bytes.fromhex(new_code[2:])
                        out['bytecode_bytes_allocated'] += len(code_bytes)
                    except:
                        pass
            # Contract destruction: code goes from nonzero to zero/empty/None
            if old_code and old_code != '0x' and old_code != '0x0' and (not new_code or new_code == '0x' or new_code == '0x0'):
                if old_code.startswith('0x'):
                    try:
                        destroyed_bytes = len(bytes.fromhex(old_code[2:]))
                        out['bytecode_bytes_freed'] += destroyed_bytes
                    except:
                        pass

        # Same transitions as _parse_diff above.
        created = False
        for fld in ("code", "balance", "nonce"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if _is_zero(ov) and (not _is_zero(nv)):
                    created = True; break
        if created:
            out['accounts_created'] += 1
        deleted = False
        for fld in ("code", "balance"):
            v = changes.get(fld)
            if isinstance(v, dict):
                ov, nv = _norm_pair(v)
                if (not _is_zero(ov)) and _is_zero(nv):
                    deleted = True; break
        if deleted:
            out['accounts_deleted'] += 1
    out['net_slots_written'] = out['zero_to_nonzero'] - out['nonzero_to_zero']  # Net growth
    out['net_bytecode_bytes'] = out['bytecode_bytes_allocated'] - out['bytecode_bytes_freed']
    return out

_EMPTY_STATE_CHANGES = {
    'zero_to_nonzero': 0,
    'nonzero_to_zero': 0,
    'nonzero_to_nonzero': 0,
    'net_slots_written': 0,
    'accounts_created': 0,
    'accounts_deleted': 0,
    # Both users of this default mean "no state diff was obtained", not "the diff
    # was empty", so births are unknown rather than zero.
    'account_births': None,
    'account_deaths': None,
    'bytecode_bytes_allocated': 0,
    'bytecode_bytes_freed': 0,
    'net_bytecode_bytes': 0,
    'storage_by_address': {},
}


class AuthorizationRefundUnavailable(RuntimeError):
    """A type-4 transaction's authorization refund could not be derived.

    Raised instead of recording a zero refund: the row would be wrong, and the
    caller should retry or skip the transaction rather than persist it.
    """


def _state_changes_from_raw(sd, debug: bool=False, pre_balances: Dict=None) -> Dict:
    """Parse a Parity stateDiff and retain the raw diff.

    The raw diff must survive: the EIP-7702 authorization refund needs each
    authority's pre-transaction nonce/code/existence, which only the diff
    carries. Dropping it silently zeroes the refund.

    pre_balances: {address: balance} in force immediately BEFORE this
    transaction, reconstructed across the block. Supplied by the batch path so
    an authority moved across zero earlier in the same block resolves correctly.
    """
    # None means no diff was obtained; {} means the node reported a diff that
    # touched nothing. Only the first is unknown -- an empty diff parses to
    # verified zeros, so conflating them would discard a real measurement.
    if sd is None:
        return dict(_EMPTY_STATE_CHANGES)
    out = _parse_trace_replay(sd, debug=debug)
    out['_raw_state_diff'] = sd
    if pre_balances is not None:
        out['_pre_balances'] = pre_balances
    return out


def eip7623_floor_adjustment(calldata, gas_after_refund,
                             block_number, chain) -> int:
    """EIP-7623 calldata-floor top-up, or 0 when the floor does not bind.

    tx.gasUsed = max(gas_after_refund, 21000 + 10*tokens), tokens =
    zero_bytes + 4*nonzero_bytes. The floor binds on POST-refund gas and sits
    outside the EIP-3529 refund cap, and is never written as a parquet column
    -- anything reconstructing net gas from stored columns must call this too.
    """
    pectra_block = PECTRA_FORK_BLOCKS.get(normalize_chain_name(chain))
    if pectra_block is None or block_number < pectra_block or not calldata:
        return 0
    hx = calldata.hex() if hasattr(calldata, 'hex') else str(calldata)
    if hx.startswith('0x'):
        hx = hx[2:]
    if not hx:
        return 0
    data = bytes.fromhex(hx)
    zero = sum(1 for b in data if b == 0)
    tokens = zero + ((len(data) - zero) * 4)

    floor_gas = 21000 + (10 * tokens)
    return max(0, floor_gas - gas_after_refund)


def block_authority_pre_balances(block_txs, raw_state, w3_local) -> Dict[str, Dict]:
    """Per-transaction authority balances for a block, in transaction order.

    Only type-4 transactions need this, so each authority is read once at the
    parent block and the block's own diffs are then replayed forward. Returns
    {tx_hash: {authority: balance}}; empty when the block has no type-4.
    """
    ordered = []
    needed = {}
    for tx in block_txs:
        h = tx['hash'].hex() if hasattr(tx['hash'], 'hex') else str(tx['hash'])
        ordered.append(h)
        auths = {a for a in (recover_authority(dict(t))
                             for t in (tx.get('authorizationList') or [])) if a}
        if auths:
            needed[h] = auths
    if not needed:
        return {}

    parent = max(0, int(block_txs[0]['blockNumber']) - 1)
    all_authorities = set().union(*needed.values())
    seed = {a: w3_local.eth.get_balance(Web3.to_checksum_address(a),
                                       block_identifier=parent)
            for a in all_authorities}
    diffs = [raw_state.get(h) or raw_state.get(h.lower())
             or raw_state.get(h[2:] if h.startswith('0x') else '0x' + h) or {}
             for h in ordered]
    per_tx = balances_before_each_tx(diffs, seed)
    # Only type-4 transactions need this, and only their own authorities: the
    # block's diffs touch thousands of unrelated addresses.
    by_hash = dict(zip(ordered, per_tx))
    return {h: {a: by_hash[h][a] for a in auths if a in by_hash[h]}
            for h, auths in needed.items()}


def get_storage_and_account_changes(tx_hash: str, rpc_url: str = RPC_FOR_TRACING, debug: bool=False) -> Dict:
    sd = _trace_replay_state_diff(tx_hash, rpc_url)
    pre = _cast_prestate_diff(tx_hash, rpc_url)
    if sd is not None:
        out = _state_changes_from_raw(sd, debug=debug)
        if pre is not None:
            out.update(_exact_account_counts(pre))
        return out
    if pre is not None:
        return _parse_prestate(pre, debug=debug)
    return dict(_EMPTY_STATE_CHANGES)


def batch_get_storage_and_account_changes(tx_hashes: List[str], rpc_url: str = RPC_FOR_TRACING,
                                          max_workers: int = 16) -> Dict[str, Dict]:
    """Parallel fetch of per-tx state/storage changes.

    ~10× faster than calling get_storage_and_account_changes in a sequential loop
    because each call is network-bound. ThreadPool beats JSON-RPC batching for
    this workload because nodes serialize batched debug calls internally.
    """
    from concurrent.futures import ThreadPoolExecutor
    if not tx_hashes:
        return {}
    out: Dict[str, Dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for tx_hash, result in zip(
            tx_hashes,
            ex.map(lambda h: get_storage_and_account_changes(h, rpc_url=rpc_url), tx_hashes),
        ):
            out[tx_hash] = result
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Refund Helpers
# ──────────────────────────────────────────────────────────────────────────────
def extract_initial_refund_from_trace(structlogs: List[Dict]) -> int:
    """
    Refund counter value BEFORE the first opcode executes.

    Clients differ on what is already in the counter at this point. Erigon
    credits the EIP-7702 authorization refund during authorization processing,
    which precedes execution, so its counter starts at 12,500 per existing
    authority. Reth reports 0 throughout. Subtracting this initial value yields
    the execution (SSTORE) refund alone on either client, so the authorization
    refund can be added once from the protocol derivation without
    double-counting.
    """
    if not structlogs:
        return 0
    UINT64_WRAP_THRESHOLD = (2**64 - 1) - 1000000
    for log in structlogs:
        if log.get('depth', 1) == 1:
            v = log.get('refund', 0) or 0
            return v if v <= UINT64_WRAP_THRESHOLD else 0
    return 0


def extract_refund_from_trace(structlogs: List[Dict], receipt: Dict) -> int:
    """
    Extract uncapped gas refund from transaction structLogs.

    Raw final counter: on some clients this already includes the EIP-7702
    authorization refund. Use combine_refunds() to reconcile type-4 gas.

    Per Ethereum spec:
    - Refunds only apply to successful transactions (status=1)
    - We extract from depth-1 log to avoid counting refunds from reverted subcalls
    - Filter out uint64 wraparound values (negative refunds in trace)

    Parameters
    ----------
    structlogs : List[Dict]
        Transaction structLogs from debug_traceTransaction
    receipt : Dict
        Transaction receipt

    Returns
    -------
    int
        Uncapped refund amount from trace (0 for failed transactions)
    """
    tx_success = receipt.get('status') == 1 or receipt.get('status') == '0x1'

    if not tx_success or not structlogs:
        return 0

    UINT64_MAX = 2**64 - 1
    UINT64_WRAP_THRESHOLD = UINT64_MAX - 1000000

    # Iterate backwards to find depth-1 refund (handles reverted subcalls)
    for log in reversed(structlogs):
        depth = log.get('depth', 1)
        if depth == 1:
            refund_value = log.get('refund', 0)
            # Filter out uint64 wraparound (negative refunds)
            if refund_value <= UINT64_WRAP_THRESHOLD:
                return refund_value

    return 0


def apply_refund_cap_eip3529(refund_uncapped: int, gas_before_refund: int) -> int:
    """
    Apply EIP-3529 refund cap (20% of gas before refunds).

    EIP-3529 reduced the refund cap from 50% to 20% of total gas used.

    Parameters
    ----------
    refund_uncapped : int
        Uncapped refund from trace
    gas_before_refund : int
        Total gas before refunds (intrinsic + execution)

    Returns
    -------
    int
        Capped refund amount (min of uncapped and 20% cap)
    """
    refund_cap = gas_before_refund // 5
    return min(refund_uncapped, refund_cap)

def _sum_gascost(structlogs: List[Dict]) -> int:
    s=0
    for l in structlogs or []:
        try: s += int(l.get("gasCost",0) or 0)
        except: pass
    return s

def _classify_halt(struct_logs):
    if not struct_logs: return "unknown"
    op = (struct_logs[-1].get("op") or "").upper()
    if op in ("STOP","RETURN","SELFDESTRUCT"): return "success"
    if op == "REVERT": return "revert"
    if op in ("INVALID","ASSERTFAIL"): return "invalid"
    return "other"

# ──────────────────────────────────────────────────────────────────────────────
# Gas constants & sets
# ──────────────────────────────────────────────────────────────────────────────
G_TRANSACTION   = 21000
G_TXDATAZERO    = 4
G_TXDATANONZERO = 16
G_TXCREATE      = 32000
ACCESS_LIST_ADDRESS_COST     = 2400
ACCESS_LIST_STORAGE_KEY_COST = 1900

CALL_FAM   = {'CALL','CALLCODE','DELEGATECALL','STATICCALL','CREATE','CREATE2'}
CREATE_SET = {'CREATE','CREATE2'}

# Opcodes to track for access counting
ACCOUNT_ACCESS_OPS = {'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH',
                      'CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'SELFDESTRUCT'}
STORAGE_ACCESS_OPS = {'SLOAD', 'SSTORE'}

# EIP-2929 Gas costs (Berlin fork onwards)
COLD_ACCOUNT_ACCESS_COST = 2600
WARM_ACCOUNT_ACCESS_COST = 100
COLD_SLOAD_COST = 2100
WARM_SLOAD_COST = 100

# SSTORE costs (EIP-2929 + EIP-2200)
SSTORE_SET_COST = 20000          # Setting slot from zero to non-zero
SSTORE_RESET_COST = 2900         # Changing non-zero value (cold)

# ──────────────────────────────────────────────────────────────────────────────
# Intrinsic & helpers
# ──────────────────────────────────────────────────────────────────────────────
def calculate_access_list_gas(tx: Dict) -> tuple:
    access_list = tx.get('accessList', []) or []
    if not access_list: return 0, [], set(), set()
    total=0; breakdown=[]; prewarm_addrs=set(); prewarm_slots=set()
    for i,e in enumerate(access_list):
        addr=(e.get('address') or "").lower()
        keys=e.get('storageKeys',[]) or []
        g = ACCESS_LIST_ADDRESS_COST + len(keys)*ACCESS_LIST_STORAGE_KEY_COST
        total += g
        breakdown.append({'index':i,'address':addr,'num_keys':len(keys),
                          'address_gas':ACCESS_LIST_ADDRESS_COST,'keys_gas':len(keys)*ACCESS_LIST_STORAGE_KEY_COST,
                          'total_gas':g})
        if addr:
            prewarm_addrs.add(addr)
            for key in keys:
                prewarm_slots.add((addr, key.lower() if isinstance(key, str) else key))
    return total, breakdown, prewarm_addrs, prewarm_slots

def eip3860_initcode_cost_for_creation_tx(tx: dict[str, Any]) -> int:
    to_field = tx.get("to")
    is_create = (to_field is None) or (isinstance(to_field,str) and to_field.strip().lower() in ("","0x","0x0"))
    if not is_create: return 0
    data_hex = (tx.get("input") or "0x")
    if hasattr(data_hex,"hex"): data_hex = data_hex.hex()  # HexBytes.hex() has NO '0x' prefix
    data_hex = str(data_hex).lower()
    if data_hex.startswith("0x"): data_hex = data_hex[2:]  # strip prefix only if present
    if data_hex == "": return 0
    nbytes = len(data_hex)//2
    INITCODE_MAX_BYTES = 49152
    if nbytes > INITCODE_MAX_BYTES:
        print(f"WARNING: EIP-3860: initcode size {nbytes} > {INITCODE_MAX_BYTES}")
    words = (nbytes + 31)//32
    return 2*words

# EIP-7702 Gas costs
PER_AUTH_BASE_COST = 12500
PER_AUTH_PROCESSING_COST = 12500

def calculate_authorization_list_gas(tx: Dict) -> tuple:
    authorization_list = tx.get('authorizationList', []) or []
    if not authorization_list:
        return 0, [], set()

    total_gas = 0
    breakdown = []
    delegation_targets = set()

    # `address` is the DELEGATION TARGET, not the authority. The authority is
    # recovered from the tuple signature in utils/eip7702_authorization.py, which
    # is also where refund eligibility is decided. Intrinsic gas is
    # PER_EMPTY_ACCOUNT_COST per tuple unconditionally, so nothing here depends on
    # authority state.
    for i, auth in enumerate(authorization_list):
        target = auth.get('address', 'N/A')
        auth_gas = PER_AUTH_BASE_COST + PER_AUTH_PROCESSING_COST
        total_gas += auth_gas
        breakdown.append({
            'index': i,
            'delegation_target': target,
            'chain_id': auth.get('chainId', 'N/A'),
            'nonce': auth.get('nonce', 'N/A'),
            'gas': auth_gas,
        })
        if target and target != 'N/A':
            delegation_targets.add(target.lower())

    return total_gas, breakdown, delegation_targets

def calculate_intrinsic_gas(tx: Dict) -> tuple:
    """
    Calculate intrinsic gas (base + calldata + creation).

    Note: EIP-7623 floor is NOT applied here - it's applied to the total
    gas (intrinsic + execution) in the final calculation.
    """
    intrinsic = G_TRANSACTION
    calldata_zero_gas = 0
    calldata_nonzero_gas = 0
    creation_gas = 0

    if tx.get('input') and tx['input'] != '0x':
        hx = tx['input'].hex() if hasattr(tx['input'],'hex') else str(tx['input'])
        if hx.startswith('0x'): hx = hx[2:]
        data = bytes.fromhex(hx)
        zero = sum(1 for b in data if b==0)
        nonz = len(data)-zero

        # Standard calldata cost (4 gas/zero, 16 gas/nonzero)
        calldata_zero_gas = zero * G_TXDATAZERO
        calldata_nonzero_gas = nonz * G_TXDATANONZERO
        intrinsic += calldata_zero_gas + calldata_nonzero_gas

    if tx.get('to') is None:
        creation_gas = G_TXCREATE
        intrinsic += creation_gas

    return intrinsic, calldata_zero_gas, calldata_nonzero_gas, creation_gas

def calculate_contract_creation_storage_gas(receipt: Dict) -> int:
    addr = receipt.get('contractAddress')
    if addr:
        # contractAddress from a raw JSON-RPC receipt is lowercase; web3 rejects
        # non-checksummed addresses, so convert before the call.
        code = w3.eth.get_code(Web3.to_checksum_address(addr))
        return len(code) * 200
    return 0

# ──────────────────────────────────────────────────────────────────────────────
# Exact per-op breakdown (modes)
# ──────────────────────────────────────────────────────────────────────────────
def _find_return_index(structlogs: List[Dict], start_i: int, start_depth: int) -> int:
    n = len(structlogs)
    for j in range(start_i + 1, n):
        if int(structlogs[j].get("depth", 0) or 0) == start_depth:
            return j
    return -1

def _exec_total_from_structlogs(structlogs: List[Dict]) -> int:
    if not structlogs:
        return 0
    try:
        g0 = int(structlogs[0].get("gas", 0) or 0)
        gN = int(structlogs[-1].get("gas", 0) or 0)
        return max(0, g0 - gN)
    except Exception:
        return 0

def _sum_direct_children_frame_net(structlogs: List[Dict], i0: int, i1: int, parent_depth: int) -> int:
    """
    Sum net gas of direct child CALL/CREATE frames (depth = parent_depth + 1)
    inside (i0, i1). Net = gas_before - gas_at_return_same_depth for that child.
    """
    if i1 < 0: i1 = len(structlogs)
    child_depth = parent_depth + 1
    k = i0 + 1
    total = 0
    while k < i1:
        log = structlogs[k]
        d = int(log.get("depth", 0) or 0)
        if d <= parent_depth:
            break
        if d == child_depth and (log.get("op") or "").upper() in CALL_FAM:
            g_before = int(log.get("gas", 0) or 0)
            r = _find_return_index(structlogs, k, child_depth)
            g_after = int(structlogs[r].get("gas", 0) or 0) if r != -1 else int(structlogs[-1].get("gas", 0) or 0)
            total += max(0, g_before - g_after)
            k = r if r != -1 else i1
        else:
            k += 1
    return total

def _effective_op_gas(log: Dict) -> int:
    """Gas actually charged by one structLog step.

    An opcode that halts out-of-gas cannot charge more than the gas remaining in its
    frame. reth reports the actually-burned gas in `gasCost`, but erigon/geth report
    the NOMINAL cost (e.g. a failed SSTORE shows 20000 even though only ~15k remained),
    which inflates execution_gas_total and per-op columns and shows up as difference!=0.
    When the step has an `error`, clamp to `gas` (gas remaining BEFORE the op). This is
    a no-op for steps without error and for reth (which already reports burned gas).
    """
    try:
        cost = int(log.get('gasCost', 0) or 0)
    except (TypeError, ValueError):
        return 0
    if log.get('error'):
        try:
            avail = int(log.get('gas', 0) or 0)
        except (TypeError, ValueError):
            return cost
        if 0 <= avail < cost:
            return avail
    return cost


def _sum_non_call_opcodes_in_frame(structlogs: List[Dict], i0: int, i1: int, parent_depth: int) -> int:
    """
    Sum gasCost of all non-CALL/CREATE opcodes at depth = parent_depth + 1 within (i0, i1),
    EXCLUDING opcodes that are inside child CALL/CREATE frames.

    This gives us the "direct" non-CALL opcodes cost for this frame.
    """
    if i1 < 0: i1 = len(structlogs)
    child_depth = parent_depth + 1
    total = 0
    k = i0 + 1

    while k < i1:
        log = structlogs[k]
        d = int(log.get("depth", 0) or 0)

        if d <= parent_depth:
            break

        op = (log.get("op") or "").upper()

        # If we hit a CALL/CREATE at child_depth, skip its entire frame
        if d == child_depth and op in CALL_FAM:
            # Find where this child frame ends
            r = _find_return_index(structlogs, k, child_depth)
            k = r if r != -1 else i1
        # Count non-CALL opcodes at exactly child_depth
        elif d == child_depth and op not in CALL_FAM:
            total += _effective_op_gas(log)
            k += 1
        else:
            # Skip deeper nested opcodes (they're in grandchild frames or deeper)
            k += 1

    return total

def compute_opcode_gas_custom(
    tx: Dict,
    receipt: Dict,
    struct_trace: Dict,
    intrinsic_gas: int,
    debug: bool = False,
    extra_implicit_gas: int = 0
):
    """
    Depth-direct mode: All-depth per-op accounting
    - Non-CALL opcodes: sum gasCost at all depths
    - CALL/CREATE: net(frame) - sum(net(direct children)) - non-CALL opcodes in frame
    This gives pure CALL/CREATE overhead without double-counting internal execution.
    """
    structlogs = (struct_trace.get('result') or {}).get('structLogs') or struct_trace.get('structLogs') or []
    per_op_noncall: Dict[str, int] = defaultdict(int)
    per_op_call: Dict[str, int] = defaultdict(int)
    used_root_fallback = False

    # Track access counts (total and cold)
    account_access_counts: Dict[str, int] = defaultdict(int)
    storage_access_counts: Dict[str, int] = defaultdict(int)
    account_cold_counts: Dict[str, int] = defaultdict(int)
    storage_cold_counts: Dict[str, int] = defaultdict(int)

    actual_gas_used = receipt.get('gasUsed', 0)
    gas_limit = tx.get('gas', 0)
    is_out_of_gas = (actual_gas_used == gas_limit) and (receipt.get('status', 1) == 0)

    # Handle empty structlogs (EOA transfers, precompile calls, immediate failures)
    if not structlogs or len(structlogs) == 0:
        # Subtract extra_implicit_gas (eip3860 initcode + contract_creation): it's part of
        # actual_gas_used but accounted separately in gross_gas. Without this, an immediate
        # failed creation (empty trace, OOG) double-counts the initcode gas here too.
        execution_gas = max(0, actual_gas_used - intrinsic_gas - extra_implicit_gas)

        # Check if this is a precompile call
        to_address = tx.get('to')
        precompile_name = is_precompile(to_address) if to_address else None

        tx_failed = receipt.get('status', 1) == 0
        if execution_gas > 0:
            # Only count as execution if out-of-gas or precompile
            if is_out_of_gas:
                per_op_noncall['OUT_OF_GAS'] = execution_gas
                non_call_gas = execution_gas
                call_overhead = 0
            elif precompile_name:
                # Precompile execution gas - treat as call gas
                per_op_call[f'PRECOMPILE_{precompile_name}'] = execution_gas
                non_call_gas = 0
                call_overhead = execution_gas
            elif tx_failed:
                # Preserve otherwise unattributable gas from a failed empty trace.
                per_op_noncall['EXCEPTIONAL_HALT_IMPLICIT'] = execution_gas
                non_call_gas = execution_gas
                call_overhead = 0
            else:
                # For successful EOA calls, remaining gas should be from EIP-7623
                non_call_gas = 0
                call_overhead = 0
            used_root_fallback = True
        else:
            non_call_gas = 0
            call_overhead = 0
        create_overhead = 0
        exec_total = non_call_gas + call_overhead
        return (
            exec_total,
            non_call_gas,
            call_overhead,
            create_overhead,
            per_op_noncall,
            per_op_call,
            used_root_fallback,
            account_access_counts,
            storage_access_counts,
            account_cold_counts,
            storage_cold_counts
        )

    # ──────────────── DEPTH_DIRECT MODE ────────────────
    n = len(structlogs)

    # First: count all non-CALL opcodes at all depths + track accesses
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
            # Cold if gas cost is 2600 (account access). For CALL-family ops the
            # structLog gasCost includes forwarded gas, so this never matches there:
            # the count is a lower bound that misses CALL-family cold accesses.
            if gas_cost == COLD_ACCOUNT_ACCESS_COST:
                account_cold_counts[op] += 1

        # Track storage accesses
        if op in STORAGE_ACCESS_OPS:
            storage_access_counts[op] += 1
            gas_cost = int(log.get('gasCost', 0) or 0)
            # For SLOAD: cold = 2100, warm = 100
            if op == 'SLOAD' and gas_cost == COLD_SLOAD_COST:
                storage_cold_counts[op] += 1
            # For SSTORE: post-Berlin costs come in warm/cold pairs
            # (100/2200 no-op or dirty, 2900/5000 reset, 20000/22100 set);
            # cold is exactly warm cost + 2100 cold-access surcharge.
            elif op == 'SSTORE' and gas_cost in (2200, 5000, 22100):
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
        # This gives us just the CALL setup/teardown cost
        self_only = max(0, parent_net - child_frames_sum - non_call_in_frame)

        per_op_call[op] += self_only

        if debug:
            print(f"  {op}@{i} d={depth}: net={parent_net:,} children={child_frames_sum:,} non_call={non_call_in_frame:,} self={self_only:,}")

    non_call_gas = sum(per_op_noncall.values())
    call_overhead  = sum(v for k,v in per_op_call.items() if k in {'CALL','CALLCODE','DELEGATECALL','STATICCALL'})
    create_overhead= sum(v for k,v in per_op_call.items() if k in {'CREATE','CREATE2'})
    exec_total = non_call_gas + call_overhead + create_overhead

    # ─────────── Calculate refunds FIRST ───────────
    # We need to calculate this before implicit costs because receipt.gasUsed is AFTER refunds
    refund_uncapped = extract_refund_from_trace(structlogs, receipt)

    # Apply preliminary cap based on opcodes (before implicit costs)
    # This prevents using uncapped refund in implicit cost calculation
    refund = apply_refund_cap_eip3529(refund_uncapped, intrinsic_gas + exec_total)

    # Handle implicit costs and OUT OF GAS
    if is_out_of_gas:
        # For out-of-gas, refund is already included in actual_gas_used accounting.
        # Subtract extra_implicit_gas (eip3860 initcode + contract_creation storage gas):
        # these are part of actual_gas_used but are accounted SEPARATELY in gross_gas, so
        # the synthetic OUT_OF_GAS_PENALTY must not absorb them (else gross_gas double-counts
        # them -> difference == -extra_implicit_gas on failed creations).
        true_execution_gas = max(0, actual_gas_used - intrinsic_gas - extra_implicit_gas + refund)
        adjustment = true_execution_gas - exec_total

        if adjustment > 0:
            per_op_noncall['OUT_OF_GAS_PENALTY'] = adjustment
            non_call_gas += adjustment
            exec_total += adjustment

            if debug:
                print(f"     OUT OF GAS penalty:       {adjustment:,}")
    elif structlogs:
        # Handle implicit costs for REVERT/RETURN/STOP/SELFDESTRUCT and exceptional halts
        # Since receipt.gasUsed is AFTER refunds: gasUsed = intrinsic + execution - refund
        # Therefore: execution = gasUsed - intrinsic + refund
        last_op = structlogs[-1].get('op', '').upper()
        tx_failed = receipt.get('status', 1) == 0

        # Check if this is a terminal opcode OR an exceptional halt (trace failed but no terminal op)
        is_terminal_op = last_op in ('REVERT', 'RETURN', 'STOP', 'SELFDESTRUCT')
        is_exceptional_halt = tx_failed and not is_terminal_op

        if is_terminal_op or is_exceptional_halt:
            # +extra_implicit_gas: eip3860/contract_creation gas is in actual_gas_used but
            # accounted separately in gross_gas; exclude it from the implicit-cost residual
            # so gross_gas doesn't double-count it.
            expected_gas = intrinsic_gas + exec_total - refund + extra_implicit_gas
            implicit_cost = max(0, actual_gas_used - expected_gas)

            # Skip implicit cost only for SUCCESSFUL contract creations (it's accounted in contract_creation_gas)
            # For FAILED contract creations (REVERT), we need to add the implicit cost
            is_contract_creation = (tx.get('to') is None)

            # Add implicit cost if:
            # 1. Not a contract creation, OR
            # 2. Is a contract creation but failed (no deployment cost, so implicit cost applies)
            if implicit_cost > 0 and (not is_contract_creation or tx_failed):
                op_label = last_op if is_terminal_op else 'EXCEPTIONAL_HALT'
                per_op_noncall[f'{op_label}_IMPLICIT'] = implicit_cost
                non_call_gas += implicit_cost
                exec_total += implicit_cost

                if debug:
                    print(f"     {op_label} implicit cost:  {implicit_cost:,}")

    return (
        exec_total,
        non_call_gas,
        call_overhead,
        create_overhead,
        per_op_noncall,
        per_op_call,
        used_root_fallback,
        account_access_counts,
        storage_access_counts,
        account_cold_counts,
        storage_cold_counts
    )

# ──────────────────────────────────────────────────────────────────────────────
# Access pattern validation
# ──────────────────────────────────────────────────────────────────────────────
def validate_access_costs(
    tx: Dict,
    per_op_noncall: Dict[str, int],
    account_access_counts: Dict[str, int],
    storage_access_counts: Dict[str, int],
    access_list_addresses: Set[str],
    access_list_storage_keys: Set[Tuple[str, str]],
    account_cold_counts: Dict[str, int] = None,
    storage_cold_counts: Dict[str, int] = None
) -> Dict:
    """
    Validate account and storage access costs against expected warm/cold patterns.

    Returns dict with validation results including:
    - Estimated warm/cold access breakdown
    - Comparison against actual gas costs
    - Warnings for anomalies
    """
    validation = {
        'account_accesses': {},
        'storage_accesses': {},
        'warnings': [],
        'estimated_warm_cold_breakdown': {}
    }

    # Validate account accesses (excluding CALL family which have complex gas calculations)
    simple_account_ops = {'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH'}

    for op in simple_account_ops:
        if op not in account_access_counts or account_access_counts[op] == 0:
            continue

        count = account_access_counts[op]
        actual_gas = per_op_noncall.get(op, 0)

        # Calculate expected gas assuming all cold
        all_cold_gas = count * COLD_ACCOUNT_ACCESS_COST
        # Calculate expected gas assuming all warm
        all_warm_gas = count * WARM_ACCOUNT_ACCESS_COST

        # Estimate warm/cold split
        # For EXTCODECOPY, there's additional memory expansion cost, so we can't validate precisely
        if op == 'EXTCODECOPY':
            validation['warnings'].append(
                f"{op}: Cannot precisely validate due to memory expansion costs (count={count}, gas={actual_gas:,})"
            )
            continue

        if actual_gas == 0:
            validation['warnings'].append(f"{op}: Zero gas cost with {count} accesses - possible error")
            continue

        avg_gas_per_access = actual_gas / count if count > 0 else 0

        # Estimate number of cold vs warm accesses
        # This is an approximation: (cold_count * 2600 + warm_count * 100) = actual_gas
        # cold_count + warm_count = count
        # Solving: cold_count = (actual_gas - warm_gas_total) / (2600 - 100)
        estimated_cold = max(0, min(count, (actual_gas - all_warm_gas) / (COLD_ACCOUNT_ACCESS_COST - WARM_ACCOUNT_ACCESS_COST)))
        estimated_warm = count - estimated_cold

        result = {
            'count': count,
            'actual_gas': actual_gas,
            'avg_gas_per_access': round(avg_gas_per_access, 2),
            'estimated_cold_accesses': round(estimated_cold, 2),
            'estimated_warm_accesses': round(estimated_warm, 2),
            'expected_all_cold_gas': all_cold_gas,
            'expected_all_warm_gas': all_warm_gas,
        }

        # Add actual cold count if available
        if account_cold_counts is not None:
            actual_cold = account_cold_counts.get(op, 0)
            result['actual_cold_accesses'] = actual_cold
            result['actual_warm_accesses'] = count - actual_cold
            # Compare estimated vs actual
            if abs(estimated_cold - actual_cold) > 0.5:
                validation['warnings'].append(
                    f"{op}: Estimated cold ({estimated_cold:.1f}) differs from actual cold ({actual_cold})"
                )

        validation['account_accesses'][op] = result

        # Add warning if gas doesn't make sense
        if actual_gas > all_cold_gas:
            validation['warnings'].append(
                f"{op}: Actual gas ({actual_gas:,}) exceeds all-cold estimate ({all_cold_gas:,})"
            )
        elif actual_gas < all_warm_gas:
            validation['warnings'].append(
                f"{op}: Actual gas ({actual_gas:,}) less than all-warm estimate ({all_warm_gas:,})"
            )

    # Validate storage accesses
    for op in ['SLOAD', 'SSTORE']:
        if op not in storage_access_counts or storage_access_counts[op] == 0:
            continue

        count = storage_access_counts[op]
        actual_gas = per_op_noncall.get(op, 0)

        if actual_gas == 0:
            validation['warnings'].append(f"{op}: Zero gas cost with {count} accesses - possible error")
            continue

        avg_gas_per_access = actual_gas / count if count > 0 else 0

        if op == 'SLOAD':
            all_cold_gas = count * COLD_SLOAD_COST
            all_warm_gas = count * WARM_SLOAD_COST

            # Estimate warm/cold split
            estimated_cold = max(0, min(count, (actual_gas - all_warm_gas) / (COLD_SLOAD_COST - WARM_SLOAD_COST)))
            estimated_warm = count - estimated_cold

            result = {
                'count': count,
                'actual_gas': actual_gas,
                'avg_gas_per_access': round(avg_gas_per_access, 2),
                'estimated_cold_accesses': round(estimated_cold, 2),
                'estimated_warm_accesses': round(estimated_warm, 2),
                'expected_all_cold_gas': all_cold_gas,
                'expected_all_warm_gas': all_warm_gas,
            }

            # Add actual cold count if available
            if storage_cold_counts is not None:
                actual_cold = storage_cold_counts.get(op, 0)
                result['actual_cold_accesses'] = actual_cold
                result['actual_warm_accesses'] = count - actual_cold
                # Compare estimated vs actual
                if abs(estimated_cold - actual_cold) > 0.5:
                    validation['warnings'].append(
                        f"SLOAD: Estimated cold ({estimated_cold:.1f}) differs from actual cold ({actual_cold})"
                    )

            validation['storage_accesses'][op] = result

            if actual_gas > all_cold_gas:
                validation['warnings'].append(
                    f"SLOAD: Actual gas ({actual_gas:,}) exceeds all-cold estimate ({all_cold_gas:,})"
                )
            elif actual_gas < all_warm_gas:
                validation['warnings'].append(
                    f"SLOAD: Actual gas ({actual_gas:,}) less than all-warm estimate ({all_warm_gas:,})"
                )

        elif op == 'SSTORE':
            # SSTORE is complex: costs include both access cost AND state transition cost
            # Access cost: 2,900 (cold) or 100 (warm)
            # State transition cost: varies (0 for no change, 20,000 for zero→non-zero, etc.)
            # We can estimate the cold/warm access split, but the total includes transition costs

            # Estimate cold/warm assuming base SSTORE costs
            # Using SSTORE_RESET_COST (2900) as cold base cost
            all_warm_gas = count * WARM_SLOAD_COST
            all_cold_gas = count * SSTORE_RESET_COST

            # Rough estimate of cold/warm access component (ignoring state transition costs)
            # This is approximate because state transition costs dominate
            estimated_cold = max(0, min(count, (actual_gas - all_warm_gas) / (SSTORE_RESET_COST - WARM_SLOAD_COST)))
            estimated_warm = count - estimated_cold

            # Min/max possible including state transitions
            min_possible = count * WARM_SLOAD_COST  # All warm, no state changes
            max_possible = count * (SSTORE_SET_COST + SSTORE_RESET_COST)  # All cold zero→non-zero

            validation['storage_accesses'][op] = {
                'count': count,
                'actual_gas': actual_gas,
                'avg_gas_per_access': round(avg_gas_per_access, 2),
                'estimated_cold_accesses': round(estimated_cold, 2),
                'estimated_warm_accesses': round(estimated_warm, 2),
                'note': 'Total cost includes state transition costs (zero→non-zero, etc.)',
                'min_possible_gas': min_possible,
                'max_possible_gas': max_possible,
            }

            if actual_gas > max_possible:
                validation['warnings'].append(
                    f"SSTORE: Actual gas ({actual_gas:,}) exceeds maximum estimate ({max_possible:,})"
                )
            elif actual_gas < min_possible:
                validation['warnings'].append(
                    f"SSTORE: Actual gas ({actual_gas:,}) less than minimum estimate ({min_possible:,})"
                )

    return validation

# ──────────────────────────────────────────────────────────────────────────────
# Main analyzer
# ──────────────────────────────────────────────────────────────────────────────
def analyze_transaction(tx_hash: str, silent: bool = False, debug: bool = False,
                       prefetched_traces: Dict = None, prefetched_tx=None, prefetched_receipt=None,
                       prefetched_state_changes: Dict = None):
    if not silent:
        print("\n" + "="*70)
        print(f"Analyzing Transaction: {tx_hash}")
        print("="*70 + "\n")

    # Use prefetched tx/receipt if available to avoid RPC calls
    if prefetched_tx is not None:
        tx = prefetched_tx
    else:
        tx = w3.eth.get_transaction(tx_hash)

    if prefetched_receipt is not None:
        receipt = prefetched_receipt
    else:
        receipt = w3.eth.get_transaction_receipt(tx_hash)

    if not tx or not receipt:
        raise ValueError("Transaction not found")

    actual_gas = receipt['gasUsed']

    if not silent:
        print(f"From:      {tx['from']}")
        print(f"To:        {tx['to'] or 'Contract Creation'}")
        print(f"Value:     {Web3.from_wei(tx['value'], 'ether')} ETH")
        if tx.get('gasPrice') is not None:
            print(f"Gas Price: {Web3.from_wei(tx['gasPrice'], 'gwei')} Gwei")
        print(f"Gas Limit: {tx['gas']}")
        print(f"\nActual Gas Used (receipt): {receipt['gasUsed']}")
        print(f"Status: {'SUCCESS' if receipt.get('status', 1) == 1 else 'FAILED'}")

    intrinsic_gas, calldata_zero_gas, calldata_nonzero_gas, creation_gas = calculate_intrinsic_gas(tx)
    access_list_gas, access_list_breakdown, access_list_addrs, access_list_slots = calculate_access_list_gas(tx)
    authorization_list_gas, authorization_list_breakdown, _ = calculate_authorization_list_gas(tx)

    intrinsic_gas_total = intrinsic_gas + access_list_gas + authorization_list_gas
    eip3860_init_gas = eip3860_initcode_cost_for_creation_tx(tx)

    if not silent:
        print("\n" + "="*70)
        print(f"1. INTRINSIC GAS: {intrinsic_gas_total}")
        print("="*70)
        print(f"   Base (21000):                    {21000:>10,}")
        if calldata_zero_gas > 0:
            print(f"   Calldata (zero bytes):           {calldata_zero_gas:>10,}")
        if calldata_nonzero_gas > 0:
            print(f"   Calldata (non-zero bytes):       {calldata_nonzero_gas:>10,}")
        if creation_gas > 0:
            print(f"   Contract creation (32000):       {creation_gas:>10,}")
        if calldata_zero_gas > 0 or calldata_nonzero_gas > 0 or creation_gas > 0:
            print(f"   Subtotal base intrinsic:         {intrinsic_gas:>10,}")
        if access_list_gas > 0:
            print(f"   Access list (EIP-2930):          {access_list_gas:>10,}")
        if authorization_list_gas > 0:
            print(f"   Authorization list (EIP-7702):   {authorization_list_gas:>10,}")
        if access_list_gas > 0 or authorization_list_gas > 0:
            print(f"   Total intrinsic:                 {intrinsic_gas_total:>10,}")

    if not silent and access_list_gas > 0:
        print("\n" + "="*70)
        print(f"2. ACCESS LIST GAS (EIP-2930): {access_list_gas}")
        print("="*70)
        for e in access_list_breakdown:
            print(f"   Entry {e['index']}: {e['address']} -> {e['total_gas']} gas")

    contract_creation_gas = 0
    if tx.get('to') is None:
        contract_creation_gas = calculate_contract_creation_storage_gas(receipt)
        if not silent:
            print("\n" + "="*70)
            print(f"3. CONTRACT CREATION STORAGE GAS: {contract_creation_gas}")
            print("="*70)

    if prefetched_traces:
        struct_trace = prefetched_traces.get('struct_trace', {})
    else:
        struct_trace = cast_rpc_dbg_tx(tx_hash, tracer=DFT_TRACER_JSON, rpc_url=RPC_FOR_TRACING)

    if prefetched_state_changes is not None:
        storage_changes = prefetched_state_changes
    elif prefetched_traces and prefetched_traces.get('state_diff'):
        # batch_trace_transactions already fetched the prestate diff — parse it directly
        # instead of issuing a redundant per-tx trace_replayTransaction call.
        sd_resp = prefetched_traces['state_diff']
        sd_result = sd_resp.get('result') if isinstance(sd_resp, dict) else None
        if sd_result:
            storage_changes = _parse_prestate(sd_result, debug=debug)
        else:
            storage_changes = get_storage_and_account_changes(tx_hash, rpc_url=RPC_FOR_TRACING, debug=debug)
    else:
        storage_changes = get_storage_and_account_changes(tx_hash, rpc_url=RPC_FOR_TRACING, debug=debug)

    opcode_gas_total, non_call_gas, call_overhead, create_overhead, per_op_noncall, per_op_call, used_root_fallback, \
        account_access_counts, storage_access_counts, account_cold_counts, storage_cold_counts = \
        compute_opcode_gas_custom(tx, receipt, struct_trace, intrinsic_gas_total, debug=debug,
                                  extra_implicit_gas=eip3860_init_gas + contract_creation_gas)

    # Validate access costs
    access_validation = validate_access_costs(
        tx, per_op_noncall, account_access_counts, storage_access_counts,
        access_list_addrs, access_list_slots, account_cold_counts, storage_cold_counts
    )

    if not silent:
        print("\n" + "="*70)
        print("4. OPCODE EXECUTION")
        print("="*70)
        print("   Mode: DEPTH_DIRECT (all-depth per-op; CALL/CREATE = net(frame) − Σ net(direct children))")
        if used_root_fallback:
            # Check if this was a precompile call
            to_address = tx.get('to')
            precompile_name = is_precompile(to_address) if to_address else None

            if precompile_name:
                print(f"\nSpecial case: Transaction to precompile ({precompile_name})")
                print("   No code execution (empty structlogs)")
                print(f"   Precompile execution gas: {opcode_gas_total:,} gas")
            else:
                print("\nSpecial case: Transaction to EOA with calldata")
                print("   No code execution (empty structlogs)")
                print(f"   Execution gas (root fallback): {opcode_gas_total:,} gas")

    # Extract refunds and structLogs from prefetched traces to avoid extra RPC calls.
    # Traces from _batch_trace_single_chunk are full JSON-RPC envelopes
    # ({'result': {'structLogs': ...}}); others are bare results — handle both.
    if isinstance(struct_trace, dict):
        struct_logs = (struct_trace.get('result') or {}).get('structLogs') or struct_trace.get('structLogs') or []
    else:
        struct_logs = []

    # Extract refund from structLogs instead of making another RPC call
    calculated_refunds = extract_refund_from_trace(struct_logs, receipt)

    halt = _classify_halt(struct_logs)
    receipt_failed = (receipt.get('status',1) == 0)
    effective_refund_pre_cap = 0 if (receipt_failed or halt in ("revert","invalid","other")) else calculated_refunds

    authorization_list_gas, authorization_list_breakdown, _ = calculate_authorization_list_gas(tx)
    if not silent and authorization_list_gas > 0:
        print("\n" + "="*70)
        print(f"X. AUTHORIZATION LIST GAS (EIP-7702): {authorization_list_gas}")
        print("="*70)
        for e in authorization_list_breakdown:
            print(f"   Entry {e['index']}: delegation_target={e['delegation_target']}")
            print(f"            chain_id={e['chain_id']}, nonce={e['nonce']}")
            print(f"            total_gas={e['gas']:,}")

    gross_gas = intrinsic_gas_total + eip3860_init_gas + contract_creation_gas + opcode_gas_total

    # Get block number for fork checks
    block_number = tx.get('blockNumber') or receipt.get('blockNumber', 0)
    if isinstance(block_number, str):
        try:
            block_number = int(block_number, 16) if block_number.startswith('0x') else int(block_number)
        except (ValueError, TypeError):
            block_number = 0
    elif not isinstance(block_number, int):
        block_number = 0

    gas_for_comparison = actual_gas

    implied_refund = max(0, gross_gas - gas_for_comparison)
    refund_cap = gross_gas // 5

    # EIP-7702 authorization refund, derived from the tuples and the authority's
    # pre-transaction state rather than from the receipt. Clients differ on
    # whether it appears in the trace counter, so combine_refunds() subtracts the
    # counter's initial value and adds this once.
    auth_refund = 0
    authorization_detail = []
    if authorization_list_gas > 0:
        # Fail closed. A type-4 row whose authorization refund could not be
        # derived is wrong, not approximate, so raise rather than write a zero.
        state_diff = (storage_changes or {}).get('_raw_state_diff')
        if not state_diff:
            raise AuthorizationRefundUnavailable(
                f"{tx_hash}: type-4 transaction has no Parity stateDiff; the "
                f"authorization refund cannot be derived")

        block = tx.get('blockNumber') or receipt.get('blockNumber', 0)
        parent = max(0, (int(block, 16) if isinstance(block, str) else int(block)) - 1)
        # Balances reconstructed across the block when the caller supplied them.
        # A parent-block read is only correct at txIndex 0: an earlier transaction
        # in the same block can move an authority across zero, flipping existence.
        pre_balances = (storage_changes or {}).get('_pre_balances')

        def _balance(addr):
            key = str(addr).lower()
            if pre_balances is not None and key in pre_balances:
                return pre_balances[key]
            # No except-and-return-zero: a failed read would silently drop the
            # refund. Let it propagate so the transaction is retried.
            return w3.eth.get_balance(Web3.to_checksum_address(addr),
                                      block_identifier=parent)

        auth_refund, authorization_detail = authorization_refund(
            tx, lambda addr: resolve_existence(
                authority_state_from_parity_statediff(state_diff, addr), addr, _balance))

        unresolved = [d for d in authorization_detail
                      if d['reason'] == 'existence_undecidable']
        if unresolved:
            raise AuthorizationRefundUnavailable(
                f"{tx_hash}: authority existence unresolved for "
                f"{len(unresolved)} tuple(s)")

    # The receipt is authoritative for success. `halt` is a trace classification
    # and reports "other" when a contract runs off the end of its bytecode with no
    # terminal STOP/RETURN -- which succeeds and keeps its SSTORE refund.
    trace_refund_initial = extract_initial_refund_from_trace(struct_logs)
    _uncapped, final_refunds, execution_refund = combine_refunds(
        auth_refund, trace_refund_initial, calculated_refunds,
        receipt_failed, gross_gas)
    actual_refund_pre_cap = _uncapped

    gas_after_refund = gross_gas - final_refunds

    # EIP-7623 calldata floor (Pectra): tx.gasUsed = max(gas_after_refund,
    # 21000 + 10*tokens), tokens = zero_bytes + 4*nonzero_bytes.
    eip7623_adjustment = eip7623_floor_adjustment(
        tx.get('input'), gas_after_refund, block_number, CURRENT_CHAIN)

    calculated_total = gas_after_refund + eip7623_adjustment
    difference = gas_for_comparison - calculated_total

    if not silent:
        print(f"   - Non-CALL opcodes sum:                  {non_call_gas:,}")
        print(f"   - CALL family (reported):                {call_overhead:,}")
        print(f"   - CREATE family (reported):              {create_overhead:,}")
        print(f"   → Execution gas total (per-op sum):      {opcode_gas_total:,}")

        print("\n" + "="*70)
        print("5. STORAGE CHANGES")
        print("="*70)
        print(f"Slots created (0→non-zero):        {storage_changes['zero_to_nonzero']:>6,}")
        print(f"Slots deleted (non-zero→0):        {storage_changes['nonzero_to_zero']:>6,}")
        print(f"Slots updated (non-zero→non-zero): {storage_changes['nonzero_to_nonzero']:>6,}")
        print(f"Net created slots (Δsize):         {storage_changes['net_slots_written']:>6,}")
        if storage_changes['accounts_created'] > 0:
            print(f"Accounts created:                  {storage_changes['accounts_created']:>6,}")
        if storage_changes['accounts_deleted'] > 0:
            print(f"Accounts deleted (selfdestruct):   {storage_changes['accounts_deleted']:>6,}")
        if storage_changes.get('bytecode_bytes_allocated', 0) > 0:
            print(f"Bytecode bytes allocated:          {storage_changes.get('bytecode_bytes_allocated', 0):>6,}")
        if storage_changes.get('bytecode_bytes_freed', 0) > 0:
            print(f"Bytecode bytes freed:              {storage_changes.get('bytecode_bytes_freed', 0):>6,}")
        if storage_changes.get('net_bytecode_bytes', 0) != 0:
            print(f"Net bytecode bytes (Δsize):        {storage_changes.get('net_bytecode_bytes', 0):>6,}")

        # Print access validation results
        if account_access_counts or storage_access_counts:
            print("\n" + "="*70)
            print("5a. ACCESS COST VALIDATION")
            print("="*70)

            if access_validation['account_accesses']:
                print("\nAccount Accesses:")
                for op, data in access_validation['account_accesses'].items():
                    print(f"  {op}:")
                    print(f"    Count: {data['count']}")
                    print(f"    Actual gas: {data['actual_gas']:,}")
                    print(f"    Avg gas/access: {data['avg_gas_per_access']:.2f}")
                    if 'actual_cold_accesses' in data:
                        print(f"    Actual cold accesses: {data['actual_cold_accesses']}")
                        print(f"    Actual warm accesses: {data['actual_warm_accesses']}")
                    if 'estimated_cold_accesses' in data:
                        print(f"    Estimated cold accesses: {data['estimated_cold_accesses']:.2f}")
                        print(f"    Estimated warm accesses: {data['estimated_warm_accesses']:.2f}")

            if access_validation['storage_accesses']:
                print("\nStorage Accesses:")
                for op, data in access_validation['storage_accesses'].items():
                    print(f"  {op}:")
                    print(f"    Count: {data['count']}")
                    print(f"    Actual gas: {data['actual_gas']:,}")
                    print(f"    Avg gas/access: {data['avg_gas_per_access']:.2f}")
                    if 'actual_cold_accesses' in data:
                        print(f"    Actual cold accesses: {data['actual_cold_accesses']}")
                        print(f"    Actual warm accesses: {data['actual_warm_accesses']}")
                    if 'estimated_cold_accesses' in data:
                        print(f"    Estimated cold accesses: {data['estimated_cold_accesses']:.2f}")
                        print(f"    Estimated warm accesses: {data['estimated_warm_accesses']:.2f}")
                    if 'note' in data:
                        print(f"    Note: {data['note']}")

            if access_validation['warnings']:
                print("\n  Warnings:")
                for warning in access_validation['warnings']:
                    print(f"    - {warning}")

        print("\n" + "="*70)
        print("6. REFUNDS")
        print("="*70)
        if calculated_refunds or implied_refund:
            if calculated_refunds:
                print(f"SSTORE Refunds (traced):        {calculated_refunds:,} gas")
            if actual_refund_pre_cap > calculated_refunds:
                print(f"Additional Refunds (EIP-7702):  {actual_refund_pre_cap - calculated_refunds:,} gas")
            print(f"Total Refunds (pre-cap):        {actual_refund_pre_cap:,} gas")
            print(f"Refund cap (20% of gross):      {refund_cap:,} gas")
            print(f"Final refunds (capped):         {final_refunds:,} gas")
            if effective_refund_pre_cap == 0 and calculated_refunds > 0:
                print("WARNING: SSTORE refunds suppressed (transaction failed)")
        else:
            print("No refunds")

        print("\n" + "="*70)
        print("FINAL GAS BREAKDOWN")
        print("="*70)
        print(f"1. Intrinsic Gas (total):        {intrinsic_gas_total:>15,}")
        if access_list_gas > 0 or authorization_list_gas > 0:
            print(f"   - Base intrinsic:             {intrinsic_gas:>15,}")
            if access_list_gas > 0:
                print(f"   - Access list (EIP-2930):     {access_list_gas:>15,}")
            if authorization_list_gas > 0:
                print(f"   - Authorization (EIP-7702):   {authorization_list_gas:>15,}")
        print(f"2. EIP-3860 Initcode Gas:        {eip3860_init_gas:>15,}")
        print(f"3. Contract Creation Gas:        {contract_creation_gas:>15,}")
        print(f"4. Execution Gas (per-op sum):   {opcode_gas_total:>15,}")
        print("-"*70)
        print(f"   Gross Gas Used:               {gross_gas:>15,}")
        if final_refunds > 0:
            print(f"5. Gas Refunds (≤20% cap):       {final_refunds:>15,} (subtract)")
        elif calculated_refunds > 0:
            print(f"5. Gas Refunds (failed tx):      {0:>15,} (suppressed)")
        if eip7623_adjustment > 0:
            print(f"6. EIP-7623 calldata floor:      {eip7623_adjustment:>15,} (add)")
        print("="*70)
        print(f"   CALCULATED TOTAL (net):     {calculated_total:>15,}")
        print(f"   ACTUAL GAS USED:            {gas_for_comparison:>15,}")
        print(f"   DIFFERENCE:                 {difference:>15,}")
        print("="*70 + "\n")

    return {
        'tx_hash': tx_hash,
        'gas_limit': tx.get('gas', 0),
        'intrinsic_gas': intrinsic_gas_total,
        'intrinsic_gas_base': intrinsic_gas,
        'calldata_zero_gas': calldata_zero_gas,
        'calldata_nonzero_gas': calldata_nonzero_gas,
        'creation_gas': creation_gas,
        'eip3860_init_gas': eip3860_init_gas,
        'eip7623_adjustment': eip7623_adjustment,
        'access_list_gas': access_list_gas,
        'authorization_list_gas': authorization_list_gas,
        'contract_creation_gas': contract_creation_gas,
        'opcode_gas_total': opcode_gas_total,
        'non_call_gas': non_call_gas,
        'call_overhead': call_overhead,
        'gross_gas': gross_gas,
        'calculated_refunds_pre_cap': calculated_refunds,
        'uncapped_refund': actual_refund_pre_cap,
        'final_refunds': final_refunds,
        'calculated_total': calculated_total,
        'actual_gas': actual_gas,
        'difference': difference,
        'halt': halt,
        'status': receipt.get('status', 1),
        'per_op_noncall': per_op_noncall,
        'per_op_call': per_op_call,
        'used_root_fallback': used_root_fallback,
        'storage_slots_created': storage_changes['zero_to_nonzero'],
        'storage_slots_deleted': storage_changes['nonzero_to_zero'],
        'storage_slots_updated': storage_changes['nonzero_to_nonzero'],
        'net_storage_slots_written': storage_changes['net_slots_written'],
        'accounts_created': storage_changes['accounts_created'],
        'account_births': storage_changes.get('account_births'),
        'account_deaths': storage_changes.get('account_deaths'),
        'accounts_deleted': storage_changes['accounts_deleted'],
        'bytecode_bytes_allocated': storage_changes.get('bytecode_bytes_allocated', 0),
        'bytecode_bytes_freed': storage_changes.get('bytecode_bytes_freed', 0),
        'net_bytecode_bytes': storage_changes.get('net_bytecode_bytes', 0),
        'account_access_counts': account_access_counts,
        'storage_access_counts': storage_access_counts,
        'account_cold_counts': account_cold_counts,
        'storage_cold_counts': storage_cold_counts,
        'access_validation': access_validation,
    }

# ──────────────────────────────────────────────────────────────────────────────
# Block runner with parallelization and batching
# ──────────────────────────────────────────────────────────────────────────────
def _analyze_block_worker(args):
    """
    Pickle-able wrapper for multiprocessing.
    Takes a tuple of (block_num, use_batching, rpc_url, rpc_tracing, chain_config).
    """
    block_num, use_batching, rpc_url, rpc_tracing, current_chain = args

    # Initialize per-process state (module globals read by analyze_transaction
    # and the batch helpers) and a Web3 instance in this process.
    global w3, RPC_URL, RPC_FOR_TRACING, CURRENT_CHAIN
    RPC_URL = rpc_url
    RPC_FOR_TRACING = rpc_tracing
    if current_chain:
        CURRENT_CHAIN = current_chain

    from web3 import Web3
    w3_local = Web3(Web3.HTTPProvider(rpc_url))
    w3 = w3_local

    try:
        block = w3_local.eth.get_block(block_num, full_transactions=True)
        tx_count = len(block.transactions)
        if tx_count == 0:
            return (block_num, [], True)

        tx_hashes = [tx['hash'].hex() for tx in block.transactions]

        batch_traces = {}
        batch_tx_receipts = {}
        batch_state_changes = {}

        if use_batching:
            try:
                batch_tx_receipts = batch_get_transactions_and_receipts(tx_hashes, rpc_url=rpc_url)
            except Exception as e:
                print(f"  Block {block_num}: Batch tx/receipt failed: {e}")

            # Fetch structlogs, Parity state diffs, and exact account births concurrently.
            try:
                from concurrent.futures import ThreadPoolExecutor as _TPE
                with _TPE(max_workers=3) as _ex:
                    f_struct = _ex.submit(trace_block_by_number, block_num, rpc_tracing)
                    f_state = _ex.submit(trace_replay_block_state_diffs, block_num, rpc_tracing)
                    f_accounts = _ex.submit(trace_block_account_changes, block_num, rpc_tracing)
                    batch_traces = f_struct.result()
                    raw_state = f_state.result()
                    exact_accounts = f_accounts.result()
                pre_balances = block_authority_pre_balances(
                    block.transactions, raw_state, w3_local)
                for tx_hash in tx_hashes:
                    sd = raw_state.get(tx_hash)
                    counts = exact_accounts.get(tx_hash)
                    if counts is None:
                        raise RuntimeError(f"missing exact account trace for {tx_hash}")
                    state_changes = _state_changes_from_raw(
                        sd, pre_balances=pre_balances.get(tx_hash))
                    state_changes.update(counts)
                    batch_state_changes[tx_hash] = state_changes
            except Exception as e:
                print(f"  Block {block_num}: block-level RPC failed ({e}), falling back to per-tx batching")
                try:
                    batch_traces = batch_trace_transactions(tx_hashes, rpc_url=rpc_tracing)
                except Exception as e2:
                    print(f"  Block {block_num}: Per-tx batch also failed: {e2}, falling back to sequential")
                    use_batching = False

        def process_single_tx(tx_hash):
            try:
                prefetched = batch_traces.get(tx_hash) if use_batching else None
                tx_data = batch_tx_receipts.get(tx_hash, {})
                result = analyze_transaction(
                    tx_hash,
                    silent=True,
                    prefetched_traces=prefetched,
                    prefetched_tx=tx_data.get('tx'),
                    prefetched_receipt=tx_data.get('receipt'),
                    prefetched_state_changes=batch_state_changes.get(tx_hash),
                )
                return result
            except Exception as e:
                try:
                    tx_obj = w3_local.eth.get_transaction(tx_hash)
                    gas_limit = tx_obj.get('gas', 0)
                except:
                    gas_limit = 0
                return {'tx_hash': tx_hash, 'gas_limit': gas_limit, 'actual_gas': 0, 'error': str(e)}

        block_results = []
        # Process transactions sequentially within each block
        for tx_hash in tx_hashes:
            try:
                result = process_single_tx(tx_hash)
                block_results.append(result)
            except Exception as e:
                block_results.append({'tx_hash': tx_hash, 'gas_limit': 0, 'actual_gas': 0, 'error': str(e)})

        return (block_num, block_results, True)

    except Exception as e:
        print(f"  Block {block_num}: Error - {e}")
        return (block_num, [], False)

def analyze_block_range(start_block: int, num_blocks: int = 10, use_batching: bool = True,
                       parallel_blocks: bool = True, num_workers: int = None):
    total_start = time.time()
    block_nums = list(range(start_block, start_block + num_blocks))
    skipped_blocks = []

    if parallel_blocks:
        # Use custom num_workers if provided, otherwise use global NUM_WORKERS
        max_workers = num_workers if num_workers is not None else NUM_WORKERS
        block_workers = min(max_workers, num_blocks)
        print(f"Processing {num_blocks} blocks with ProcessPoolExecutor ({block_workers} concurrent processes)")
        print(f"Batching: {'enabled' if use_batching else 'disabled'}\n")

        completed_blocks = {}
        processed_txs = 0
        processed_gas = 0
        start_time = time.time()

        # Prepare arguments for multiprocessing
        worker_args = [(block_num, use_batching, RPC_URL, RPC_FOR_TRACING, CURRENT_CHAIN)
                       for block_num in block_nums]

        with ProcessPoolExecutor(max_workers=block_workers) as executor:
            future_to_block = {executor.submit(_analyze_block_worker, args): args[0]
                               for args in worker_args}
            for future in as_completed(future_to_block):
                block_num = future_to_block[future]
                try:
                    block_num_result, block_results, success = future.result()
                    if success and block_results:
                        write_block_opcode_breakdown_to_file(block_num_result, block_results, chain=CURRENT_CHAIN)
                        mismatches = sum(1 for r in block_results if r.get('difference', 0) != 0)
                        block_gas = sum(r.get('actual_gas', 0) for r in block_results)
                        processed_txs += len(block_results)
                        processed_gas += block_gas
                        elapsed = time.time() - start_time
                        tx_per_sec = processed_txs / elapsed if elapsed > 0 else 0
                        gas_per_sec = processed_gas / elapsed if elapsed > 0 else 0
                        print(f"Block {block_num_result}: {len(block_results)} txs, {mismatches} mismatches ({tx_per_sec:.1f} tx/s, {gas_per_sec:,.0f} gas/s)")
                        completed_blocks[block_num_result] = (len(block_results), mismatches, block_gas)
                    elif success:
                        # No transactions: complete with nothing to write, not skipped.
                        print(f"Block {block_num_result}: 0 txs, empty block")
                        completed_blocks[block_num_result] = (0, 0, 0)
                    else:
                        print(f"Block {block_num_result}: failed, not written")
                        skipped_blocks.append(block_num_result)
                except BlockNotWritable as e:
                    print(f"Block {block_num}: NOT WRITTEN - {e}")
                    skipped_blocks.append(block_num)
                except Exception as e:
                    print(f"Block {block_num}: Exception - {e}")
                    skipped_blocks.append(block_num)

        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        total_txs = sum(txs for txs, _, _ in completed_blocks.values())
        total_mismatches = sum(mis for _, mis, _ in completed_blocks.values())
        total_gas = sum(gas for _, _, gas in completed_blocks.values())
        total_time = time.time() - total_start

        print(f"Blocks processed: {len(completed_blocks)}/{num_blocks}")
        if skipped_blocks:
            print(f"Blocks NOT written (retry required): {len(skipped_blocks)}")
            print(f"  {sorted(skipped_blocks)}")
        print(f"Total transactions: {total_txs:,}")
        print(f"Total gas: {total_gas:,}")
        print(f"Total mismatches: {total_mismatches:,}")
        print(f"Accuracy: {(total_txs - total_mismatches) / total_txs * 100:.4f}%" if total_txs > 0 else "N/A")
        if total_time > 0:
            print(f"Total time: {total_time:.2f}s")
            print(f"Throughput: {total_txs / total_time:.1f} tx/s, {total_gas / total_time:,.0f} gas/s")
        print(f"{'='*70}\n")

    else:
        for block_num in block_nums:
            print(f"\n{'='*70}\nProcessing Block {block_num}\n{'='*70}")
            block_results = []
            try:
                block = w3.eth.get_block(block_num, full_transactions=True)
                tx_count = len(block.transactions)
                print(f"Found {tx_count} transactions")
                if tx_count == 0:
                    continue

                tx_hashes = [tx['hash'].hex() for tx in block.transactions]

                if use_batching:
                    print(f"  Batching {tx_count * 3} trace requests...")
                    start_time = time.time()
                    try:
                        batch_traces = batch_trace_transactions(tx_hashes, rpc_url=RPC_FOR_TRACING)
                        fetch_time = time.time() - start_time
                        print(f"  Batch fetch completed in {fetch_time:.2f}s")
                    except Exception as e:
                        print(f"  WARNING: Batch fetch failed: {e}, falling back to sequential")
                        use_batching = False
                        batch_traces = {}

                def process_single_tx(tx_hash):
                    try:
                        prefetched = batch_traces.get(tx_hash) if use_batching else None
                        result = analyze_transaction(tx_hash, silent=True, prefetched_traces=prefetched)
                        return result
                    except Exception as e:
                        try:
                            tx_obj = w3.eth.get_transaction(tx_hash)
                            gas_limit = tx_obj.get('gas', 0)
                        except:
                            gas_limit = 0
                        return {'tx_hash': tx_hash, 'gas_limit': gas_limit, 'actual_gas': 0, 'error': str(e)}

                process_start = time.time()
                # Process transactions sequentially within each block
                for tx_hash in tx_hashes:
                    try:
                        result = process_single_tx(tx_hash)
                        block_results.append(result)
                        if result.get('difference', 0) != 0:
                            print(f"  {tx_hash}: diff={result['difference']:,}")
                    except Exception as e:
                        print(f"  {tx_hash}: {e}")
                        block_results.append({'tx_hash': tx_hash, 'gas_limit': 0, 'actual_gas': 0, 'error': str(e)})

                process_time = time.time() - process_start
                print(f"  Processed {tx_count} transactions in {process_time:.2f}s ({tx_count/process_time:.1f} tx/s)")
                if block_results:
                    write_block_opcode_breakdown_to_file(block_num, block_results, chain=CURRENT_CHAIN)

            except BlockNotWritable as e:
                print(f"Block {block_num}: NOT WRITTEN - {e}")
                skipped_blocks.append(block_num)
            except Exception as e:
                print(f"Error processing block {block_num}: {e}")
                skipped_blocks.append(block_num)
                import traceback
                traceback.print_exc()

    print("\n" + "="*70 + "\nBlock processing complete\n" + "="*70)
    if skipped_blocks:
        raise RuntimeError(
            f"{len(skipped_blocks)} blocks failed; rerun to collect them")

# ──────────────────────────────────────────────────────────────────────────────
# Multi-Chain Date Range Analysis
# ──────────────────────────────────────────────────────────────────────────────
def analyze_date_range_multi_chain(start_date: str, end_date: str,
                                    chains: List[str] = None,
                                    blocks_per_day: int = 100,
                                    use_batching: bool = False,
                                    parallel_blocks: bool = True,
                                    num_workers: int = None):
    """
    Analyze blocks across multiple chains for a date range.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        chains: List of chain names (default: all chains)
        blocks_per_day: Number of blocks to sample per day
        use_batching: Whether to batch RPC calls
        parallel_blocks: Whether to process blocks in parallel (default: True)
        num_workers: Number of parallel workers (default: use NUM_WORKERS global)
    """
    if chains is None:
        chains = list(CHAIN_CONFIGS.keys())

    print(f"{'='*80}")
    print(f"MULTI-CHAIN ANALYSIS: {start_date} to {end_date}")
    print(f"{'='*80}")
    print(f"Chains: {', '.join(chains)}")
    print(f"Blocks per day: {blocks_per_day}")
    print(f"{'='*80}\n")

    all_results = {}

    for chain in chains:
        print(f"\n{'='*80}")
        print(f"Processing Chain: {CHAIN_CONFIGS[chain]['name']}")
        print(f"{'='*80}\n")

        # Sample blocks for this chain
        sampled_blocks = sample_blocks_for_date_range(
            start_date, end_date,
            blocks_per_day=blocks_per_day,
            chain=chain
        )

        # Analyze each day's blocks
        chain_results = {}
        chain_mismatches = []

        total_days = len(sampled_blocks)
        day_counter = 0
        chain_start_time = time.time()

        for date_str, block_list in sampled_blocks.items():
            day_counter += 1
            day_start_time = time.time()

            print(f"\n--- {chain} {date_str}: {len(block_list)} blocks (Day {day_counter}/{total_days}) ---")

            day_results = []
            day_mismatches = []

            # Skip if no blocks to process
            if len(block_list) == 0:
                print(f"    No blocks to process (already have enough blocks)")
                continue

            # Process blocks for this day in parallel; with --no-parallel a single
            # worker processes blocks sequentially through the same path.
            blocks_processed = 0
            day_skipped = []
            max_workers = num_workers if num_workers is not None else NUM_WORKERS
            block_workers = min(max_workers, len(block_list)) if parallel_blocks else 1

            with ThreadPoolExecutor(max_workers=block_workers) as executor:
                future_to_block = {
                    executor.submit(analyze_single_block_chain, block_num, chain, use_batching): block_num
                    for block_num in block_list
                }

                print(f"    Processing {len(block_list)} blocks with {block_workers} workers...")

                # Track statistics without storing full results
                day_tx_count = 0
                day_mismatch_count = 0

                for future in as_completed(future_to_block):
                    block_num = future_to_block[future]
                    try:
                        block_num_result, block_results, success = future.result()

                        if success:
                            if block_results:
                                # Write block immediately to free memory
                                write_block_opcode_breakdown_to_file(block_num_result, block_results, chain=chain)

                                # Track mismatches for this block
                                mismatches_in_block = []
                                for result in block_results:
                                    if result.get('difference', 0) != 0:
                                        result['block_number'] = block_num
                                        mismatches_in_block.append(result)

                                # Update statistics
                                day_tx_count += len(block_results)
                                day_mismatch_count += len(mismatches_in_block)

                                # Write mismatches immediately
                                if mismatches_in_block:
                                    day_mismatches.extend(mismatches_in_block)

                                # Print block completion
                                print(f"    Block {block_num_result}: {len(block_results)} txs, {len(mismatches_in_block)} mismatches")
                            else:
                                # A block with no transactions is complete, not skipped:
                                # the worker returned ([], True) and there is nothing to write.
                                print(f"    Block {block_num_result}: 0 txs, empty block")
                        else:
                            # The worker failed, so nothing was written and the block
                            # must be retried. Distinct from an empty block above.
                            print(f"    Block {block_num_result}: failed, not written")
                            day_skipped.append(block_num_result)

                        blocks_processed += 1

                        # Print progress summary every 10 blocks
                        if blocks_processed % 10 == 0:
                            day_elapsed = time.time() - day_start_time
                            blocks_remaining = len(block_list) - blocks_processed
                            if blocks_processed > 0:
                                avg_time_per_block = day_elapsed / blocks_processed
                                eta_seconds = blocks_remaining * avg_time_per_block
                                print(f"    Progress: {blocks_processed}/{len(block_list)} blocks "
                                      f"({blocks_processed/len(block_list)*100:.1f}%), "
                                      f"ETA: {eta_seconds:.0f}s")

                    except BlockNotWritable as e:
                        print(f"  Block {block_num}: NOT WRITTEN - {e}")
                        day_skipped.append(block_num)
                        blocks_processed += 1
                    except Exception as e:
                        print(f"  Block {block_num}: {e}")
                        day_skipped.append(block_num)
                        blocks_processed += 1

            # Print mismatches for the day
            if day_mismatches:
                print_mismatches(day_mismatches, chain=chain, date=date_str)
                chain_mismatches.extend(day_mismatches)

            # Print day summary
            accuracy = ((day_tx_count - day_mismatch_count) / day_tx_count * 100) if day_tx_count > 0 else 100
            print(f"  {date_str}: {day_tx_count} txs, {day_mismatch_count} mismatches ({accuracy:.2f}% accuracy)")
            if day_skipped:
                print(f"  {date_str}: {len(day_skipped)} block(s) NOT written, retry required: "
                      f"{sorted(day_skipped)}")

            # Track stats only, not full results
            chain_results[date_str] = {'txs': day_tx_count, 'mismatches': day_mismatch_count,
                                       'skipped': list(day_skipped)}

            day_mismatches.clear()
            gc.collect()

        # Chain summary
        total_txs = sum(v['txs'] for v in chain_results.values())
        total_mismatches = sum(v['mismatches'] for v in chain_results.values())
        total_accuracy = ((total_txs - total_mismatches) / total_txs * 100) if total_txs > 0 else 100

        print(f"\n{'='*80}")
        print(f"{chain.upper()} SUMMARY")
        print(f"{'='*80}")
        total_skipped = sum(len(v.get('skipped', ())) for v in chain_results.values())
        print(f"Total transactions: {total_txs:,}")
        print(f"Total mismatches: {total_mismatches:,}")
        print(f"Accuracy: {total_accuracy:.4f}%")
        if total_skipped:
            print(f"Blocks NOT written (incomplete data, retry required): {total_skipped}")
            for d, v in sorted(chain_results.items()):
                if v.get('skipped'):
                    print(f"  {d}: {sorted(v['skipped'])}")
        print(f"{'='*80}\n")

        all_results[chain] = {
            'total_txs': total_txs,
            'total_mismatches': total_mismatches,
            'accuracy': total_accuracy,
            'blocks_not_written': total_skipped
        }

        chain_results.clear()
        chain_mismatches.clear()

    # Overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")
    for chain, data in all_results.items():
        print(f"{chain}: {data['total_txs']:,} txs, {data['total_mismatches']:,} mismatches ({data['accuracy']:.4f}%)")
    print(f"{'='*80}\n")

    total_not_written = sum(data["blocks_not_written"] for data in all_results.values())
    if total_not_written:
        raise RuntimeError(
            f"{total_not_written} blocks failed; rerun to collect them")
    return all_results

def analyze_single_block_chain(block_num: int, chain: str, use_batching: bool = False):
    """
    Analyze a single block for a specific chain.

    Args:
        block_num: Block number to analyze
        chain: Chain name
        use_batching: Whether to use batch RPC calls

    Returns:
        Tuple of (block_num, results_list, success_bool)
    """
    # Get chain-specific web3 instance
    config = CHAIN_CONFIGS[chain]
    w3_chain = Web3(Web3.HTTPProvider(config['rpc_url']))
    rpc_tracing = config['rpc_tracing']

    try:
        block = w3_chain.eth.get_block(block_num, full_transactions=True)
        tx_count = len(block.transactions)

        if tx_count == 0:
            return (block_num, [], True)

        tx_hashes = [tx['hash'].hex() for tx in block.transactions]

        # Prefer three parallel block-level traces; fall back to transaction batches.
        if use_batching:
            prefetched_data = batch_get_transactions_and_receipts(tx_hashes, rpc_url=config['rpc_url'], chunk_size=BATCH_CHUNK_SIZE)
            tx_map = {tx['hash'].hex(): tx for tx in block.transactions}
            prefetched_traces = {}
            prefetched_state_changes = {}
            try:
                from concurrent.futures import ThreadPoolExecutor as _TPE
                with _TPE(max_workers=3) as _ex:
                    f_struct = _ex.submit(trace_block_by_number, block_num, rpc_tracing)
                    f_state = _ex.submit(trace_replay_block_state_diffs, block_num, rpc_tracing)
                    f_accounts = _ex.submit(trace_block_account_changes, block_num, rpc_tracing)
                    prefetched_traces = f_struct.result()
                    raw_state = f_state.result()
                    exact_accounts = f_accounts.result()
                pre_balances = block_authority_pre_balances(
                    block.transactions, raw_state, w3_chain)
                for tx_hash in tx_hashes:
                    sd = raw_state.get(tx_hash)
                    counts = exact_accounts.get(tx_hash)
                    if counts is None:
                        raise RuntimeError(f"missing exact account trace for {tx_hash}")
                    state_changes = _state_changes_from_raw(
                        sd, pre_balances=pre_balances.get(tx_hash))
                    state_changes.update(counts)
                    prefetched_state_changes[tx_hash] = state_changes
            except Exception as e:
                # Fallback: use legacy 2N-batched call with state_diff inline-parsed in analyze_transaction.
                print(f"  Block {block_num}: block-level RPC failed ({e}), falling back to batch_trace_transactions")
                try:
                    prefetched_traces = batch_trace_transactions(tx_hashes, rpc_url=rpc_tracing, chunk_size=BATCH_CHUNK_SIZE)
                except Exception as e2:
                    print(f"  Block {block_num}: per-tx batch also failed ({e2})")
                    prefetched_traces = {}
                prefetched_state_changes = {}
        else:
            prefetched_traces = {}
            prefetched_data = {}
            prefetched_state_changes = {}
            tx_map = {}

        def process_single_tx(tx_hash):
            try:
                traces = prefetched_traces.get(tx_hash, {}) if use_batching else None
                tx_obj = tx_map.get(tx_hash) if use_batching else None
                receipt_obj = prefetched_data.get(tx_hash, {}).get('receipt') if use_batching else None

                result = analyze_transaction_chain(
                    tx_hash, w3_chain, rpc_tracing,
                    silent=True, prefetched_traces=traces,
                    prefetched_tx=tx_obj, prefetched_receipt=receipt_obj,
                    chain_name=chain,
                    prefetched_state_changes=prefetched_state_changes.get(tx_hash) if use_batching else None,
                )
                if result is None:
                    return None
                result['block_number'] = block_num
                return result
            except Exception as e:
                return {
                    'tx_hash': tx_hash,
                    'block_number': block_num,
                    'error': str(e)
                }

        block_results = []
        # Process transactions in parallel within each block
        tx_workers = 16  # Concurrent transactions per block
        with ThreadPoolExecutor(max_workers=tx_workers) as tx_executor:
            future_to_tx = {tx_executor.submit(process_single_tx, tx_hash): tx_hash
                           for tx_hash in tx_hashes}
            for future in as_completed(future_to_tx):
                tx_hash = future_to_tx[future]
                try:
                    result = future.result()
                    if result is not None:
                        block_results.append(result)
                except Exception as e:
                    print(f"    TX {tx_hash[:10]}: {str(e)[:80]}")
                    block_results.append({
                        'tx_hash': tx_hash,
                        'block_number': block_num,
                        'error': str(e)
                    })

        return (block_num, block_results, True)

    except Exception as e:
        print(f"  Block {block_num}: Error - {e}")
        return (block_num, [], False)

def analyze_transaction_chain(tx_hash: str, w3_instance: Web3, rpc_tracing: str,
                               silent: bool = True, debug: bool = False,
                               prefetched_traces: Dict = None, prefetched_tx=None,
                               prefetched_receipt=None, chain_name: str = None,
                               prefetched_state_changes: Dict = None):
    """
    Chain-specific version of analyze_transaction that accepts a web3 instance.
    Sets the module globals for the target chain before delegating.
    """
    global w3, RPC_FOR_TRACING, CURRENT_CHAIN

    # Globals are set per-chain (idempotently) and must NOT be restored per-tx:
    # sibling worker threads share these globals while processing the same chain,
    # so restoring defaults here would race with transactions still in flight.
    if w3 is not w3_instance:
        w3 = w3_instance
    if RPC_FOR_TRACING != rpc_tracing:
        RPC_FOR_TRACING = rpc_tracing
    if chain_name and CURRENT_CHAIN != chain_name:
        CURRENT_CHAIN = chain_name

    return analyze_transaction(tx_hash, silent=silent, debug=debug,
                               prefetched_traces=prefetched_traces,
                               prefetched_tx=prefetched_tx,
                               prefetched_receipt=prefetched_receipt,
                               prefetched_state_changes=prefetched_state_changes)

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────
def deep_dive_transaction(tx_hash: str):
    print(f"\n{'='*80}\nDEEP DIVE ANALYSIS: {tx_hash}\n{'='*80}")
    tx = w3.eth.get_transaction(tx_hash)
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    struct_trace = cast_rpc_dbg_tx(tx_hash, tracer=DFT_TRACER_JSON, rpc_url=RPC_FOR_TRACING)
    structlogs = (struct_trace.get('result') or {}).get('structLogs') or []
    if structlogs:
        first_gas = int(structlogs[0].get("gas",0) or 0)
        last_gas  = int(structlogs[-1].get("gas",0) or 0)
        delta = max(0, first_gas - last_gas)
        print(f"Execution (structlog delta): {delta:,}")
        print(f"sum(gasCost): {_sum_gascost(structlogs):,}")
        print("Last 5 ops:")
        for i,log in enumerate(structlogs[-5:], start=len(structlogs)-4):
            print(f"  [{i}] {log.get('op')} gas={log.get('gas')} gasCost={log.get('gasCost')}")
    else:
        print("No structlogs")
    print(f"{'='*80}\n")

# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("""
Usage:
  # Single transaction analysis
  python script.py single <TX_HASH>

  # Block range analysis (old method)
  python script.py blocks

  # Multi-chain date range analysis
  python script.py daterange <START_DATE> <END_DATE> [OPTIONS]

  Options for daterange:
    --chains <chain1,chain2,...>  Chains to analyze (default: ethereum,base)
                                  Available chains are loaded from rpc_config.json
    --blocks-per-day <N>          Blocks to sample per day (default: 100)
    --workers <N>                 Number of parallel workers (default: 4)
    --batch-size <N>              Max transactions per RPC batch (default: None = no limit)
                                  Use 20-25 for nodes with 100MB response limit
    --no-batching                 Disable RPC batching
    --no-parallel                 Disable parallel processing

  Applies to all modes:
    --require-reth                Abort if the tracing endpoint is not reth (default: warn only).
                                  This collector's gas accounting assumes reth trace semantics.

  Examples:
    # Analyze all chains for August 2024, 100 blocks/day
    python script.py daterange 2024-08-01 2024-08-31

    # Analyze Base with 50 blocks/day, 32 workers
    python script.py daterange 2024-08-01 2024-08-31 --chains base --blocks-per-day 50 --workers 32
""")
        sys.exit(1)

    mode = sys.argv[1]

    check_tracing_client(RPC_FOR_TRACING, strict='--require-reth' in sys.argv)

    try:
        if mode == "single":
            if len(sys.argv) < 3:
                print("Error: Transaction hash required")
                print("Usage: python script.py single <TX_HASH> [--chain CHAIN_NAME]")
                sys.exit(1)

            txh = sys.argv[2]

            # Check for optional chain parameter
            chain = 'ethereum'  # default
            if len(sys.argv) > 3 and sys.argv[3] == '--chain' and len(sys.argv) > 4:
                chain = sys.argv[4]

            # Use chain-specific RPC
            if chain not in CHAIN_CONFIGS:
                print(f"Error: Unknown chain '{chain}'")
                print(f"Available chains: {', '.join(CHAIN_CONFIGS.keys())}")
                sys.exit(1)

            chain_w3 = Web3(Web3.HTTPProvider(CHAIN_CONFIGS[chain]['rpc_url']))
            chain_rpc_tracing = CHAIN_CONFIGS[chain]['rpc_tracing']

            if chain_w3.is_connected():
                analyze_transaction_chain(txh, chain_w3, chain_rpc_tracing, silent=False, debug=True, chain_name=chain)
            else:
                print(f"Error: Not connected to {chain} node")

        elif mode == "blocks":
            if not w3.is_connected():
                print("Error: Not connected to node")
                sys.exit(1)

            latest = w3.eth.block_number
            analyze_block_range(latest - 20, 6, use_batching=True)

        elif mode == "daterange":
            if len(sys.argv) < 4:
                print("Error: Start and end dates required")
                print("Usage: python script.py daterange <START_DATE> <END_DATE> [OPTIONS]")
                sys.exit(1)

            start_date = sys.argv[2]
            end_date = sys.argv[3]

            # Parse options
            chains = None
            blocks_per_day = 100
            use_batching = True
            parallel_blocks = True
            num_workers = None

            i = 4
            while i < len(sys.argv):
                if sys.argv[i] == '--chains' and i + 1 < len(sys.argv):
                    chains = sys.argv[i + 1].split(',')
                    i += 2
                elif sys.argv[i] == '--blocks-per-day' and i + 1 < len(sys.argv):
                    blocks_per_day = int(sys.argv[i + 1])
                    i += 2
                elif sys.argv[i] == '--workers' and i + 1 < len(sys.argv):
                    num_workers = int(sys.argv[i + 1])
                    i += 2
                elif sys.argv[i] == '--batch-size' and i + 1 < len(sys.argv):
                    BATCH_CHUNK_SIZE = int(sys.argv[i + 1])
                    i += 2
                elif sys.argv[i] == '--no-batching':
                    use_batching = False
                    i += 1
                elif sys.argv[i] == '--no-parallel':
                    parallel_blocks = False
                    i += 1
                else:
                    print(f"Warning: Unknown option '{sys.argv[i]}'")
                    i += 1

            # Run multi-chain analysis
            analyze_date_range_multi_chain(
                start_date, end_date,
                chains=chains,
                blocks_per_day=blocks_per_day,
                use_batching=use_batching,
                parallel_blocks=parallel_blocks,
                num_workers=num_workers
            )

        elif mode == "deepdive":
            if len(sys.argv) < 3:
                print("Error: Transaction hash required")
                sys.exit(1)

            txh = sys.argv[2]
            if w3.is_connected():
                deep_dive_transaction(txh)
            else:
                print("Error: Not connected to node")

        else:
            print(f"Error: Unknown mode '{mode}'")
            print("Run without arguments to see usage")
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
