#!/usr/bin/env python3
"""
collect_prestate_poststate.py

Collect prestate and poststate data for transactions at different lookbacks.
Similar to collect_tx_analysis_integrated.py but uses debug_traceCall with prestateTracer
instead of eth_estimateGas.

For each transaction, traces it at multiple lookback points and stores:
- tx_hash
- block_number
- lookback (0, 5, 10, 20)
- state_type ('pre' or 'post')
- address (contract address)
- slot (storage slot)
- value (slot value as hex string)

Output: DuckDB database with prestate/poststate data
"""

import argparse
import csv
import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb

# Resolve sibling package path at import time so the script works regardless of CWD.
_OPCODE_SENSITIVITY_DIR = Path(__file__).resolve().parent.parent / 'opcode_sensitivity'
if str(_OPCODE_SENSITIVITY_DIR) not in sys.path:
    sys.path.insert(0, str(_OPCODE_SENSITIVITY_DIR))

_UTILS_DIR = Path(__file__).resolve().parent.parent / 'utils'
if str(_UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(_UTILS_DIR))

from state_opcode_breakdown import (
    get_session,
    get_transaction,
)
from tx_call_object import build_call_object


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


def is_terminal_trace_error(error: Any) -> bool:
    message = str(error or "").lower()
    return any(part in message for part in TERMINAL_RPC_SUBSTRINGS)

# ──────────────────────────────────────────────────────────────────────────────
# RPC Functions
# ──────────────────────────────────────────────────────────────────────────────

def trace_transaction_prestate(
    tx_hash: str,
    state_block: int,
    rpc_url: str,
    cached_tx: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Trace a transaction at a specific block state to get prestate/poststate.

    Uses debug_traceCall with prestateTracer in diffMode to capture before/after state.

    Args:
        tx_hash: Transaction hash
        state_block: Block number for state
        rpc_url: RPC endpoint
        cached_tx: Pre-fetched transaction data

    Returns:
        Tagged result with status `ok`, `terminal`, or `retryable`.
    """
    session = get_session()

    if cached_tx:
        tx = cached_tx
    else:
        tx = get_transaction(tx_hash, rpc_url)

    if not tx:
        return {"status": "retryable", "error": "transaction not found"}

    call_obj = build_call_object(tx)

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
            error = result["error"]
            status = "terminal" if is_terminal_trace_error(error) else "retryable"
            print(
                f"  {status.title()} trace rejection for tx {tx_hash} at state block "
                f"{state_block}: {error}", flush=True)
            return {"status": status, "error": error}

        if "result" not in result:
            print(f"  Trace returned no result for tx {tx_hash} at state block {state_block}", flush=True)
            return {"status": "retryable", "error": "trace returned no result"}

        return {"status": "ok", "result": result["result"]}

    except Exception as e:
        print(f"  Trace request failed for tx {tx_hash} at state block {state_block}: {e}", flush=True)
        return {"status": "retryable", "error": str(e)}


def flatten_prestate_poststate(
    tx_hash: str,
    block_number: int,
    lookback: int,
    prestate_data: Dict[str, Any]
) -> List[Dict[str, str | int]]:
    """
    Flatten prestate/poststate data into rows for DuckDB.

    Input format: {'pre': {addr: {storage: {slot: value}}}, 'post': {...}}
    Output: List of dicts with columns: tx_hash, block_number, lookback, state_type, address, slot, value

    Args:
        tx_hash: Transaction hash
        block_number: Block number
        lookback: Lookback value
        prestate_data: Dict from prestateTracer with 'pre' and 'post' keys

    Returns:
        List of row dicts for database
    """
    rows = []

    if not isinstance(prestate_data, dict):
        return rows

    for state_type in ['pre', 'post']:
        if state_type not in prestate_data:
            continue

        state = prestate_data[state_type]
        if not isinstance(state, dict):
            continue

        for address, addr_data in state.items():
            if not isinstance(addr_data, dict):
                continue

            storage = addr_data.get('storage', {})
            if not isinstance(storage, dict):
                continue

            for slot, value in storage.items():
                rows.append({
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'lookback': lookback,
                    'state_type': state_type,
                    'address': address.lower(),
                    'slot': slot,
                    'value': value
                })

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Transaction Analysis
# ──────────────────────────────────────────────────────────────────────────────

def analyze_transaction_prestate(
    tx_hash: str,
    block_number: int,
    lookback_points: List[int],
    rpc_url: str
) -> Dict[str, Any]:
    """
    Analyze prestate/poststate for a transaction at multiple lookbacks.

    Args:
        tx_hash: Transaction hash
        block_number: Original block number
        lookback_points: List of lookback values (e.g., [0, 5, 10, 20])
        rpc_url: RPC endpoint

    Returns:
        Dict with 'tx_hash', 'block_number', 'rows' (list of flattened rows),
        'trace_failures' (count of lookbacks that failed to trace), and 'error' if any
    """
    result = {
        'tx_hash': tx_hash,
        'block_number': block_number,
        'rows': [],
        'trace_failures': 0,
        'terminal_rejections': 0,
        'error': None
    }

    tx = get_transaction(tx_hash, rpc_url)
    if not tx:
        result['error'] = 'Failed to fetch transaction'
        return result

    for lookback in lookback_points:
        state_block = block_number - 1 - lookback

        if state_block < 0:
            continue

        trace = trace_transaction_prestate(tx_hash, state_block, rpc_url, cached_tx=tx)
        if trace["status"] == "terminal":
            result['terminal_rejections'] += 1
            continue
        if trace["status"] != "ok":
            result['trace_failures'] += 1
            continue

        lookback_rows = flatten_prestate_poststate(
            tx_hash, block_number, lookback, trace["result"])
        result['rows'].extend(lookback_rows)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Block Processing
# ──────────────────────────────────────────────────────────────────────────────

def process_block_prestate(
    block_num: int,
    lookback_points: List[int],
    rpc_url: str,
    tx_parallelism: int = 1
) -> Dict[str, Any]:
    """
    Process all transactions in a block to collect prestate/poststate at different lookbacks.

    Args:
        block_num: Block number to process
        lookback_points: List of lookback values
        rpc_url: RPC endpoint
        tx_parallelism: Number of parallel workers for transactions

    Returns:
        Dict with block_num, tx_count, row_count, rows list, trace_failures, and tx_errors
    """
    session = get_session()

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_num), True],
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=30)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise RuntimeError(f"eth_getBlockByNumber error: {body['error']}")
        if "result" not in body or body["result"] is None:
            raise RuntimeError(f"eth_getBlockByNumber returned no block for {block_num}")

        block = body["result"]
        transactions = block.get("transactions")
        if not isinstance(transactions, list):
            raise RuntimeError(f"block {block_num} has no transaction list")
        if not transactions:
            return {'block_num': block_num, 'tx_count': 0, 'row_count': 0, 'rows': [],
                    'trace_failures': 0, 'terminal_rejections': 0, 'tx_errors': 0}

        all_rows = []
        trace_failures = 0
        tx_errors = 0
        terminal_rejections = 0

        if tx_parallelism > 1 and len(transactions) > 1:
            with ProcessPoolExecutor(max_workers=tx_parallelism) as executor:
                futures = {
                    executor.submit(analyze_transaction_prestate, tx["hash"], block_num, lookback_points, rpc_url): tx["hash"]
                    for tx in transactions
                }

                for future in as_completed(futures):
                    tx_hash = futures[future]
                    try:
                        result = future.result()
                        trace_failures += result['trace_failures']
                        terminal_rejections += result['terminal_rejections']
                        if result['error']:
                            tx_errors += 1
                            print(f"  Warning: tx {tx_hash} in block {block_num}: {result['error']}", flush=True)
                        if result['rows']:
                            all_rows.extend(result['rows'])
                    except Exception as e:
                        tx_errors += 1
                        print(f"  Warning: Failed to process tx {tx_hash}: {e}", flush=True)
        else:
            for tx in transactions:
                result = analyze_transaction_prestate(tx["hash"], block_num, lookback_points, rpc_url)
                trace_failures += result['trace_failures']
                terminal_rejections += result['terminal_rejections']
                if result['error']:
                    tx_errors += 1
                    print(f"  Warning: tx {tx['hash']} in block {block_num}: {result['error']}", flush=True)
                if result['rows']:
                    all_rows.extend(result['rows'])

        return {
            'block_num': block_num,
            'tx_count': len(transactions),
            'row_count': len(all_rows),
            'rows': all_rows,
            'trace_failures': trace_failures,
            'terminal_rejections': terminal_rejections,
            'tx_errors': tx_errors,
        }

    except Exception as e:
        return {'block_num': block_num, 'tx_count': 0, 'row_count': 0, 'rows': [],
                'trace_failures': 0, 'terminal_rejections': 0, 'tx_errors': 0, 'error': str(e)}


# ──────────────────────────────────────────────────────────────────────────────
# Main Collection Logic
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Collect prestate/poststate data for transactions at different lookbacks'
    )
    parser.add_argument('--block-list', required=True, help='CSV file with block_number column')
    parser.add_argument('--chain', required=True, choices=['ethereum', 'base'],
                       help='Chain name (must match rpc_config.json)')
    parser.add_argument('--block-range', help='Optional: Only process blocks in range, format: START-END')
    parser.add_argument('--num-workers', type=int, default=8, help='Number of parallel block workers')
    parser.add_argument('--tx-parallelism', type=int, default=1, help='Parallel transactions per block')
    parser.add_argument('--output-file', required=True, help='Output DuckDB file path')
    parser.add_argument('--lookbacks', default='0,5,10,20', help='Comma-separated lookback points')

    args = parser.parse_args()

    lookback_points = [int(x.strip()) for x in args.lookbacks.split(',')]

    rpc_config_path = Path(__file__).parent / 'rpc_config.json'
    with open(rpc_config_path, 'r') as f:
        rpc_config = json.load(f)

    if args.chain not in rpc_config:
        print(f"ERROR: Chain '{args.chain}' not found in rpc_config.json")
        sys.exit(1)

    rpc_url = rpc_config[args.chain].get('rpc_tracing') or rpc_config[args.chain]['rpc_url']

    print("="*80)
    print(f"PRESTATE/POSTSTATE COLLECTION - {args.chain.upper()}")
    print("="*80)
    print(f"Chain: {args.chain}")
    print(f"RPC: {rpc_url}")
    print(f"Lookback points: {lookback_points}")
    print(f"Workers: {args.num_workers}")
    print(f"Tx parallelism: {args.tx_parallelism}")
    print(f"Output: {args.output_file}")

    all_blocks = []
    with open(args.block_list, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_num = int(row['block_number'])

            if args.block_range:
                start, end = map(int, args.block_range.split('-'))
                if not (start <= block_num <= end):
                    continue

            all_blocks.append(block_num)

    all_blocks.sort()
    total_blocks = len(all_blocks)

    print(f"\nBlocks to process: {total_blocks:,}")
    if total_blocks == 0:
        print("No blocks to process!")
        sys.exit(0)

    print(f"Block range: {min(all_blocks):,} to {max(all_blocks):,}")

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(output_path))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS prestate_poststate (
            tx_hash VARCHAR,
            block_number BIGINT,
            lookback INTEGER,
            state_type VARCHAR,
            address VARCHAR,
            slot VARCHAR,
            value VARCHAR
        )
    """)

    expected_columns = [
        'tx_hash', 'block_number', 'lookback', 'state_type',
        'address', 'slot', 'value',
    ]
    actual_columns = [
        row[1] for row in conn.execute("PRAGMA table_info('prestate_poststate')").fetchall()
    ]
    if actual_columns != expected_columns:
        conn.close()
        raise SystemExit(
            f"Refusing positional writes: unexpected prestate_poststate schema {actual_columns}"
        )
    invalid_rows = conn.execute(
        """SELECT count(*) FROM prestate_poststate
           WHERE lower(coalesce(state_type, '')) NOT IN ('pre', 'post')
              OR lower(address) IN ('pre', 'post')"""
    ).fetchone()[0]
    if invalid_rows:
        conn.close()
        raise SystemExit(f"Refusing to append to {invalid_rows:,} malformed slot rows")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_blocks (
            block_number BIGINT PRIMARY KEY,
            tx_count INTEGER,
            trace_failures INTEGER
        )
    """)
    processed_columns = {
        row[1] for row in conn.execute("PRAGMA table_info('processed_blocks')").fetchall()
    }
    if 'terminal_rejections' not in processed_columns:
        conn.execute(
            "ALTER TABLE processed_blocks ADD COLUMN terminal_rejections INTEGER DEFAULT 0"
        )

    result = conn.execute("SELECT block_number FROM processed_blocks").fetchall()
    processed_set = {row[0] for row in result}
    all_blocks = [b for b in all_blocks if b not in processed_set]
    print(f"Skipping {len(processed_set):,} already processed blocks")
    print(f"Remaining: {len(all_blocks):,} blocks to process")

    result = conn.execute("""
        SELECT DISTINCT block_number FROM prestate_poststate
        WHERE block_number NOT IN (SELECT block_number FROM processed_blocks)
    """).fetchall()
    stale_blocks = {row[0] for row in result} & set(all_blocks)
    if stale_blocks:
        conn.executemany(
            "DELETE FROM prestate_poststate WHERE block_number = ?",
            [(b,) for b in stale_blocks]
        )
        print(f"Deleted partial rows from {len(stale_blocks):,} unfinished blocks before retrying")

    total_blocks = len(all_blocks)
    if total_blocks == 0:
        print("\nAll blocks already processed!")
        conn.close()
        sys.exit(0)

    print(f"\nStarting collection...")
    print("="*80)

    MAX_PENDING_FUTURES = 50
    batch_size = 10

    total_rows = 0
    total_txs = 0
    total_terminal_rejections = 0
    processed = 0
    blocks_with_failures = []
    start_time = datetime.now()

    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        batch_results = []
        completed_blocks_batch = []
        block_index = 0

        def flush_batches():
            if batch_results:
                conn.executemany(
                    "INSERT INTO prestate_poststate (tx_hash, block_number, lookback, state_type, address, slot, value) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(r['tx_hash'], r['block_number'], r['lookback'], r['state_type'],
                      r['address'], r['slot'], r['value']) for r in batch_results]
                )
                batch_results.clear()
            if completed_blocks_batch:
                conn.executemany(
                    """INSERT OR IGNORE INTO processed_blocks
                       (block_number, tx_count, trace_failures, terminal_rejections) VALUES (?, ?, ?, ?)""",
                    completed_blocks_batch
                )
                completed_blocks_batch.clear()

        while block_index < total_blocks:
            futures = {}
            batch_end = min(block_index + MAX_PENDING_FUTURES, total_blocks)

            for i in range(block_index, batch_end):
                block_num = all_blocks[i]
                future = executor.submit(
                    process_block_prestate, block_num, lookback_points,
                    rpc_url, args.tx_parallelism
                )
                futures[future] = block_num

            for future in as_completed(futures):
                block_num = futures[future]
                processed += 1

                try:
                    result = future.result()

                    if result.get('error'):
                        blocks_with_failures.append((block_num, result['error']))
                        print(f"  ERROR processing block {block_num}: {result['error']}", flush=True)
                    elif result['trace_failures'] or result['tx_errors']:
                        detail = (f"{result['trace_failures']} trace failures, "
                                  f"{result['tx_errors']} tx errors")
                        blocks_with_failures.append((block_num, detail))
                        print(f"  Block {block_num} incomplete: {detail}", flush=True)
                    else:
                        completed_blocks_batch.append((
                            block_num, result['tx_count'], result['trace_failures'],
                            result.get('terminal_rejections', 0),
                        ))
                        total_terminal_rejections += result.get('terminal_rejections', 0)

                    total_txs += result['tx_count']
                    if result['rows']:
                        batch_results.extend(result['rows'])
                        total_rows += result['row_count']

                    if len(batch_results) >= batch_size or completed_blocks_batch:
                        flush_batches()

                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta_seconds = (total_blocks - processed) / rate if rate > 0 else 0

                    print(f"Progress: {processed}/{total_blocks} ({100*processed/total_blocks:.1f}%) | "
                          f"{total_txs:,} txs | {total_rows:,} rows | "
                          f"{rate:.2f} blocks/s | ETA: {eta_seconds/3600:.1f}h", flush=True)

                except Exception as e:
                    blocks_with_failures.append((block_num, str(e)))
                    print(f"  ERROR processing block {block_num}: {e}", flush=True)

            flush_batches()

            block_index = batch_end

    conn.close()

    elapsed_total = (datetime.now() - start_time).total_seconds()

    print("\n" + "="*80)
    print("COLLECTION COMPLETE")
    print("="*80)
    print(f"Total blocks processed: {total_blocks:,}")
    print(f"Total transactions: {total_txs:,}")
    print(f"Total rows collected: {total_rows:,}")
    print(f"Time elapsed: {elapsed_total/3600:.2f} hours")
    print(f"Terminal lookback rejections: {total_terminal_rejections:,}")
    print(f"Average rate: {total_blocks/elapsed_total:.2f} blocks/s")
    print(f"Output: {args.output_file}")
    print(f"Blocks with failures: {len(blocks_with_failures):,}")
    print("="*80)

    if blocks_with_failures:
        print(f"\nWARNING: {len(blocks_with_failures):,} blocks had failures and were "
              f"NOT marked processed (re-run to retry them):", flush=True)
        for block_num, detail in blocks_with_failures:
            print(f"  Block {block_num}: {detail}", flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
