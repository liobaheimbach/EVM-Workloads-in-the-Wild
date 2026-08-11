#!/usr/bin/env python3
"""
State sensitivity collection: measures how gas estimates vary across lookback distances.

For each block, fetches all transactions and calls eth_estimateGas at each lookback
state (block - 1 - lookback). Results are stored wide: one row per transaction with
one gas_estimate_lookback_N column per sample point.
"""

import json
import os
import sys
import time as time_module
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / '.env')

_UTILS_DIR = Path(__file__).resolve().parent.parent / 'utils'
if str(_UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(_UTILS_DIR))

from tx_call_object import build_call_object

# ── RPC error classification ────────────────────────────────────────────────

class RetryableRPCError(RuntimeError):
    pass


TERMINAL_ESTIMATE_SUBSTRINGS = (
    "execution reverted",
    "gas required exceeds allowance",
    "insufficient funds",
    "less than block base fee",
    "fee cap less than",
    "nonce too ",
    "intrinsic gas",
    "out of gas",
    "exceeds block gas limit",
    "gas limit reached",
    "evm error:",
)


def is_terminal_estimate_error(error: Optional[str]) -> bool:
    message = str(error or "").lower()
    return any(part in message for part in TERMINAL_ESTIMATE_SUBSTRINGS)


# ── Session pool (per-process, reused across RPC calls) ──────────────────────

_session_pool: Dict[int, requests.Session] = {}

def _get_session() -> requests.Session:
    pid = os.getpid()
    if pid not in _session_pool:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _session_pool[pid] = session
    return _session_pool[pid]


# ── RPC helpers ───────────────────────────────────────────────────────────────

def get_rpc_url(chain: str) -> str:
    config_path = Path(__file__).parent / 'rpc_config.json'
    with open(config_path) as f:
        config = json.load(f)
    if chain not in config:
        raise ValueError(f"Chain '{chain}' not found in rpc_config.json")
    rpc_url = config[chain].get('rpc_url')
    if not rpc_url:
        raise ValueError(f"No rpc_url configured for chain '{chain}'")
    return rpc_url


def _rpc(rpc_url: str, method: str, params: List[Any], with_error: bool = False) -> Any:
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    try:
        resp = _get_session().post(rpc_url, json=payload, timeout=(30, 120))
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        raise RetryableRPCError(f"RPC {method} transport failure: {exc}") from exc
    if not isinstance(body, dict):
        raise RetryableRPCError(f"RPC {method} returned a non-object response")
    err = body.get("error")
    err_msg = err.get("message", str(err)) if isinstance(err, dict) else (str(err) if err else None)
    if with_error:
        if body.get("result") is None and err_msg is None:
            raise RetryableRPCError(f"RPC {method} returned neither result nor error")
        return body.get("result"), err_msg
    if err_msg is not None:
        raise RetryableRPCError(f"RPC {method} error: {err_msg}")
    return body.get("result")


def _rpc_batch(rpc_url: str, calls: List[Dict[str, Any]]) -> List[Any]:
    """Send a batch RPC request, return results in request order."""
    try:
        resp = _get_session().post(rpc_url, json=calls, timeout=(240, 900))
        resp.raise_for_status()
        results = resp.json()
    except Exception as exc:
        raise RetryableRPCError(f"batch RPC transport failure: {exc}") from exc
    if not isinstance(results, list):
        raise RetryableRPCError("batch RPC returned a non-list response")
    by_id = {row.get("id"): row.get("result") for row in results if isinstance(row, dict)}
    return [by_id.get(call["id"]) for call in calls]


def _get_block(block_number: int, rpc_url: str, full_txs: bool = True) -> Optional[Dict[str, Any]]:
    return _rpc(rpc_url, "eth_getBlockByNumber", [hex(block_number), full_txs])


def _estimate_gas_at_state(tx: Dict[str, Any], state_block: int, rpc_url: str) -> tuple[Optional[int], Optional[str]]:
    """
    Call eth_estimateGas replaying tx on the state at state_block.
    Returns (gas estimate as int, None), or (None, error message) if the call reverts/fails.
    """
    call = {k: v for k, v in build_call_object(tx).items() if v is not None}

    result, error = _rpc(rpc_url, "eth_estimateGas", [call, hex(state_block)], with_error=True)
    if result is None:
        message = error or f"eth_estimateGas returned no result at state block {state_block}"
        if not is_terminal_estimate_error(message):
            raise RetryableRPCError(message)
        return None, message
    try:
        return int(result, 16), None
    except (ValueError, TypeError) as exc:
        raise RetryableRPCError(
            f"unparseable eth_estimateGas result: {result!r}") from exc


# ── Block sampling ────────────────────────────────────────────────────────────

def sample_blocks_for_date_range(
    start_date: str,
    end_date: str,
    blocks_per_day: int,
    rpc_url: str,
    existing_blocks: set[int],
) -> Dict[str, List[int]]:
    """
    Sample random blocks per day between start_date and end_date (YYYY-MM-DD).
    Excludes blocks already in existing_blocks.
    Estimates block numbers from timestamps via binary search on eth_getBlockByNumber.
    """
    import random
    from datetime import datetime, timedelta, timezone

    def _block_at_timestamp(target_ts: int, lo: int, hi: int) -> int:
        while lo < hi:
            mid = (lo + hi) // 2
            block = None
            for _ in range(3):
                block = _get_block(mid, rpc_url, full_txs=False)
                if block is not None:
                    break
                time_module.sleep(1)
            if block is None:
                raise RuntimeError(f"eth_getBlockByNumber failed for block {mid} during timestamp binary search")
            ts = int(block["timestamp"], 16)
            if ts < target_ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    latest_block = _rpc(rpc_url, "eth_blockNumber", [])
    if latest_block is None:
        raise RuntimeError("Could not fetch latest block number")
    chain_tip = int(latest_block, 16)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    result: Dict[str, List[int]] = {}
    current = start_dt
    while current <= end_dt:
        day_str = current.strftime("%Y-%m-%d")
        day_start_ts = int(current.timestamp())
        day_end_ts = int((current + timedelta(days=1)).timestamp())

        day_start_block = _block_at_timestamp(day_start_ts, 0, chain_tip)
        day_end_block = _block_at_timestamp(day_end_ts, day_start_block, chain_tip)

        already_collected = 0
        candidates = []
        for b in range(day_start_block, day_end_block):
            if b in existing_blocks:
                already_collected += 1
            else:
                candidates.append(b)
        sample_size = min(max(0, blocks_per_day - already_collected), len(candidates))
        if sample_size > 0:
            result[day_str] = sorted(random.sample(candidates, sample_size))
        else:
            result[day_str] = []

        current += timedelta(days=1)

    return result


# ── Per-transaction gas estimation ───────────────────────────────────────────

def _estimate_tx_at_lookbacks(
    tx: Dict[str, Any],
    receipt: Optional[Dict[str, Any]],
    block_number: int,
    sample_points: List[int],
    rpc_url: str,
) -> Dict[str, Any]:
    """
    For a single transaction, call eth_estimateGas at each lookback state.
    Returns a result dict matching the transactions table schema.
    """
    tx_hash = tx["hash"]

    # Skip zero-gas transactions (e.g., system txs on L2s)
    gas_used = int(receipt["gasUsed"], 16) if receipt else 0
    if gas_used == 0:
        return {"skip": True}

    row: Dict[str, Any] = {
        "tx_hash": tx_hash,
        "block_number": block_number,
        "tx_index": int(tx.get("transactionIndex", "0x0"), 16),
        "from_address": tx.get("from"),
        "to_address": tx.get("to"),
        "value": int(tx.get("value", "0x0"), 16),
        "gas_limit": int(tx.get("gas", "0x0"), 16),
        "gas_used": gas_used,
        "gas_price": int(tx.get("gasPrice", "0x0"), 16) if tx.get("gasPrice") else None,
        "max_fee_per_gas": int(tx.get("maxFeePerGas", "0x0"), 16) if tx.get("maxFeePerGas") else None,
        "max_priority_fee_per_gas": int(tx.get("maxPriorityFeePerGas", "0x0"), 16) if tx.get("maxPriorityFeePerGas") else None,
        "nonce": int(tx.get("nonce", "0x0"), 16),
        "method_signature": tx["input"][:10] if tx.get("input") and len(tx.get("input", "")) >= 10 else None,
        "input_size": len(bytes.fromhex(tx["input"][2:])) if tx.get("input") and tx["input"] != "0x" else 0,
        "status": int(receipt["status"], 16) if receipt and receipt.get("status") else None,
        "contract_address": receipt.get("contractAddress") if receipt else None,
        "logs_count": len(receipt.get("logs", [])) if receipt else None,
    }

    max_lookback_success = None
    first_fail_block = None
    first_fail_error = None

    for lb in sample_points:
        state_block = block_number - 1 - lb
        if state_block < 0:
            row[f"gas_estimate_lookback_{lb}"] = None
            continue

        estimate, estimate_error = _estimate_gas_at_state(tx, state_block, rpc_url)
        row[f"gas_estimate_lookback_{lb}"] = estimate

        if estimate is not None:
            max_lookback_success = lb
        elif first_fail_block is None:
            first_fail_block = state_block
            first_fail_error = estimate_error or f"eth_estimateGas failed at state block {state_block}"

    row["max_lookback_success"] = max_lookback_success
    row["first_fail_block"] = first_fail_block
    row["first_fail_error"] = first_fail_error

    if max_lookback_success is None:
        row["max_lookback_success"] = -1
        row["error_db"] = True

    return row


# ── Block-level worker (runs in subprocess) ───────────────────────────────────

def process_block_integrated(
    block_number: int,
    sample_points: List[int],
    rpc_url: str,
    tx_parallelism: int,
) -> List[Dict[str, Any]]:
    """
    Fetch all transactions in block_number and estimate gas at each lookback point.
    Designed to run inside a ProcessPoolExecutor worker.

    Returns list of result dicts (one per non-skipped transaction).
    """
    block = _get_block(block_number, rpc_url, full_txs=True)
    if block is None:
        raise RuntimeError(f"failed to fetch block {block_number}")
    if not block.get("transactions"):
        return []

    txs = block["transactions"]
    tx_hashes = [tx["hash"] for tx in txs]

    receipt_calls = [
        {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [h], "id": i}
        for i, h in enumerate(tx_hashes)
    ]
    receipts_raw = _rpc_batch(rpc_url, receipt_calls)
    missing = [i for i, r in enumerate(receipts_raw) if r is None]
    if missing:
        retry_results = _rpc_batch(rpc_url, [receipt_calls[i] for i in missing])
        for i, r in zip(missing, retry_results):
            receipts_raw[i] = r
    if all(r is None for r in receipts_raw):
        raise RuntimeError(f"receipt batch RPC failed for block {block_number} (after retry)")
    missing_receipts = [tx_hashes[i] for i, r in enumerate(receipts_raw) if r is None]
    if missing_receipts:
        raise RetryableRPCError(
            f"{len(missing_receipts)} receipts missing after retry in block {block_number}")
    receipts: Dict[str, Any] = {tx_hashes[i]: receipts_raw[i] for i in range(len(tx_hashes))}

    results = []
    skipped = 0
    tx_errors = []
    with ThreadPoolExecutor(max_workers=tx_parallelism) as pool:
        futures = {
            pool.submit(
                _estimate_tx_at_lookbacks,
                tx, receipts.get(tx["hash"]), block_number, sample_points, rpc_url
            ): tx["hash"]
            for tx in txs
        }
        for future in as_completed(futures):
            try:
                row = future.result()
                if row.get("skip"):
                    skipped += 1
                else:
                    results.append(row)
            except Exception as exc:
                tx_errors.append((futures[future], str(exc)))

    if tx_errors:
        examples = "; ".join(f"{tx_hash}: {error}" for tx_hash, error in tx_errors[:3])
        raise RetryableRPCError(
            f"{len(tx_errors)} transaction analyses failed in block {block_number}: {examples}")
    if len(results) + skipped != len(txs):
        raise RuntimeError(
            f"block {block_number} produced {len(results)} rows and {skipped} skips "
            f"for {len(txs)} transactions")
    return results


# ── DB helpers ────────────────────────────────────────────────────────────────

def verify_table_schema(conn: Any, sample_points: List[int], db_file: Path) -> None:
    """Exit if an existing transactions table has lookback columns that don't match sample_points."""
    actual_columns = {row[1] for row in conn.execute("PRAGMA table_info('transactions')").fetchall()}
    expected_lookbacks = {f"gas_estimate_lookback_{lb}" for lb in sample_points}
    actual_lookbacks = {c for c in actual_columns if c.startswith("gas_estimate_lookback_")}
    if actual_lookbacks != expected_lookbacks:
        existing_points = sorted(int(c.rsplit("_", 1)[1]) for c in actual_lookbacks)
        print(f"ERROR: {db_file} already contains a transactions table created with different sample points.")
        print(f"  Existing lookback columns: {','.join(str(p) for p in existing_points)}")
        print(f"  Requested sample points:   {','.join(str(p) for p in sample_points)}")
        print("  Rerun with matching --sample-points or use a different output file.")
        sys.exit(1)


def insert_batch(batch_results: List[Dict[str, Any]], column_names: List[str], conn: Any, error_conn: Any) -> None:
    main_rows = []
    error_rows = []
    skipped = 0

    for result in batch_results:
        if result.get('skip'):
            skipped += 1
            continue

        is_error = result.get('error_db', False)

        row = []
        for col in column_names:
            val = result.get(col)
            if val == '':
                val = None
            row.append(val)

        if is_error:
            error_msg = result.get('first_fail_error', 'Unknown')
            error_rows.append(tuple(row) + (error_msg,))
        else:
            main_rows.append(tuple(row))

    if main_rows:
        placeholders = ', '.join(['?' for _ in column_names])
        conn.executemany(
            f"INSERT OR IGNORE INTO transactions ({', '.join(column_names)}) VALUES ({placeholders})",
            main_rows
        )
        conn.commit()
        tx_hash_idx = column_names.index('tx_hash')
        error_conn.executemany(
            "DELETE FROM transactions WHERE tx_hash = ?",
            [(row[tx_hash_idx],) for row in main_rows]
        )
        error_conn.commit()

    if error_rows:
        error_columns = column_names + ['error']
        placeholders = ', '.join(['?' for _ in error_columns])
        error_conn.executemany(
            f"INSERT OR IGNORE INTO transactions ({', '.join(error_columns)}) VALUES ({placeholders})",
            error_rows
        )
        error_conn.commit()

    if skipped > 0:
        print(f"  Skipped {skipped} txs with gas_used=0", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description='Memory-efficient integrated transaction analysis')
    parser.add_argument('--chain', default='base', help='Chain name')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--blocks-per-day', type=int, default=3000, help='Number of blocks to sample per day')
    parser.add_argument('--num-workers', type=int, default=4, help='Number of parallel block workers')
    parser.add_argument('--tx-parallelism', type=int, default=4, help='Number of parallel tx threads per block')
    parser.add_argument('--sample-points', default='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20', help='Comma-separated lookback points')
    parser.add_argument('--output-dir', help='Output directory')
    parser.add_argument('--output-file', help='Output database filename')
    parser.add_argument('--chunk-size', type=int, default=100, help='Process blocks in chunks of this size')
    parser.add_argument('--max-pending-futures', type=int, default=50, help='Max pending futures in memory')

    args = parser.parse_args()

    sample_points = sorted([int(x.strip()) for x in args.sample_points.split(',')])
    rpc_url = get_rpc_url(args.chain)

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        db_base = os.getenv('DB_BASE_DIR')
        if not db_base:
            print("ERROR: DB_BASE_DIR not set in .env file")
            sys.exit(1)
        output_dir = Path(db_base) / 'estimate_sensitivity'

    output_dir.mkdir(parents=True, exist_ok=True)

    if args.output_file:
        db_file = Path(args.output_file) if Path(args.output_file).is_absolute() else output_dir / args.output_file
    else:
        db_file = output_dir / f"state_sensitivity_analysis_{args.chain}.duckdb"

    print("=" * 80)
    print("MEMORY-EFFICIENT INTEGRATED TRANSACTION ANALYSIS")
    print("=" * 80)
    print(f"Chain: {args.chain}")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Workers: {args.num_workers}")
    print(f"Chunk size: {args.chunk_size} blocks")
    print(f"Max pending futures: {args.max_pending_futures}")
    print(f"Output: {db_file}")
    print("=" * 80)

    conn = duckdb.connect(str(db_file))
    error_db_file = db_file.parent / f"{db_file.stem}_errors.duckdb"
    error_conn = duckdb.connect(str(error_db_file))

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS transactions (
        tx_hash VARCHAR PRIMARY KEY,
        block_number BIGINT, tx_index INTEGER, from_address VARCHAR, to_address VARCHAR,
        value HUGEINT, gas_limit BIGINT, gas_used BIGINT, gas_price BIGINT,
        max_fee_per_gas BIGINT, max_priority_fee_per_gas BIGINT, nonce BIGINT,
        method_signature VARCHAR, input_size INTEGER, status INTEGER,
        contract_address VARCHAR, logs_count INTEGER,
        {', '.join([f'gas_estimate_lookback_{i} BIGINT' for i in sample_points])},
        max_lookback_success INTEGER, first_fail_block BIGINT, first_fail_error VARCHAR
    )
    """
    conn.execute(create_table_sql)
    error_conn.execute(create_table_sql.replace(
        "first_fail_error VARCHAR",
        "first_fail_error VARCHAR, error VARCHAR"
    ))
    verify_table_schema(conn, sample_points, db_file)
    verify_table_schema(error_conn, sample_points, error_db_file)

    print("Databases initialized")

    existing_blocks: set[int] = set()
    try:
        result = conn.execute("SELECT DISTINCT block_number FROM transactions WHERE block_number IS NOT NULL").fetchall()
        existing_blocks = {row[0] for row in result}
        print(f"Found {len(existing_blocks):,} blocks already processed")
    except Exception as e:
        print(f"Warning: Could not query existing blocks (first run?): {e}")

    sampled_blocks_by_date = sample_blocks_for_date_range(
        args.start_date, args.end_date, args.blocks_per_day, rpc_url, existing_blocks
    )

    all_blocks = [b for blocks in sorted(sampled_blocks_by_date.items(), key=lambda x: x[0]) for b in blocks[1]]
    total_blocks = len(all_blocks)
    if total_blocks == 0:
        print("\nNo new blocks to process.")
        conn.close()
        error_conn.close()
        return

    print(f"\nTotal blocks to process: {total_blocks:,}")
    print("=" * 80)

    column_names = [
        "tx_hash", "block_number", "tx_index", "from_address", "to_address", "value",
        "gas_limit", "gas_used", "gas_price", "max_fee_per_gas", "max_priority_fee_per_gas",
        "nonce", "method_signature", "input_size", "status", "contract_address", "logs_count",
        *[f"gas_estimate_lookback_{i}" for i in sample_points],
        "max_lookback_success", "first_fail_block", "first_fail_error"
    ]

    blocks_processed = 0
    total_txs_written = 0
    failed_blocks: List[tuple[int, str]] = []
    start_time = time_module.time()

    for chunk_start in range(0, total_blocks, args.chunk_size):
        chunk_end = min(chunk_start + args.chunk_size, total_blocks)
        chunk_blocks = all_blocks[chunk_start:chunk_end]

        print(f"\n[Chunk {chunk_start // args.chunk_size + 1}] Processing blocks {chunk_start + 1:,} to {chunk_end:,}", flush=True)

        with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
            futures: Dict[Any, int] = {}
            batch_results: List[Dict[str, Any]] = []

            blocks_to_submit = chunk_blocks[:args.max_pending_futures]
            remaining_blocks = list(chunk_blocks[args.max_pending_futures:])

            for block_num in blocks_to_submit:
                future = executor.submit(process_block_integrated, block_num, sample_points, rpc_url, args.tx_parallelism)
                futures[future] = block_num

            pending = set(futures)
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    block_num = futures.pop(future)
                    blocks_processed += 1

                    try:
                        results = future.result()
                        batch_results.extend(results)
                        total_txs_written += len(results)

                        if len(batch_results) >= 100:
                            insert_batch(batch_results, column_names, conn, error_conn)
                            batch_results = []

                    except Exception as e:
                        failed_blocks.append((block_num, str(e)))
                        print(f"[Block {block_num:,}] Error: {e}", flush=True)

                    if remaining_blocks:
                        next_block = remaining_blocks.pop(0)
                        new_future = executor.submit(process_block_integrated, next_block, sample_points, rpc_url, args.tx_parallelism)
                        futures[new_future] = next_block
                        pending.add(new_future)

                    if blocks_processed % 10 == 0:
                        elapsed = time_module.time() - start_time
                        rate = blocks_processed / elapsed if elapsed > 0 else 0
                        print(f"Progress: {blocks_processed:,}/{total_blocks:,} ({100 * blocks_processed / total_blocks:.1f}%) | {total_txs_written:,} txs | {rate:.2f} blocks/s", flush=True)

            if batch_results:
                insert_batch(batch_results, column_names, conn, error_conn)

        print(f"[Chunk {chunk_start // args.chunk_size + 1}] Completed.", flush=True)

    conn.close()
    error_conn.close()

    print(f"\n{'=' * 80}")
    print(f"Analysis complete!")
    print(f"Processed {blocks_processed:,} blocks, {total_txs_written:,} transactions")
    if failed_blocks:
        print(f"Failed blocks: {len(failed_blocks):,}")
        for block_num, error in failed_blocks:
            print(f"  {block_num}: {error}")
    print(f"Main DB: {db_file}")
    print(f"Error DB: {error_db_file}")
    print("=" * 80)
    if failed_blocks:
        raise RuntimeError(
            f"{len(failed_blocks)} blocks failed; rerun to collect them")


if __name__ == "__main__":
    main()
