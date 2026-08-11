#!/usr/bin/env python3
"""
Collect state sensitivity for specific blocks from a CSV file.

Reads block numbers from a CSV (block_number column), runs eth_estimateGas at
each lookback point per transaction, and writes results to DuckDB — same schema
as collect_tx_analysis_integrated.py.
"""

import argparse
import csv
import time as time_module
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List
import duckdb

from collect_tx_analysis_integrated import (
    get_rpc_url,
    process_block_integrated,
    insert_batch,
    verify_table_schema,
)


def main() -> None:
    parser = argparse.ArgumentParser(description='Collect state sensitivity for blocks from CSV')
    parser.add_argument('--block-list', required=True, help='CSV file with block_number column')
    parser.add_argument('--chain', default='base', help='Chain name')
    parser.add_argument('--num-workers', type=int, default=8, help='Number of parallel block workers')
    parser.add_argument('--tx-parallelism', type=int, default=4, help='Number of parallel tx threads per block')
    parser.add_argument('--sample-points', default='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20', help='Comma-separated lookback points')
    parser.add_argument('--output-file', required=True, help='Output database filename')
    parser.add_argument('--block-range', help='Optional: Only process blocks in range, format: START-END')

    args = parser.parse_args()

    sample_points = sorted([int(x.strip()) for x in args.sample_points.split(',')])
    rpc_url = get_rpc_url(args.chain)
    db_file = Path(args.output_file)

    print("=" * 80)
    print("STATE SENSITIVITY COLLECTION FROM BLOCK LIST")
    print("=" * 80)
    print(f"Chain: {args.chain}")
    print(f"Block list: {args.block_list}")
    print(f"Workers: {args.num_workers}")
    print(f"Output: {db_file}")
    print("=" * 80)

    all_blocks: List[int] = []
    expected_tx_counts: Dict[int, int] = {}
    with open(args.block_list) as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_num = int(row['block_number'])
            if args.block_range:
                start, end = map(int, args.block_range.split('-'))
                if not (start <= block_num <= end):
                    continue
            all_blocks.append(block_num)
            if row.get("paper_tx_count") not in (None, ""):
                expected_tx_counts[block_num] = int(row["paper_tx_count"])

    if len(all_blocks) != len(set(all_blocks)):
        raise RuntimeError("block list contains duplicate block numbers")
    if expected_tx_counts and len(expected_tx_counts) != len(all_blocks):
        raise RuntimeError(
            "paper_tx_count must be present for every block or no blocks")
    all_blocks.sort()
    requested_blocks = list(all_blocks)
    print(f"Found {len(all_blocks):,} blocks to process")
    if all_blocks:
        print(f"Block range: {all_blocks[0]:,} to {all_blocks[-1]:,}")

    if not all_blocks:
        print("ERROR: No blocks to process!")
        return

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

    main_counts = dict(conn.execute(
        "SELECT block_number, count(*) FROM transactions "
        "WHERE block_number IS NOT NULL GROUP BY block_number").fetchall())
    error_counts = dict(error_conn.execute(
        "SELECT block_number, count(*) FROM transactions "
        "WHERE block_number IS NOT NULL GROUP BY block_number").fetchall())
    existing_blocks = set(main_counts) | set(error_counts)
    if expected_tx_counts:
        complete_blocks = {
            block for block in all_blocks
            if main_counts.get(block, 0) + error_counts.get(block, 0)
            == expected_tx_counts.get(block)
        }
        incomplete_blocks = sorted((existing_blocks & set(all_blocks)) - complete_blocks)
        if incomplete_blocks:
            conn.executemany(
                "DELETE FROM transactions WHERE block_number = ?",
                [(block,) for block in incomplete_blocks])
            error_conn.executemany(
                "DELETE FROM transactions WHERE block_number = ?",
                [(block,) for block in incomplete_blocks])
            conn.commit()
            error_conn.commit()
            print(f"Cleared {len(incomplete_blocks):,} incomplete existing blocks")
    else:
        complete_blocks = set(main_counts)
    all_blocks = [block for block in all_blocks if block not in complete_blocks]
    print(f"Skipping {len(complete_blocks):,} exactly complete blocks")
    print(f"Remaining: {len(all_blocks):,} blocks to process")

    if not all_blocks:
        print("\nAll blocks already processed!")
        conn.close()
        error_conn.close()
        return

    total_blocks = len(all_blocks)
    column_names = [
        "tx_hash", "block_number", "tx_index", "from_address", "to_address", "value",
        "gas_limit", "gas_used", "gas_price", "max_fee_per_gas", "max_priority_fee_per_gas",
        "nonce", "method_signature", "input_size", "status", "contract_address", "logs_count",
        *[f"gas_estimate_lookback_{i}" for i in sample_points],
        "max_lookback_success", "first_fail_block", "first_fail_error"
    ]

    MAX_PENDING = 50
    blocks_processed = 0
    failed_blocks: List[tuple[int, str]] = []
    total_txs_written = 0
    start_time = time_module.time()
    batch_results: List[Dict[str, Any]] = []
    block_index = 0

    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        while block_index < total_blocks:
            batch_end = min(block_index + MAX_PENDING, total_blocks)
            futures: Dict[Any, int] = {}

            for i in range(block_index, batch_end):
                future = executor.submit(
                    process_block_integrated,
                    all_blocks[i], sample_points, rpc_url, args.tx_parallelism
                )
                futures[future] = all_blocks[i]

            for future in as_completed(futures):
                block_num = futures[future]
                try:
                    results = future.result()
                    blocks_processed += 1
                    total_txs_written += len(results)
                    batch_results.extend(results)

                    if len(batch_results) >= 100:
                        insert_batch(batch_results, column_names, conn, error_conn)
                        batch_results.clear()

                    if blocks_processed % 10 == 0 or blocks_processed == total_blocks:
                        elapsed = time_module.time() - start_time
                        rate = blocks_processed / elapsed if elapsed > 0 else 0
                        pct = blocks_processed / total_blocks * 100
                        print(f"Progress: {blocks_processed:,}/{total_blocks:,} ({pct:.1f}%) | {total_txs_written:,} txs | {rate:.2f} blocks/s", flush=True)

                except Exception as e:
                    blocks_processed += 1
                    failed_blocks.append((block_num, str(e)))
                    print(f"[Block {block_num:,}] Error: {e}", flush=True)

            block_index = batch_end

        if batch_results:
            insert_batch(batch_results, column_names, conn, error_conn)

    if expected_tx_counts:
        final_main = dict(conn.execute(
            "SELECT block_number, count(*) FROM transactions "
            "WHERE block_number IS NOT NULL GROUP BY block_number").fetchall())
        final_error = dict(error_conn.execute(
            "SELECT block_number, count(*) FROM transactions "
            "WHERE block_number IS NOT NULL GROUP BY block_number").fetchall())
        bad_counts = {
            block: (final_main.get(block, 0) + final_error.get(block, 0), expected_tx_counts[block])
            for block in requested_blocks
            if final_main.get(block, 0) + final_error.get(block, 0) != expected_tx_counts[block]
        }
        if bad_counts:
            examples = list(bad_counts.items())[:10]
            raise RuntimeError(f"final transaction-count mismatch in {len(bad_counts)} blocks: {examples}")

    conn.close()
    error_conn.close()

    print(f"\n{'=' * 80}")
    print(f"Complete! Processed {blocks_processed:,} blocks, {total_txs_written:,} transactions")
    print(f"Main DB: {db_file}")
    print(f"Error DB: {error_db_file}")
    if failed_blocks:
        print(f"Failed blocks: {len(failed_blocks):,}")
        for block_num, error in failed_blocks:
            print(f"  {block_num}: {error}")
        raise RuntimeError(
            f"{len(failed_blocks)} blocks failed; rerun to collect them")
    print("=" * 80)


if __name__ == "__main__":
    main()
