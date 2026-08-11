#!/usr/bin/env python3
"""
Label transactions that use CREATE or CREATE2 opcodes as 'Contract Creation'

These are factory pattern contracts that create other contracts internally.
Uses opcode breakdown parquet files to identify transactions with CREATE/CREATE2 opcodes.

Usage:
    python label_create_opcode_transactions.py --chain base --db /path/to/data/tx_metadata_base.duckdb
    python label_create_opcode_transactions.py --chain ethereum --db /path/to/data/tx_metadata_ethereum.duckdb
"""

import argparse
import duckdb
from pathlib import Path
from datetime import datetime, timedelta

def label_create_opcode_transactions(chain, db_path, start_date, num_days, opcode_path_template):
    """
    Label transactions using CREATE or CREATE2 opcodes as 'Contract Creation'
    """
    print(f"\n{'='*80}")
    print(f"Labeling CREATE/CREATE2 opcode transactions as Contract Creation")
    print(f"{'='*80}")
    print(f"Chain: {chain}")
    print(f"Database: {db_path}")
    print(f"Date range: {start_date} to {start_date + timedelta(days=num_days-1)}")
    print(f"{'='*80}\n")

    con = duckdb.connect(str(db_path))

    # Check if label columns exist
    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
    has_to_category = 'to_category' in columns
    has_to_label = 'to_label' in columns

    if not has_to_category or not has_to_label:
        print("WARNING: Label columns (to_category, to_label) don't exist yet")
        print("  Please run step 10 (Update database with labels) first")
        con.close()
        return False

    # Build list of parquet files to scan
    dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]
    parquet_files = []

    print("Checking for opcode breakdown parquet files...")
    for date in dates:
        date_path = opcode_path_template.format(chain=chain, date=date)
        path = Path(date_path)
        if path.exists():
            parquet_files.append(str(path))
        else:
            print(f"  WARNING: Not found: {date}")

    if not parquet_files:
        print("\nERROR: No opcode breakdown parquet files found!")
        print(f"   Expected path pattern: {opcode_path_template}")
        con.close()
        return False

    print(f"Found {len(parquet_files)} parquet files\n")

    # Process parquet files in 7-day chunks to avoid RAM overload
    print("Querying parquet files for CREATE/CREATE2 transactions (processing in 7-day chunks)...")

    chunk_size = 7
    all_tx_hashes = set()  # Use set to avoid duplicates

    try:
        for chunk_idx in range(0, len(parquet_files), chunk_size):
            chunk_files = parquet_files[chunk_idx:chunk_idx + chunk_size]
            chunk_start_idx = chunk_idx
            chunk_end_idx = min(chunk_idx + chunk_size, len(parquet_files))

            print(f"  Processing files {chunk_start_idx + 1}-{chunk_end_idx} of {len(parquet_files)}...")

            # Build file list for this chunk
            file_list = "', '".join(chunk_files)

            query = f"""
                SELECT DISTINCT '0x' || tx_hash as tx_hash
                FROM read_parquet(['{file_list}'])
                WHERE (COALESCE(CREATE_total_gas, 0) > 0 OR COALESCE(CREATE2_total_gas, 0) > 0)
            """

            result = con.execute(query).fetchdf()
            chunk_hashes = result['tx_hash'].tolist()
            all_tx_hashes.update(chunk_hashes)
            print(f"    Found {len(chunk_hashes):,} transactions in this chunk (total unique: {len(all_tx_hashes):,})")

        tx_hashes = list(all_tx_hashes)
        print(f"\nFound {len(tx_hashes):,} unique transactions using CREATE/CREATE2 opcodes across all chunks")

        if len(tx_hashes) == 0:
            print("  No transactions to label")
            con.close()
            return True

        # Count how many are already labeled (sample check)
        print("\nChecking existing labels...")
        sample_size = min(1000, len(tx_hashes))
        existing_query = f"""
            SELECT COUNT(*) as count
            FROM transactions
            WHERE tx_hash IN ({','.join([f"'{h}'" for h in tx_hashes[:sample_size]])})
              AND to_category IS NOT NULL
        """
        existing_count = con.execute(existing_query).fetchone()[0]
        print(f"  Already labeled (sample of {sample_size}): {existing_count:,}")
        print(f"  Estimated to be updated: ~{len(tx_hashes) - (existing_count * len(tx_hashes) // sample_size):,}")

        # Create temporary table with transaction hashes for faster JOIN
        print("\nCreating temporary table with CREATE/CREATE2 transaction hashes...")
        con.execute("CREATE TEMP TABLE temp_create_txs (tx_hash VARCHAR)")

        # Insert in batches
        batch_size = 10000
        print(f"Inserting {len(tx_hashes):,} transaction hashes into temp table...")
        for i in range(0, len(tx_hashes), batch_size):
            batch = tx_hashes[i:i+batch_size]
            values = ','.join([f"('{h}')" for h in batch])
            con.execute(f"INSERT INTO temp_create_txs VALUES {values}")
            if (i // batch_size + 1) % 10 == 0 or i + batch_size >= len(tx_hashes):
                print(f"  Inserted {min(i + batch_size, len(tx_hashes)):,}/{len(tx_hashes):,} hashes...")

        print("Temporary table created")

        # Single bulk update using JOIN (much faster)
        print("\nUpdating database with bulk JOIN operation...")

        # Count how many will be updated
        count_query = """
            SELECT COUNT(*)
            FROM transactions t
            INNER JOIN temp_create_txs tc ON t.tx_hash = tc.tx_hash
            WHERE t.to_category IS NULL
        """
        rows_to_update = con.execute(count_query).fetchone()[0]
        print(f"  Will update: {rows_to_update:,} transactions")

        # Perform bulk update
        update_query = """
            UPDATE transactions
            SET to_category = 'Contract Creation',
                to_label = 'Contract Creation'
            FROM temp_create_txs
            WHERE transactions.tx_hash = temp_create_txs.tx_hash
              AND transactions.to_category IS NULL
        """
        con.execute(update_query)
        con.commit()

        print(f"Updated {rows_to_update:,} previously unlabeled transactions with CREATE/CREATE2 labels")

        # Clean up temp table
        con.execute("DROP TABLE temp_create_txs")

        if rows_to_update < len(tx_hashes):
            print(f"  ({len(tx_hashes) - rows_to_update:,} transactions were already labeled and preserved)")

        print("\nSummary:")
        summary = con.execute("""
            SELECT
                COUNT(*) as total_contract_creation,
                SUM(CASE WHEN receiver IS NULL THEN 1 ELSE 0 END) as direct_creation,
                SUM(CASE WHEN receiver IS NOT NULL THEN 1 ELSE 0 END) as factory_creation
            FROM transactions
            WHERE to_category = 'Contract Creation'
        """).fetchone()

        print(f"  Total Contract Creation: {summary[0]:,}")
        print(f"    Direct (receiver=NULL): {summary[1]:,}")
        print(f"    Factory (CREATE/CREATE2 opcodes): {summary[2]:,}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        con.close()
        return False

    con.close()
    print("\nComplete")
    return True

def main():
    parser = argparse.ArgumentParser(description='Label CREATE/CREATE2 opcode transactions')
    parser.add_argument('--chain', required=True, choices=['ethereum', 'base'], help='Chain to process')
    parser.add_argument('--db', required=True, help='Path to DuckDB database')
    parser.add_argument('--start-date', default='2025-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--num-days', type=int, default=7, help='Number of days to process')
    parser.add_argument('--opcode-path',
                       default='/path/to/data/opcode_breakdown/{chain}/date={date}/data.parquet',
                       help='Path template for opcode breakdown parquet files')

    args = parser.parse_args()

    # Parse start date
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')

    success = label_create_opcode_transactions(
        args.chain,
        args.db,
        start_date,
        args.num_days,
        args.opcode_path
    )

    return 0 if success else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
