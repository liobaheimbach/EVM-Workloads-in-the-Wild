#!/usr/bin/env python3
"""
Add receiver_is_eoa and receiver_is_contract columns to transactions table
and populate them from identified_pools_tokens CSV.

Usage:
    python add_eoa_contract_columns.py --db ~/data.duckdb --csv identified_pools_tokens.csv
"""

import argparse
import duckdb
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_columns_if_not_exist(con):
    """Add receiver_is_eoa and receiver_is_contract columns if they don't exist."""
    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]

    if 'receiver_is_eoa' not in columns:
        logger.info("Adding receiver_is_eoa column...")
        con.execute("ALTER TABLE transactions ADD COLUMN receiver_is_eoa BOOLEAN DEFAULT NULL")
        logger.info("Added receiver_is_eoa column")
    else:
        logger.info("receiver_is_eoa column already exists")

    if 'receiver_is_contract' not in columns:
        logger.info("Adding receiver_is_contract column...")
        con.execute("ALTER TABLE transactions ADD COLUMN receiver_is_contract BOOLEAN DEFAULT NULL")
        logger.info("Added receiver_is_contract column")
    else:
        logger.info("receiver_is_contract column already exists")

def update_from_csv(con, csv_path: Path):
    """Update database with EOA/Contract flags from CSV."""
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return False

    logger.info(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)

    logger.info(f"Loaded {len(df):,} addresses")
    logger.info(f"  EOAs: {df['is_eoa'].sum():,}")
    logger.info(f"  Contracts: {df['is_contract'].sum():,}")

    # Create temporary table with address mappings
    logger.info("Creating temporary table with address mappings...")
    con.execute("DROP TABLE IF EXISTS temp_address_types")
    con.execute("""
        CREATE TEMPORARY TABLE temp_address_types (
            address VARCHAR,
            is_eoa BOOLEAN,
            is_contract BOOLEAN
        )
    """)

    # Insert data in batches
    batch_size = 10000
    for i in range(0, len(df), batch_size):
        batch = df[i:i+batch_size]
        values = [(row['Address'].lower(), bool(row['is_eoa']), bool(row['is_contract']))
                  for _, row in batch.iterrows()]
        con.executemany(
            "INSERT INTO temp_address_types VALUES (?, ?, ?)",
            values
        )
        if (i + batch_size) % 100000 == 0:
            logger.info(f"  Inserted {i + batch_size:,} addresses...")

    logger.info(f"Inserted {len(df):,} address mappings into temporary table")

    # Update transactions table
    logger.info("Updating transactions table...")
    result = con.execute("""
        UPDATE transactions t
        SET
            receiver_is_eoa = temp.is_eoa,
            receiver_is_contract = temp.is_contract
        FROM temp_address_types temp
        WHERE LOWER(t.receiver) = temp.address
    """)

    updated_count = con.execute("""
        SELECT COUNT(*)
        FROM transactions
        WHERE receiver_is_eoa IS NOT NULL OR receiver_is_contract IS NOT NULL
    """).fetchone()[0]

    logger.info(f"Updated {updated_count:,} transaction records")

    # Show statistics
    eoa_count = con.execute("""
        SELECT COUNT(DISTINCT receiver)
        FROM transactions
        WHERE receiver_is_eoa = TRUE
    """).fetchone()[0]

    contract_count = con.execute("""
        SELECT COUNT(DISTINCT receiver)
        FROM transactions
        WHERE receiver_is_contract = TRUE
    """).fetchone()[0]

    logger.info(f"\nDatabase statistics:")
    logger.info(f"  Unique EOA receivers: {eoa_count:,}")
    logger.info(f"  Unique contract receivers: {contract_count:,}")

    return True

def main():
    parser = argparse.ArgumentParser(description="Add and populate EOA/Contract columns")
    parser.add_argument('--db', required=True, help='Path to DuckDB database')
    parser.add_argument('--csv', required=True, help='Path to identified_pools_tokens CSV')

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("ADD EOA/CONTRACT COLUMNS TO DATABASE")
    logger.info("="*80)
    logger.info(f"Database: {args.db}")
    logger.info(f"CSV: {args.csv}")
    logger.info("="*80)

    con = duckdb.connect(args.db)

    # Add columns
    add_columns_if_not_exist(con)

    # Update from CSV
    success = update_from_csv(con, Path(args.csv))

    con.close()

    if success:
        logger.info("\n" + "="*80)
        logger.info("SUCCESSFULLY UPDATED DATABASE")
        logger.info("="*80)
        return 0
    else:
        logger.error("\n" + "="*80)
        logger.error("FAILED: FAILED TO UPDATE DATABASE")
        logger.error("="*80)
        return 1

if __name__ == '__main__':
    exit(main())
