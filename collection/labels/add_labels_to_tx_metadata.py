#!/usr/bin/env python3
"""
Add label and category columns to the tx_metadata transactions table from a labels CSV.
"""

import duckdb
import os
import argparse
from pathlib import Path

def add_label_columns(db_path):
    """Add label columns to transactions table if they don't exist."""
    con = duckdb.connect(db_path)

    # Check current schema
    schema = con.execute("DESCRIBE transactions").fetchall()
    existing_cols = {col[0] for col in schema}

    # Add columns for from_address labels
    if 'from_label' not in existing_cols:
        con.execute("ALTER TABLE transactions ADD COLUMN from_label VARCHAR")
        print("Added 'from_label' column")

    if 'from_category' not in existing_cols:
        con.execute("ALTER TABLE transactions ADD COLUMN from_category VARCHAR")
        print("Added 'from_category' column")

    # Add columns for to_address labels
    if 'to_label' not in existing_cols:
        con.execute("ALTER TABLE transactions ADD COLUMN to_label VARCHAR")
        print("Added 'to_label' column")

    if 'to_category' not in existing_cols:
        con.execute("ALTER TABLE transactions ADD COLUMN to_category VARCHAR")
        print("Added 'to_category' column")

    con.close()

def update_labels_in_db(db_path, labels_csv_path):
    """Update transactions table with label information using fast JOIN approach."""
    con = duckdb.connect(db_path)

    print("\nCreating temporary labels table...")
    con.execute("""
        CREATE TEMP TABLE temp_labels (
            address VARCHAR,
            label VARCHAR,
            category VARCHAR
        )
    """)

    con.execute(f"""
        INSERT INTO temp_labels
        SELECT
            LOWER(Address) as address,
            Application_Name as label,
            Category as category
        FROM read_csv_auto('{labels_csv_path}')
    """)

    label_count = con.execute("SELECT COUNT(*) FROM temp_labels").fetchone()[0]
    print(f"Loaded {label_count:,} labels into temporary table")

    print("\nUpdating sender labels...")
    from_count = con.execute("""
        SELECT COUNT(*)
        FROM transactions
        JOIN temp_labels ON LOWER(transactions.sender) = temp_labels.address
        WHERE transactions.from_label IS NULL
           OR transactions.from_category IS NULL
           OR transactions.from_category = 'Uncategorized'
           OR transactions.from_label != temp_labels.label
           OR transactions.from_category != temp_labels.category
    """).fetchone()[0]

    con.execute("""
        UPDATE transactions
        SET from_label = temp_labels.label,
            from_category = temp_labels.category
        FROM temp_labels
        WHERE LOWER(transactions.sender) = temp_labels.address
          AND (transactions.from_label IS NULL
               OR transactions.from_category IS NULL
               OR transactions.from_category = 'Uncategorized'
               OR transactions.from_label != temp_labels.label
               OR transactions.from_category != temp_labels.category)
    """)
    print(f"Updated {from_count:,} rows with sender labels")

    print("\nUpdating receiver labels...")
    to_count = con.execute("""
        SELECT COUNT(*)
        FROM transactions
        JOIN temp_labels ON LOWER(transactions.receiver) = temp_labels.address
        WHERE transactions.to_label IS NULL
           OR transactions.to_category IS NULL
           OR transactions.to_category = 'Uncategorized'
           OR transactions.to_label != temp_labels.label
           OR transactions.to_category != temp_labels.category
    """).fetchone()[0]

    con.execute("""
        UPDATE transactions
        SET to_label = temp_labels.label,
            to_category = temp_labels.category
        FROM temp_labels
        WHERE LOWER(transactions.receiver) = temp_labels.address
          AND (transactions.to_label IS NULL
               OR transactions.to_category IS NULL
               OR transactions.to_category = 'Uncategorized'
               OR transactions.to_label != temp_labels.label
               OR transactions.to_category != temp_labels.category)
    """)
    print(f"Updated {to_count:,} rows with receiver labels")

    con.execute("DROP TABLE temp_labels")
    con.close()

def print_summary(db_path):
    """Print summary statistics."""
    con = duckdb.connect(db_path, read_only=True)

    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    # Overall stats
    result = con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(from_label) as from_labeled,
            COUNT(to_label) as to_labeled
        FROM transactions
    """).fetchone()

    total, from_labeled, to_labeled = result

    print(f"\nTotal transactions: {total:,}")
    print(f"  From addresses labeled: {from_labeled:,} ({from_labeled/total*100:.1f}%)")
    print(f"  To addresses labeled: {to_labeled:,} ({to_labeled/total*100:.1f}%)")

    # Category breakdown for from_address
    print("\nTop from_address categories:")
    result = con.execute("""
        SELECT
            from_category,
            COUNT(*) as count
        FROM transactions
        WHERE from_category IS NOT NULL
        GROUP BY from_category
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    for category, count in result:
        pct = (count / from_labeled * 100) if from_labeled > 0 else 0
        print(f"  {category}: {count:,} ({pct:.1f}%)")

    # Category breakdown for to_address
    print("\nTop to_address categories:")
    result = con.execute("""
        SELECT
            to_category,
            COUNT(*) as count
        FROM transactions
        WHERE to_category IS NOT NULL
        GROUP BY to_category
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    for category, count in result:
        pct = (count / to_labeled * 100) if to_labeled > 0 else 0
        print(f"  {category}: {count:,} ({pct:.1f}%)")

    # Top labeled addresses (receivers)
    print("\nTop 10 labeled receivers by transaction count:")
    result = con.execute("""
        SELECT
            receiver,
            to_label,
            to_category,
            COUNT(*) as tx_count
        FROM transactions
        WHERE to_label IS NOT NULL
        GROUP BY receiver, to_label, to_category
        ORDER BY tx_count DESC
        LIMIT 10
    """).fetchall()

    for receiver_addr, label, category, tx_count in result:
        print(f"  {receiver_addr}: {label} ({category}) - {tx_count:,} txs")

    con.close()
    print("="*80)

def main():
    parser = argparse.ArgumentParser(
        description="Add label and category information to tx_metadata table"
    )

    parser.add_argument("--db", required=True,
                       help="Path to DuckDB database")
    parser.add_argument("--labels-csv", required=True,
                       help="Path to labels CSV file")

    args = parser.parse_args()

    print("="*80)
    print("ADD LABELS TO TX_METADATA TABLE")
    print("="*80)
    print(f"Database: {args.db}")
    print(f"Labels CSV: {args.labels_csv}")
    print("="*80)
    print()

    # Check if files exist
    if not os.path.exists(args.db):
        print(f"Error: Database not found: {args.db}")
        return 1

    if not os.path.exists(args.labels_csv):
        print(f"Error: Labels CSV not found: {args.labels_csv}")
        return 1

    # Add columns if needed
    print("Step 1: Adding label columns to database...")
    add_label_columns(args.db)
    print()

    # Update database
    print("Step 2: Updating database with labels...")
    update_labels_in_db(args.db, args.labels_csv)
    print()

    # Print summary
    print_summary(args.db)

    return 0

if __name__ == "__main__":
    exit(main())
