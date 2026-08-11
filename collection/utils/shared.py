"""
Shared utilities for the EVM workload analysis scripts.

Provides a subprocess command runner, Kleros label categorisation and
conversion helpers, DuckDB transaction-labeling helpers, and orchestration
for the block-list collection runs.
"""
import csv
import subprocess
import sys
import pandas as pd
import duckdb
from pathlib import Path


# ---------------------------------------------------------------------------
# Workflow helpers
# ---------------------------------------------------------------------------

def run_command(cmd, description, stream_output=False):
    """Run a shell command and print status.

    If stream_output is True, output goes straight to the terminal; otherwise
    stdout/stderr are captured and printed. Returns True on a zero exit code.
    """
    print(f"\n{'='*80}")
    print(f"STEP: {description}")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")

    if stream_output:
        result = subprocess.run(cmd)
        return result.returncode == 0
    else:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"ERROR: {description} failed")
            print(result.stderr)
            return False
        print(result.stdout)
        return True


# ---------------------------------------------------------------------------
# Kleros label helpers (shared between workflow_label_transactions*.py)
# ---------------------------------------------------------------------------

KLEROS_CATEGORY_KEYWORDS = {
    'DEX': ['dex', 'swap', 'uniswap', 'pancakeswap', 'aerodrome', 'sushiswap', 'curve',
            'exchange', 'liquidity pool', 'amm', 'router', 'aggregator', '1inch'],
    'Lending': ['aave', 'compound', 'radiant', 'lending', 'borrow', 'moonwell', 'zerolend'],
    'Bridge': ['bridge', 'layer zero', 'layerzero', 'across', 'stargate', 'wormhole', 'synapse'],
    'NFT': ['nft', 'opensea', 'blur', 'marketplace', 'zora', 'seaport', 'sound.xyz'],
    'Staking': ['staking', 'stake', 'liquid staking', 'lido', 'rocket', 'validator'],
    'Gaming': ['game', 'gaming', 'play to earn', 'p2e'],
    'Social': ['social', 'lens', 'farcaster', 'galxe', 'friend.tech', 'basenames'],
    'DeFi': ['defi', 'finance', 'protocol', 'token', 'erc20', 'treasury', 'stablecoin'],
}


def categorize_kleros_protocol(project_name, contract_name, website, notes):
    """Classify a Kleros protocol entry into one of the standard categories."""
    text = ' '.join([
        str(project_name).lower() if project_name and not pd.isna(project_name) else '',
        str(contract_name).lower() if contract_name and not pd.isna(contract_name) else '',
        str(website).lower() if website and not pd.isna(website) else '',
        str(notes).lower() if notes and not pd.isna(notes) else '',
    ])
    for category, keywords in KLEROS_CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return category
    return 'Uncategorized'


def convert_kleros_df_to_label_rows(kleros_df):
    """Convert a raw Kleros DataFrame to the standard label-row format.

    Returns a list of dicts suitable for building a labels DataFrame.
    """
    rows = []
    for _, row in kleros_df.iterrows():
        category = categorize_kleros_protocol(
            row.get('project_name', ''),
            row.get('contract_name', ''),
            row.get('website', ''),
            row.get('notes', ''),
        )
        rows.append({
            'Type': 'Contract',
            'Application_Name': row.get('project_name', ''),
            'Contract_Name': row.get('contract_name', ''),
            'Address': row['address'],
            'Source': 'Kleros',
            'Category': category,
            'Chain': '',  # Leave blank — addresses are cross-chain
        })
    return rows


# ---------------------------------------------------------------------------
# DuckDB label-writing helpers (shared between workflow_label_transactions*.py)
# ---------------------------------------------------------------------------

def label_simple_transfers(db_path):
    """Label 21k-gas non-type-3 transactions as simple transfers.

    Adds the simple_transfer boolean column if absent, then marks all
    gas_used=21000 rows (excluding type-3 if tx_type exists).
    Returns True on success.
    """
    con = duckdb.connect(db_path)

    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
    if 'simple_transfer' not in columns:
        print("  Adding simple_transfer column...")
        con.execute("ALTER TABLE transactions ADD COLUMN simple_transfer BOOLEAN DEFAULT FALSE")
        print("  Added simple_transfer column")

    current = con.execute(
        "SELECT COUNT(*) FROM transactions WHERE simple_transfer = TRUE"
    ).fetchone()[0]
    print(f"Current simple transfers: {current:,}")

    has_tx_type = 'tx_type' in columns
    if has_tx_type:
        print("  Note: tx_type column found, excluding type 3 transactions")
        con.execute("""
            UPDATE transactions
            SET simple_transfer = TRUE
            WHERE gas_used = 21000
              AND (tx_type IS NULL OR tx_type != 3)
              AND simple_transfer = FALSE
        """)
    else:
        print("  Note: tx_type column not found, labeling all 21k gas transactions")
        con.execute("""
            UPDATE transactions
            SET simple_transfer = TRUE
            WHERE gas_used = 21000
              AND simple_transfer = FALSE
        """)

    updated = con.execute(
        "SELECT COUNT(*) FROM transactions WHERE simple_transfer = TRUE"
    ).fetchone()[0]
    print(f"Updated simple transfers: {updated:,} (added {updated - current:,})")

    con.close()
    return True


def label_contract_creations(db_path):
    """Label NULL-receiver transactions as contract creations.

    Requires that to_category and to_label columns already exist (created by
    the add_labels_to_tx_metadata step).  Returns True on success.
    """
    con = duckdb.connect(db_path)

    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
    has_to_category = 'to_category' in columns
    has_to_label = 'to_label' in columns

    if not has_to_category or not has_to_label:
        print("  Label columns don't exist yet — skipping contract creation labeling")
        con.close()
        return True

    current = con.execute("""
        SELECT COUNT(*)
        FROM transactions
        WHERE receiver IS NULL
          AND to_category IS NULL
    """).fetchone()[0]
    print(f"Unlabeled NULL receiver transactions: {current:,}")

    con.execute("""
        UPDATE transactions
        SET to_category = 'Contract Creation',
            to_label = 'Contract Creation'
        WHERE receiver IS NULL
          AND to_category IS NULL
    """)

    updated = con.execute("""
        SELECT COUNT(*)
        FROM transactions
        WHERE receiver IS NULL
          AND to_category = 'Contract Creation'
    """).fetchone()[0]
    print(f"Labeled contract creation transactions: {updated:,} (updated {current:,})")

    con.close()
    return True


# ---------------------------------------------------------------------------
# September-run helpers (shared between run_*_september*.py scripts)
# ---------------------------------------------------------------------------

def read_blocks_in_range(csv_file, start_block, end_block):
    """Read block numbers from a CSV (must have a 'block_number' column) and
    return those that fall within [start_block, end_block] inclusive."""
    blocks = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_num = int(row['block_number'])
            if start_block <= block_num <= end_block:
                blocks.append(block_num)
    return blocks


def run_september_collection(
    title,
    chain,
    csv_file,
    start_block,
    end_block,
    output_db,
    collection_script,
    extra_args=None,
):
    """Read September blocks from *csv_file*, validate, then invoke
    *collection_script* via subprocess with standard arguments.

    *extra_args* is an optional list of additional CLI arguments appended
    after the standard ones (e.g. ``['--lookbacks', '0,5,10,20']``).
    """
    print("=" * 80)
    print(title)
    print("=" * 80)

    print(f"Reading blocks from: {csv_file}")
    blocks = read_blocks_in_range(csv_file, start_block, end_block)

    if not blocks:
        print("ERROR: No September blocks found in CSV!")
        sys.exit(1)

    print(f"Found {len(blocks):,} blocks in September range")
    print(f"Block range: {min(blocks):,} to {max(blocks):,}")

    print("\n" + "=" * 80)
    print(f"Running {Path(collection_script).name} for September blocks...")
    print("=" * 80)

    cmd = [
        'python3',
        collection_script,
        '--block-list', str(csv_file),
        '--chain', chain,
        '--block-range', f'{start_block}-{end_block}',
        '--num-workers', '16',
        '--tx-parallelism', '4',
        '--output-file', str(output_db),
    ]
    if extra_args:
        cmd.extend(extra_args)

    print(f"Command: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, cwd=Path(collection_script).parent)
    sys.exit(result.returncode)
