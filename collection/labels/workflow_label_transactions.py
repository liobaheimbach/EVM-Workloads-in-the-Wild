#!/usr/bin/env python3
"""
Complete workflow for labeling blockchain transactions (Ethereum or Base).

Usage:
    python workflow_label_transactions.py --chain ethereum --db /path/to/tx_metadata_ethereum.duckdb
    python workflow_label_transactions.py --chain base --db /path/to/tx_metadata_base.duckdb
"""

import argparse
import sys
import pandas as pd
import json
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.shared import (
    run_command,
    convert_kleros_df_to_label_rows,
    label_simple_transfers,
    label_contract_creations,
)

LABELS_DIR = Path(__file__).parent

def step1_fetch_spellbook_defillama(chain):
    """Step 1: Fetch labels from Spellbook and DefiLlama (chain-agnostic, includes all chains)."""
    print(f"\n{'='*80}")
    print(f"STEP 1: Spellbook and DefiLlama labels (all chains)")
    print(f"{'='*80}")

    # Use shared CSV for all chains (Spellbook/DefiLlama are chain-agnostic)
    shared_csv = LABELS_DIR / "addresses_with_categories_all_chains.csv"
    chain_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    # Check if chain-specific CSV already exists
    if chain_csv.exists():
        df = pd.read_csv(chain_csv)
        spellbook_count = len(df[df['Source'] == 'Spellbook'])
        defillama_count = len(df[df['Source'] == 'DefiLlama'])
        print(f"Found existing chain-specific CSV: {chain_csv}")
        print(f"  Spellbook labels: {spellbook_count:,}")
        print(f"  DefiLlama labels: {defillama_count:,}")
        print(f"  Total labels: {len(df):,}")
        print("  Skipping (file already exists)")
        return True

    # Check if shared CSV exists
    if not shared_csv.exists():
        print("No shared labels CSV found - creating from Spellbook and DefiLlama...")

        # Step 1a: Run Spellbook scraper
        print("\n  Running Spellbook scraper...")
        if not run_command(
            ["python", str(LABELS_DIR / "spellbook_scraper.py")],
            "Fetch Spellbook labels"
        ):
            print("  WARNING: Spellbook scraper failed")
            return False

        # Step 1b: Run DefiLlama scraper
        print("\n  Running DefiLlama scraper...")
        if not run_command(
            ["python", str(LABELS_DIR / "defi_llama_scraper.py")],
            "Fetch DefiLlama labels"
        ):
            print("  WARNING: DefiLlama scraper failed")
            return False

        # Step 1c: Combine Spellbook and DefiLlama
        print("\n  Combining Spellbook and DefiLlama labels...")
        if not run_command(
            ["python", str(LABELS_DIR / "combine_defillama_spellbook.py")],
            "Combine labels"
        ):
            print("  WARNING: Combine script failed")
            return False

        # Rename combined_addresses.csv to shared CSV
        combined_csv = LABELS_DIR / "combined_addresses.csv"
        if combined_csv.exists():
            import shutil
            shutil.move(str(combined_csv), str(shared_csv))
            print(f"\nCreated shared CSV: {shared_csv}")
        else:
            print("  WARNING: combine_defillama_spellbook.py did not produce combined_addresses.csv")
            return False

    # Copy shared CSV to chain-specific CSV (all chains included)
    print(f"\nUsing shared Spellbook/DefiLlama labels for {chain}")
    df = pd.read_csv(shared_csv)
    print(f"  Total labels in shared CSV: {len(df):,}")

    # Copy to chain-specific file
    df.to_csv(chain_csv, index=False, quoting=1)
    print(f"Created chain-specific CSV: {chain_csv}")

    spellbook_count = len(df[df['Source'] == 'Spellbook'])
    defillama_count = len(df[df['Source'] == 'DefiLlama'])
    print(f"  Spellbook labels: {spellbook_count:,}")
    print(f"  DefiLlama labels: {defillama_count:,}")

    return True

def step2_fetch_and_merge_kleros(chain):
    """Step 2: Fetch Kleros Curate labels and merge into main CSV (all chains, no filtering)."""
    print(f"\n{'='*80}")
    print(f"STEP 2: Fetch and merge Kleros Curate labels (all chains)")
    print(f"{'='*80}")

    # Paths
    kleros_all = LABELS_DIR / "kleros_curate_all_chains.csv"
    main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"
    fetch_script = LABELS_DIR / "fetch_kleros_curate.py"

    # Step 1: Fetch from Envio (if not already exists)
    if kleros_all.exists():
        print(f"Kleros labels already fetched: {kleros_all}")
        df = pd.read_csv(kleros_all)
        print(f"  Found {len(df):,} addresses across all chains")
    else:
        print("Fetching Kleros labels from Envio...")
        if not fetch_script.exists():
            print(f"WARNING: Fetch script not found: {fetch_script}")
            print("  Skipping Kleros labels")
            return True

        if not run_command(
            ["python", str(fetch_script)],
            "Fetch Kleros labels from Envio",
            stream_output=True
        ):
            print("  WARNING: Failed to fetch Kleros labels")
            return False

    # Step 2: Convert to label-row format (all chains, no filtering)
    print(f"\nConverting Kleros labels (all chains)...")

    kleros_df = pd.read_csv(kleros_all)

    print(f"  Total addresses from all chains: {len(kleros_df):,}")

    kleros_formatted_df = pd.DataFrame(convert_kleros_df_to_label_rows(kleros_df))
    print(f"Converted {len(kleros_formatted_df):,} Kleros labels")

    # Show category breakdown
    print("\nKleros category breakdown:")
    for cat, count in kleros_formatted_df['Category'].value_counts().items():
        print(f"  {cat}: {count}")

    # Step 3: Merge with main CSV
    print(f"\nMerging Kleros labels into main CSV...")
    main_df = pd.read_csv(main_csv)
    existing_addresses = set(main_df['Address'].str.lower())

    # Filter out addresses that already exist
    kleros_new = kleros_formatted_df[
        ~kleros_formatted_df['Address'].str.lower().isin(existing_addresses)
    ]

    print(f"Main CSV before: {len(main_df):,}")
    print(f"New Kleros labels to add: {len(kleros_new):,}")

    if len(kleros_new) > 0:
        combined = pd.concat([main_df, kleros_new], ignore_index=True)
        combined.to_csv(main_csv, index=False, quoting=1)
        print(f"Main CSV after: {len(combined):,}")
    else:
        print("  All Kleros addresses already in main CSV")

    return True

def step3_identify_pools_and_tokens(chain, db_path, process_all=True):
    """Step 3: Identify pools and tokens for receiver addresses via RPC calls."""
    print(f"\n{'='*80}")
    print(f"STEP 3: Identify pools and tokens ({'ALL addresses' if process_all else 'top 10k'})")
    print(f"{'='*80}")

    output_csv = LABELS_DIR / f"identified_pools_tokens_{chain}.csv"

    # Check if output already exists
    if output_csv.exists():
        df = pd.read_csv(output_csv)
        print(f"Found existing pools/tokens CSV: {output_csv}")
        print(f"  Already identified: {len(df):,} addresses")
        if 'is_eoa' in df.columns:
            print(f"  EOAs: {df['is_eoa'].sum():,}")
            print(f"  Contracts: {df['is_contract'].sum():,}")
        print(f"  Skipping RPC identification (file already exists)")
        print(f"  To re-run, delete: {output_csv}")

        # Update database with EOA/Contract flags
        print(f"\n  Updating database with EOA/Contract flags...")
        update_cmd = [
            "python", str(LABELS_DIR / "add_eoa_contract_columns.py"),
            "--db", db_path,
            "--csv", str(output_csv)
        ]
        if not run_command(update_cmd, "Update database with EOA/Contract flags"):
            print("  WARNING: Database update failed")

        # Still merge labeled contracts in case it wasn't merged before
        if len(df) > 0:
            main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"
            main_df = pd.read_csv(main_csv)
            existing_addresses = set(main_df['Address'].str.lower())
            # Only merge non-EOA addresses that have labels
            labeled_df = df[df['Category'] != 'EOA']
            new_contracts = labeled_df[~labeled_df['Address'].str.lower().isin(existing_addresses)]
            if len(new_contracts) > 0:
                print(f"\n  Merging {len(new_contracts):,} new labeled contracts into main CSV...")
                combined = pd.concat([main_df, new_contracts], ignore_index=True)
                combined.to_csv(main_csv, index=False, quoting=1)
                print(f"  Main CSV updated: {len(combined):,} total addresses")
            else:
                print(f"  All identified contracts already in main CSV")
        return True

    # Check that the RPC config file for the chain exists
    rpc_config = LABELS_DIR / "rpc_config.json"
    if not rpc_config.exists():
        print(f"WARNING: RPC config not found: {rpc_config}")
        print("  Skipping pool/token identification")
        return True

    # Skip addresses already in main CSV (from Spellbook, DefiLlama, Kleros, manual labels)
    main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    cmd = [
        "python", str(LABELS_DIR / "identify_pools_and_tokens.py"),
        "--db", db_path,
        "--chain", chain,
        "--batch-size", "1",
        "--max-workers", "2",
        "--output", str(output_csv),
        "--rpc-config", str(rpc_config),
        "--min-tx-count", "100",
        "--skip-csv", str(main_csv)  # Skip addresses already labeled
    ]

    # Add limit only if not processing all
    if not process_all:
        cmd.extend(["--limit", "10000"])

    if not run_command(cmd, f"Identify EOAs, contracts, pools, and tokens", stream_output=True):
        print("  WARNING: Identification failed")
        return False

    # Update database with EOA/Contract flags
    print(f"\nUpdating database with EOA/Contract flags...")
    update_cmd = [
        "python", str(LABELS_DIR / "add_eoa_contract_columns.py"),
        "--db", db_path,
        "--csv", str(output_csv)
    ]
    if not run_command(update_cmd, "Update database with EOA/Contract flags"):
        print("  WARNING: Database update failed")
        return False

    # Merge identified contracts into main CSV (exclude EOAs)
    if output_csv.exists():
        main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"
        main_df = pd.read_csv(main_csv)
        identified_df = pd.read_csv(output_csv)

        # Only merge non-EOA addresses
        labeled_df = identified_df[identified_df['Category'] != 'EOA']

        if len(labeled_df) > 0:
            print(f"\nMerging {len(labeled_df):,} identified contracts into main CSV (excluding EOAs)...")
            existing_addresses = set(main_df['Address'].str.lower())
            new_identified = labeled_df[~labeled_df['Address'].str.lower().isin(existing_addresses)]

            if len(new_identified) > 0:
                combined = pd.concat([main_df, new_identified], ignore_index=True)
                combined.to_csv(main_csv, index=False, quoting=1)
                print(f"Added {len(new_identified):,} new identified contracts")
                print(f"Main CSV now has {len(combined):,} addresses")
            else:
                print("  All identified contracts already in main CSV")
        else:
            print("  No contracts could be identified (only EOAs found)")

    return True

def step4_fetch_etherscan_top_unlabeled(chain, db_path, limit=1000000, min_tx_count=1000, skip_existing=False):
    """Step 4: Run Etherscan for ALL unlabeled addresses with ≥min_tx_count transactions (excluding those in main CSV)."""
    print(f"\n{'='*80}")
    print(f"STEP 4: Fetch Etherscan labels for unlabeled addresses with ≥{min_tx_count} txs")
    print(f"{'='*80}")

    if skip_existing:
        print("--skip-etherscan set: skipping Etherscan fetch (using existing labels)")
        return True

    print(f"Note: Will fetch ALL addresses with ≥{min_tx_count:,} transactions")
    print(f"      Will skip addresses already in addresses_with_categories.csv")
    print(f"      (Spellbook, DefiLlama, Kleros, and previously fetched addresses)")

    output_csv = LABELS_DIR / f"etherscan_labels_{chain}.csv"

    # Check if we already have some Etherscan labels
    if output_csv.exists():
        df = pd.read_csv(output_csv)
        # Count addresses that have non-null labels (successfully fetched from Etherscan)
        existing_etherscan_count = len(df[df['label'].notna()])
        print(f"Found existing Etherscan CSV: {output_csv}")
        print(f"  Addresses already fetched: {existing_etherscan_count:,}")
        print(f"  Will resume and fetch remaining addresses")
    else:
        print(f"No existing Etherscan labels found")

    # Query database to count how many addresses meet criteria
    print(f"\nQuerying database to count unlabeled addresses with ≥{min_tx_count:,} txs...")
    import duckdb
    con = duckdb.connect(db_path, read_only=True)

    # Check if to_label column exists
    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
    has_to_label = 'to_label' in columns
    has_to_category = 'to_category' in columns

    # Build query
    where_conditions = ["receiver IS NOT NULL", "receiver != ''"]
    if has_to_label:
        where_conditions.append("to_label IS NULL")
    elif has_to_category:
        where_conditions.append("to_category IS NULL")

    where_clause = " AND ".join(where_conditions)

    count_query = f"""
        SELECT COUNT(DISTINCT receiver) as addr_count
        FROM (
            SELECT receiver, COUNT(*) as tx_count
            FROM transactions
            WHERE {where_clause}
            GROUP BY receiver
            HAVING tx_count >= {min_tx_count}
        )
    """

    actual_count = con.execute(count_query).fetchone()[0]
    con.close()

    print(f"Found {actual_count:,} addresses with ≥{min_tx_count:,} transactions that need labels")

    # Use the actual count as the limit (plus a small buffer)
    actual_limit = min(actual_count + 100, limit)
    print(f"  Setting limit to {actual_limit:,} (actual count + buffer)")

    # Always use --skip-csv to exclude addresses in main CSV
    # Add --min-tx-count to only fetch high-traffic addresses
    cmd = [
        "python", str(LABELS_DIR / "fetch_etherscan_incremental.py"),
        "--db", db_path,
        "--chain", chain,
        "--limit", str(actual_limit),
        "--min-tx-count", str(min_tx_count),  # Only addresses with ≥min_tx_count txs
        "--batch-size", "50",
        "--delay", "1.5",
        "--output", str(output_csv),
        "--skip-csv", str(LABELS_DIR / f"addresses_with_categories_{chain}.csv")
    ]

    if output_csv.exists():
        cmd.append("--resume")

    print(f"\nFetching Etherscan labels...")
    print(f"  This will fetch up to {actual_limit:,} addresses with ≥{min_tx_count:,} transactions")
    print(f"  NOT in addresses_with_categories.csv")
    print(f"  Progress saved every 50 addresses")
    return run_command(cmd, "Fetch Etherscan labels", stream_output=True)

def step5_merge_etherscan_labels(chain):
    print(f"\n{'='*80}")
    print(f"STEP 5: Merge Etherscan labels into main CSV")
    print(f"{'='*80}")

    etherscan_csv = LABELS_DIR / f"etherscan_labels_{chain}.csv"
    main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    if not etherscan_csv.exists():
        print(f"WARNING: Etherscan CSV not found: {etherscan_csv}")
        return False

    # Load both CSVs
    etherscan_df = pd.read_csv(etherscan_csv)
    main_df = pd.read_csv(main_csv)

    print(f"Etherscan labels: {len(etherscan_df):,}")
    print(f"Main CSV before: {len(main_df):,}")

    # Filter etherscan for successful labels
    etherscan_df = etherscan_df[etherscan_df['label'].notna()].copy()

    # Format to match main CSV structure
    etherscan_formatted = pd.DataFrame({
        'Type': 'Contract',
        'Application_Name': etherscan_df['label'],
        'Contract_Name': '',
        'Address': etherscan_df['address'],
        'Source': 'Etherscan',
        'Category': etherscan_df['category'],
        'Chain': chain
    })

    # Remove addresses that already exist in main CSV
    existing_addresses = set(main_df['Address'].str.lower())
    etherscan_new = etherscan_formatted[
        ~etherscan_formatted['Address'].str.lower().isin(existing_addresses)
    ]

    print(f"New Etherscan labels to add: {len(etherscan_new):,}")

    # Append to main CSV
    combined = pd.concat([main_df, etherscan_new], ignore_index=True)
    combined.to_csv(main_csv, index=False, quoting=1)  # QUOTE_ALL to handle commas in labels

    print(f"Main CSV after: {len(combined):,}")
    return True

def step6_merge_manual_labels(chain):
    """Step 6: Merge manual labels into main CSV."""
    print(f"\n{'='*80}")
    print(f"STEP 6: Merge manual labels into main CSV")
    print(f"{'='*80}")

    manual_csv = LABELS_DIR / "manual_labels.csv"
    main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    if not manual_csv.exists():
        print(f"WARNING: Manual labels CSV not found: {manual_csv}")
        print("  Skipping manual labels (this is OK)")
        return True

    # Load both CSVs
    manual_df = pd.read_csv(manual_csv)
    main_df = pd.read_csv(main_csv)

    print(f"Manual labels: {len(manual_df):,}")
    print(f"Main CSV before: {len(main_df):,}")

    # Remove addresses that already exist in main CSV (manual labels override)
    existing_addresses = set(main_df['Address'].str.lower())

    # Separate manual labels into new and updates
    manual_new = manual_df[~manual_df['Address'].str.lower().isin(existing_addresses)]
    manual_updates = manual_df[manual_df['Address'].str.lower().isin(existing_addresses)]

    print(f"New manual labels to add: {len(manual_new):,}")
    print(f"Manual labels updating existing: {len(manual_updates):,}")

    if len(manual_updates) > 0:
        # Remove existing addresses that will be updated
        main_df = main_df[~main_df['Address'].str.lower().isin(manual_updates['Address'].str.lower())]
        # Add all manual labels (both new and updates)
        combined = pd.concat([main_df, manual_df], ignore_index=True)
    elif len(manual_new) > 0:
        # Just add new labels
        combined = pd.concat([main_df, manual_new], ignore_index=True)
    else:
        print("  No manual labels to add")
        return True

    combined.to_csv(main_csv, index=False, quoting=1)  # QUOTE_ALL to handle commas in labels

    print(f"Main CSV after: {len(combined):,}")
    return True

def step7_add_mev_json_labels(chain, mev_json_paths=None):
    """Add labels from Dune MEV query result JSON files (cex-dex, atomic-mev, etc.)."""
    print(f"\n{'='*80}")
    print(f"STEP 7: Add MEV labels from JSON files")
    print(f"{'='*80}")

    if not mev_json_paths:
        print("  No --mev-json files provided, skipping")
        return True

    main_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"
    main_df = pd.read_csv(main_csv)
    existing_addresses = set(main_df['Address'].str.lower())

    print(f"Main CSV before: {len(main_df):,} addresses")

    new_addresses = []
    for json_path in mev_json_paths:
        json_path = Path(json_path)
        if not json_path.exists():
            print(f"  WARNING: Not found: {json_path}, skipping")
            continue
        source_tag = json_path.stem.replace('-', '_').title()
        try:
            with open(json_path) as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"  WARNING: {json_path.name} is empty or invalid JSON, skipping")
            continue
        if 'result' in data and 'rows' in data['result']:
            rows = data['result']['rows']
            for row in rows:
                addr = row.get('tx_to', row.get('to_addr', '')).lower()
                if addr and addr not in existing_addresses:
                    new_addresses.append({
                        'Type': 'Contract',
                        'Application_Name': 'MEV Contract',
                        'Contract_Name': '',
                        'Address': addr,
                        'Source': f'Dune-{source_tag}',
                        'Category': 'MEV',
                        'Chain': chain
                    })
                    existing_addresses.add(addr)
            print(f"Loaded {json_path.name}: {len(rows):,} addresses")

    print(f"New MEV addresses to add: {len(new_addresses):,}")

    if new_addresses:
        new_df = pd.DataFrame(new_addresses)
        combined = pd.concat([main_df, new_df], ignore_index=True)
        combined.to_csv(main_csv, index=False, quoting=1)
        print(f"Main CSV after: {len(combined):,}")

    return True

def step8_apply_categories(chain):
    print(f"\n{'='*80}")
    print(f"STEP 8: Apply categorization rules")
    print(f"{'='*80}")

    input_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"
    output_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    cmd = [
        "python", str(LABELS_DIR / "apply_categories_to_csv.py"),
        str(input_csv),
        str(output_csv)
    ]
    return run_command(cmd, "Apply categorization rules")

def step10_update_database(chain, db_path):
    print(f"\n{'='*80}")
    print(f"STEP 10: Update database with labels")
    print(f"{'='*80}")

    labels_csv = LABELS_DIR / f"addresses_with_categories_{chain}.csv"

    cmd = [
        "python", str(LABELS_DIR / "add_labels_to_tx_metadata.py"),
        "--db", db_path,
        "--labels-csv", str(labels_csv)
    ]

    return run_command(cmd, "Update database with labels")

def step9_label_simple_transfers(chain, db_path):
    """Step 9: Label 21k gas non-type-3 transactions as simple transfers."""
    print(f"\n{'='*80}")
    print(f"STEP 9: Label simple transfers (21k gas)")
    print(f"{'='*80}")
    return label_simple_transfers(db_path)

def step11_label_type3_as_l2(chain, db_path):
    print(f"\n{'='*80}")
    print(f"STEP 11: Label type 3 transactions as L2")
    print(f"{'='*80}")

    import duckdb
    con = duckdb.connect(db_path)

    # Check if tx_type column exists
    try:
        type3_count = con.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE tx_type = 3
        """).fetchone()[0]

        print(f"Type 3 transactions found: {type3_count:,}")

        if type3_count > 0:
            # Update type 3 transactions with L2 category (only if not already labeled)
            result = con.execute("""
                UPDATE transactions
                SET to_category = 'L2',
                    to_label = 'Blob Transaction (EIP-4844)'
                WHERE tx_type = 3
                  AND to_category IS NULL
            """)
            updated = result.fetchone()[0]
            print(f"Updated {updated:,} type 3 transactions with L2 category")
        else:
            print("No type 3 transactions to label")

    except Exception as e:
        print(f"WARNING: tx_type column may not exist: {e}")

    con.close()
    return True

def step12_label_contract_creations(chain, db_path):
    print(f"\n{'='*80}")
    print(f"STEP 12: Label contract creations (NULL receiver)")
    print(f"{'='*80}")
    return label_contract_creations(db_path)


def main():
    parser = argparse.ArgumentParser(description="Complete workflow for labeling transactions")
    parser.add_argument("--chain", required=True, choices=["ethereum", "base"], help="Chain to process")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--limit-addresses", action="store_true", help="Limit to top 10k addresses (default: process ALL)")
    parser.set_defaults(process_all_addresses=True)
    parser.add_argument("--etherscan-limit", type=int, default=1000000, help="Max addresses to fetch from Etherscan (default: 1000000, effectively unlimited)")
    parser.add_argument("--etherscan-min-txs", type=int, default=1000, help="Minimum transaction count for Etherscan fetching")
    parser.add_argument("--skip-etherscan", action="store_true", help="Skip Etherscan fetching (use existing)")
    parser.add_argument("--start-from", type=int, default=1, help="Start from step N (1-12)")
    parser.add_argument("--stop-at", type=int, help="Stop after step N (1-12)")
    parser.add_argument("--mev-json", nargs="*", metavar="FILE",
                        help="Paths to Dune MEV query result JSON files (e.g. cex-dex.json atomic-mev.json)")

    args = parser.parse_args()

    # Handle the new --limit-addresses flag
    if args.limit_addresses:
        args.process_all_addresses = False

    print("="*80)
    print(f"TRANSACTION LABELING WORKFLOW - {args.chain.upper()}")
    print("="*80)
    print(f"Database: {args.db}")
    print(f"Process all addresses: {args.process_all_addresses}")
    print(f"Etherscan min txs: {args.etherscan_min_txs:,}")
    print(f"Starting from step: {args.start_from}")
    print("="*80)

    steps = [
        (1, "Fetch Spellbook and DefiLlama labels", lambda: step1_fetch_spellbook_defillama(args.chain)),
        (2, "Fetch and merge Kleros labels", lambda: step2_fetch_and_merge_kleros(args.chain)),
        (3, "Identify EOAs/contracts, pools and tokens (RPC)", lambda: step3_identify_pools_and_tokens(args.chain, args.db, args.process_all_addresses)),
        (4, "Fetch Etherscan labels", lambda: step4_fetch_etherscan_top_unlabeled(args.chain, args.db, args.etherscan_limit, args.etherscan_min_txs, args.skip_etherscan)),
        (5, "Merge Etherscan labels", lambda: step5_merge_etherscan_labels(args.chain)),
        (6, "Merge manual labels", lambda: step6_merge_manual_labels(args.chain)),
        (7, "Add MEV JSON labels", lambda: step7_add_mev_json_labels(args.chain, args.mev_json)),
        (8, "Apply categorization rules", lambda: step8_apply_categories(args.chain)),
        (9, "Label simple transfers", lambda: step9_label_simple_transfers(args.chain, args.db)),
        (10, "Update database with labels", lambda: step10_update_database(args.chain, args.db)),
        (11, "Label type 3 as L2", lambda: step11_label_type3_as_l2(args.chain, args.db)),
        (12, "Label contract creations", lambda: step12_label_contract_creations(args.chain, args.db)),
    ]

    for step_num, step_name, step_func in steps:
        if step_num < args.start_from:
            continue

        if args.stop_at and step_num > args.stop_at:
            print(f"\nStopping after step {args.stop_at} as requested")
            break

        try:
            success = step_func()
            if not success:
                print(f"\nWARNING: Step {step_num} failed: {step_name}")
                response = input("Continue anyway? (y/n): ")
                if response.lower() != 'y':
                    print("Workflow stopped.")
                    return 1
        except Exception as e:
            print(f"\nERROR: Step {step_num} error: {step_name}")
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            response = input("Continue anyway? (y/n): ")
            if response.lower() != 'y':
                print("Workflow stopped.")
                return 1

    print("\n" + "="*80)
    print("WORKFLOW COMPLETE")
    print("="*80)
    return 0

if __name__ == "__main__":
    sys.exit(main())
