#!/usr/bin/env python3
"""
Apply categorization rules to addresses_with_categories.csv
"""
import sys
from pathlib import Path
import argparse
import pandas as pd

# Ensure the script's own directory is on sys.path so same-directory imports
# work regardless of the working directory from which the script is invoked.
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from categories import rule_set

def apply_categories(input_csv, output_csv):
    """Apply categorization rules to the CSV file."""

    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"Loaded {len(df)} addresses")

    # Remove known bad labels from DefiLlama (will be relabeled by RPC token identification)
    print("\nRemoving known bad DefiLlama labels...")
    bad_defillama_labels = ['keel-finance', 'precog', 'everclear', 'alienx', 'symbiosis-finance']
    mask = (df['Source'] == 'DefiLlama') & (df['Application_Name'].isin(bad_defillama_labels))
    removed_count = mask.sum()

    if removed_count > 0:
        # Remove these rows entirely - they'll be relabeled by token identification
        df = df[~mask].reset_index(drop=True)
        print(f"Removed {removed_count} bad DefiLlama labels (will be relabeled by token identification)")
    else:
        print(f"No bad DefiLlama labels found to remove")

    # First pass: Apply Token rule to ALL addresses (high priority - fixes miscategorized tokens)
    print("\nApplying Token categorization (high priority pass)...")
    token_rule = rule_set[0]  # Token rule is first in rule_set
    token_recategorized = 0
    for idx, row in df.iterrows():
        app_name = str(row['Application_Name']).lower()
        contract_name = str(row['Contract_Name']).lower()

        # Check if this should be a token
        for term in token_rule["terms"]:
            term_lower = term.lower()
            if term_lower in app_name or term_lower in contract_name:
                if df.at[idx, 'Category'] != 'Token':
                    df.at[idx, 'Category'] = 'Token'
                    token_recategorized += 1
                break

    print(f"Recategorized {token_recategorized} addresses as Token")

    # Count addresses that need categorization (Unknown, Uncategorized, or NULL/NaN)
    needs_cat = df['Category'].isna() | (df['Category'] == 'Unknown') | (df['Category'] == 'Uncategorized')
    unknown_count = needs_cat.sum()
    print(f"\nAddresses needing categorization (Unknown, Uncategorized, or NULL): {unknown_count}")

    # Second pass: Apply other rules only to uncategorized addresses
    updated_count = 0
    for idx, row in df.iterrows():
        # Skip if already has a good category (not Unknown, Uncategorized, or NULL)
        if pd.notna(row['Category']) and row['Category'] not in ['Unknown', 'Uncategorized']:
            continue

        app_name = str(row['Application_Name']).lower()
        contract_name = str(row['Contract_Name']).lower()

        # Apply each rule (skip first rule since we already applied Token)
        for rule in rule_set[1:]:
            for term in rule["terms"]:
                term_lower = term.lower()
                if term_lower in app_name or term_lower in contract_name:
                    df.at[idx, 'Category'] = rule['category']
                    updated_count += 1
                    break

            # Break if category was set
            if pd.notna(df.at[idx, 'Category']) and df.at[idx, 'Category'] not in ['Unknown', 'Uncategorized']:
                break

    print(f"Updated {updated_count} uncategorized addresses with other categories")

    # Merge categories: Batch_Submitter -> L2, Bots -> MEV
    category_mapping = {
        'Batch_Submitter': 'L2',
        'Bots': 'MEV'
    }

    merge_count = 0
    for old_cat, new_cat in category_mapping.items():
        mask = df['Category'] == old_cat
        merge_count += mask.sum()
        df.loc[mask, 'Category'] = new_cat

    if merge_count > 0:
        print(f"Merged {merge_count} addresses into consolidated categories")

    # Save
    df.to_csv(output_csv, index=False, quoting=1)  # QUOTE_ALL to handle commas in labels
    print(f"Saved to {output_csv}")

    # Print summary
    print("\nCategory breakdown:")
    category_counts = df['Category'].value_counts()
    for category, count in category_counts.items():
        print(f"  {category}: {count}")

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply categorization rules to addresses CSV")
    parser.add_argument("input_csv", nargs="?", default="addresses_with_categories.csv",
                        help="Input CSV file path")
    parser.add_argument("output_csv", nargs="?", default="addresses_with_categories.csv",
                        help="Output CSV file path")

    args = parser.parse_args()

    df = apply_categories(args.input_csv, args.output_csv)
