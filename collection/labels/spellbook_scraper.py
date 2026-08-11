import os
import re
import csv

# Constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SPELLBOOK_ROOT = os.path.expanduser("~/libs/spellbook")

# File paths in new Spellbook structure
ADDRESS_FILES = {
    'dex_ethereum': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/dex/models/addresses/ethereum/dex_ethereum_addresses.sql"),
        'category': 'DEX',
        'chain': 'ethereum'
    },
    'dex_base': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/dex/models/addresses/base/dex_base_addresses.sql"),
        'category': 'DEX',
        'chain': 'base'
    },
    'bridges_ethereum': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/daily_spellbook/models/bridges/ethereum/bridges_ethereum_addresses.sql"),
        'category': 'Bridge',
        'chain': 'ethereum'
    },
    'bridges_base': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/daily_spellbook/models/bridges/base/bridges_base_addresses.sql"),
        'category': 'Bridge',
        'chain': 'base'
    },
    'cex_evms': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/hourly_spellbook/models/_sector/cex/addresses/chains/cex_evms_addresses.sql"),
        'category': 'CEX',
        'chain': 'multi'
    },
    'l2_batch_submitters': {
        'path': os.path.join(SPELLBOOK_ROOT, "dbt_subprojects/daily_spellbook/models/addresses/ethereum/addresses_ethereum_l2_batch_submitters.sql"),
        'category': 'L2_Batch_Submitter',
        'chain': 'ethereum'
    }
}

# Sanity check
if not os.path.isdir(SPELLBOOK_ROOT):
    raise FileNotFoundError(
        f"Spellbook directory not found: {SPELLBOOK_ROOT}\n"
        "Expected ~/libs/spellbook\n"
        "Clone it with: cd ~/libs && git clone https://github.com/duneanalytics/spellbook.git"
    )

def extract_addresses_from_sql(file_path, category, chain):
    """
    Extract addresses from SQL files in Spellbook format.
    These files typically contain SQL with VALUES clauses containing tuples.
    """
    if not os.path.exists(file_path):
        print(f"  Warning: File not found: {file_path}")
        return []

    addresses = []

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # Pattern to match tuples in SQL VALUES clauses
        # Example: ('ethereum', '0x...', 'ProjectName', 'ContractName')
        tuple_pattern = re.compile(r"\(([^)]+)\)", re.MULTILINE)
        matches = tuple_pattern.findall(content)

        for match in matches:
            # Split tuple values and clean them
            values = [v.strip().strip("'\"") for v in match.split(',')]

            # Skip if not enough values or doesn't look like an address entry
            if len(values) < 2:
                continue

            # Try to find the address (starts with 0x and is 42 chars)
            address = None
            chain_name = None
            project_name = None
            contract_name = None

            for i, val in enumerate(values):
                # Check for hex address (starts with 0x, with or without quotes)
                if val.startswith('0x') and len(val) == 42:
                    address = val.lower()
                    # Try to extract other fields based on position
                    if i > 0:
                        chain_name = values[0]
                    if i < len(values) - 1:
                        project_name = values[i + 1] if i + 1 < len(values) else ''
                    if i < len(values) - 2:
                        contract_name = values[i + 2] if i + 2 < len(values) else ''
                    break
                # Also check for numeric hex format (e.g., 0x123... without quotes)
                # After stripping quotes, these show up without 0x prefix
                if val and not val.startswith('(') and not 'date' in val.lower():
                    # Try to convert to hex if it looks like it could be an address
                    test_val = val if val.startswith('0x') else f'0x{val}'
                    if len(test_val) == 42 and all(c in '0123456789abcdefx' for c in test_val.lower()):
                        address = test_val.lower()
                        # Extract other fields
                        if i < len(values) - 1:
                            project_name = values[i + 1] if i + 1 < len(values) else ''
                        if i < len(values) - 2:
                            contract_name = values[i + 2] if i + 2 < len(values) else ''
                        break

            # If we found an address, add it
            if address:
                # Filter by chain if specified
                if chain != 'multi':
                    if chain_name and chain_name.lower() != chain.lower():
                        continue

                addresses.append({
                    'Type': 'Contract',
                    'Category': category,
                    'Project': project_name or '',
                    'Contract': contract_name or '',
                    'Address': address,
                    'Source': 'Spellbook',
                    'Chain': chain_name or chain
                })

        print(f"  Extracted {len(addresses)} addresses from {os.path.basename(file_path)}")
        return addresses

    except Exception as e:
        print(f"  Error processing {file_path}: {e}")
        return []

def write_to_csv(data, csv_file):
    """Write extracted addresses to CSV file."""
    if not data:
        print(f"No data to write to {csv_file}")
        return

    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = ['Type', 'Category', 'Project', 'Contract', 'Address', 'Source', 'Chain']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in data:
            writer.writerow(entry)

def main():
    all_addresses = []

    print(f"Extracting addresses from Spellbook at {SPELLBOOK_ROOT}...\n")

    # Process each address file
    for file_key, file_info in ADDRESS_FILES.items():
        print(f"Processing {file_key}...")
        addresses = extract_addresses_from_sql(
            file_info['path'],
            file_info['category'],
            file_info['chain']
        )
        all_addresses.extend(addresses)

    print(f"\n{'='*60}")
    print(f"Total addresses extracted: {len(all_addresses)}")

    # Count by category
    from collections import defaultdict
    by_category = defaultdict(int)
    by_chain = defaultdict(int)

    for addr in all_addresses:
        by_category[addr['Category']] += 1
        by_chain[addr['Chain']] += 1

    print(f"\nBy category:")
    for cat, count in sorted(by_category.items()):
        print(f"  {cat}: {count}")

    print(f"\nBy chain:")
    for chain, count in sorted(by_chain.items()):
        print(f"  {chain}: {count}")
    print(f"{'='*60}\n")

    # Write combined output
    output_file = os.path.join(SCRIPT_DIR, 'spellbook_combined.csv')
    write_to_csv(all_addresses, output_file)
    print(f"Results written to: {output_file}")

    # Write chain-specific files
    for chain in ['ethereum', 'base']:
        chain_addresses = [addr for addr in all_addresses if addr['Chain'].lower() == chain]
        if chain_addresses:
            chain_file = os.path.join(SCRIPT_DIR, f'spellbook_{chain}.csv')
            write_to_csv(chain_addresses, chain_file)
            print(f"  {chain} addresses written to: {chain_file}")

if __name__ == "__main__":
    main()
