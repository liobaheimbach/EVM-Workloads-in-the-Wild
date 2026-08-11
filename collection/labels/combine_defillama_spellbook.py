import csv
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

# Category mapping configuration
CATEGORIES = [
    {"name": "DEX", "category_aliases": ["DEX", "Decentralized Exchange", "Dexes"]},
    {"name": "CEX", "category_aliases": ["CEX", "Centralized Exchange", "Cexes"]},
    {"name": "Bridge", "category_aliases": ["Bridge", "Bridges", "Cross Chain"]},
    {"name": "Batch_Submitter", "category_aliases": ["Batch Submitter", "L2_Batch_Submitter"]},
    {"name": "DeFi", "category_aliases": ["DeFi", "Lending", "Yield", "Staking"]},
    {"name": "MEV", "category_aliases": ["MEV", "MEV Bot"]},
]

LABEL_UNCATEGORIZED = "Uncategorized"

def load_csv_file(file_path):
    """Load addresses from a CSV file."""
    addresses = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                addresses.append(row)
        print(f"  Loaded {len(addresses)} addresses from {file_path}")
    except FileNotFoundError:
        print(f"  Warning: File not found: {file_path}")
    return addresses

def assign_category(scraped_category, application_name=""):
    """Assign a standardized category based on scraped category."""
    # Check category aliases
    for category in CATEGORIES:
        for alias in category["category_aliases"]:
            if alias.lower() in scraped_category.lower() or alias.lower() in application_name.lower():
                return category["name"]

    return LABEL_UNCATEGORIZED

def main():
    print("Combining DefiLlama and Spellbook address labels...\n")

    # Dictionary to track unique addresses (lowercase) -> best entry
    address_mapping = {}

    # Priority: Spellbook > DefiLlama (Spellbook has more detailed categorization)

    # 1. Load DefiLlama data
    print("Loading DefiLlama addresses...")
    defillama_addresses = load_csv_file(SCRIPT_DIR / 'defillama_combined.csv')

    for entry in defillama_addresses:
        address = entry.get('Address', '').lower()
        if not address or len(address) != 42:
            continue

        # Skip known bad data from DefiLlama (protocols incorrectly mapped to token addresses)
        app_name = entry.get('Application_Name', '')
        bad_labels = [
            'symbiosis-finance',
            'everclear',
            'alienx',
            'precog',
            'keel-finance'
        ]
        if app_name in bad_labels:
            print(f"  Skipping bad DefiLlama label: {app_name} for {address}")
            continue

        category = assign_category(
            entry.get('Category', ''),
            app_name
        )

        address_mapping[address] = {
            'Type': entry.get('Type', 'Contract'),
            'Application_Name': entry.get('Application_Name', ''),
            'Contract_Name': entry.get('Contract_Name', ''),
            'Address': address,
            'Source': 'DefiLlama',
            'Category': category,
            'Chain': entry.get('Chain', 'unknown')
        }

    print(f"  DefiLlama: {len(address_mapping)} unique addresses\n")

    # 2. Load Spellbook data (overwrites DefiLlama if same address)
    print("Loading Spellbook addresses...")
    spellbook_addresses = load_csv_file(SCRIPT_DIR / 'spellbook_combined.csv')

    spellbook_count = 0
    for entry in spellbook_addresses:
        address = entry.get('Address', '').lower()
        if not address or len(address) != 42:
            continue

        category = assign_category(
            entry.get('Category', ''),
            entry.get('Project', '')
        )

        # Spellbook has priority - overwrite if exists
        address_mapping[address] = {
            'Type': entry.get('Type', 'Contract'),
            'Application_Name': entry.get('Project', ''),
            'Contract_Name': entry.get('Contract', ''),
            'Address': address,
            'Source': 'Spellbook',
            'Category': category,
            'Chain': entry.get('Chain', 'unknown')
        }
        spellbook_count += 1

    print(f"  Spellbook: {spellbook_count} addresses (overwrites DefiLlama when duplicate)")
    print(f"  Total unique addresses: {len(address_mapping)}\n")

    # 3. Generate statistics
    category_counts = defaultdict(int)
    chain_counts = defaultdict(int)
    source_counts = defaultdict(int)

    for addr_info in address_mapping.values():
        category_counts[addr_info['Category']] += 1
        chain_counts[addr_info['Chain']] += 1
        source_counts[addr_info['Source']] += 1

    print("="*60)
    print("STATISTICS")
    print("="*60)

    print("\nBy Category:")
    for cat, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat}: {count}")

    print("\nBy Chain:")
    for chain, count in sorted(chain_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {chain}: {count}")

    print("\nBy Source:")
    for source, count in sorted(source_counts.items()):
        print(f"  {source}: {count}")

    print("="*60)

    # 4. Write combined output
    output_file = SCRIPT_DIR / 'combined_addresses.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['Type', 'Application_Name', 'Contract_Name', 'Address', 'Source', 'Category', 'Chain']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Sort by category, then by address
        sorted_addresses = sorted(
            address_mapping.values(),
            key=lambda x: (x['Category'], x['Address'])
        )

        for entry in sorted_addresses:
            writer.writerow(entry)

    print(f"\nCombined addresses written to: {output_file}")

    # 5. Write chain-specific files
    for chain in ['ethereum', 'base']:
        chain_file = SCRIPT_DIR / f'combined_{chain}.csv'
        chain_addresses = [
            addr for addr in address_mapping.values()
            if addr['Chain'].lower() == chain.lower()
        ]

        if chain_addresses:
            with open(chain_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(sorted(chain_addresses, key=lambda x: (x['Category'], x['Address'])))
            print(f"  {chain.capitalize()}: {len(chain_addresses)} addresses -> {chain_file}")

if __name__ == "__main__":
    main()
