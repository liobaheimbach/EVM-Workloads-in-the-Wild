import os
import csv
import re
from collections import defaultdict

# Constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFILLAMA_ROOT = os.path.expanduser("~/libs/defillama")
PROJECTS_DIR = os.path.join(DEFILLAMA_ROOT, "projects")

# Chains we care about
CHAINS = {"ethereum", "base"}

# Sanity check
if not os.path.isdir(PROJECTS_DIR):
    raise FileNotFoundError(
        f"DefiLlama projects dir not found: {PROJECTS_DIR}\n"
        "Expected ~/libs/defillama/projects"
    )

def extract_addresses_from_file(file_path):
    """
    Extract Ethereum addresses (0x...) from JavaScript files.
    Returns list of dicts with address info.
    """
    addresses = []

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Pattern to match Ethereum addresses (0x followed by 40 hex chars)
        address_pattern = re.compile(r"'(0x[a-fA-F0-9]{40})'|\"(0x[a-fA-F0-9]{40})\"")

        # Find all addresses
        matches = address_pattern.findall(content)
        for match in matches:
            # match is a tuple, get the non-empty group
            address = match[0] or match[1]
            if address:
                addresses.append(address.lower())  # Normalize to lowercase

        # Also check for chain-specific blocks (ethereum:, base:)
        chain_addresses = defaultdict(list)
        for chain in CHAINS:
            # Look for patterns like: ethereum: { ... }
            # The lookbehind keeps e.g. "base" from matching inside "coinbase:".
            # Known limitation: [^}]+ stops at the first '}', so nested-brace
            # blocks are truncated and only their leading portion is scanned.
            chain_pattern = re.compile(
                rf'(?<![\w]){chain}\s*:\s*{{([^}}]+)}}',
                re.DOTALL | re.IGNORECASE
            )
            chain_matches = chain_pattern.finditer(content)
            for block_match in chain_matches:
                block_content = block_match.group(1)
                # Extract addresses from this block
                block_addresses = address_pattern.findall(block_content)
                for addr_match in block_addresses:
                    address = addr_match[0] or addr_match[1]
                    if address:
                        chain_addresses[chain].append(address.lower())

        return list(set(addresses)), chain_addresses  # Remove duplicates

    except Exception as e:
        print(f"Warning: failed to parse {file_path}: {e}", flush=True)
        return [], {}

def write_to_csv(data, csv_file):
    """Write extracted addresses to CSV file."""
    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = ['Type', 'Application_Name', 'Contract_Name', 'Address', 'Source', 'Category', 'Chain']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for entry in data:
            writer.writerow(entry)

def main():
    all_addresses = defaultdict(set)  # chain -> set of addresses
    project_data = []

    print(f"Scanning projects in {PROJECTS_DIR}...")
    processed_count = 0

    for project_name in sorted(os.listdir(PROJECTS_DIR)):
        project_path = os.path.join(PROJECTS_DIR, project_name)

        # Skip if not a directory
        if not os.path.isdir(project_path):
            continue

        project_addresses = set()
        project_chain_addresses = defaultdict(set)

        # Look for JavaScript files that might contain contract addresses
        for config_file in ['config.js', 'index.js', 'api.js', 'treasury.js']:
            file_path = os.path.join(project_path, config_file)
            if os.path.isfile(file_path):
                addresses, chain_addresses = extract_addresses_from_file(file_path)
                project_addresses.update(addresses)

                for chain, addrs in chain_addresses.items():
                    project_chain_addresses[chain].update(addrs)

        # If we found addresses, add to results
        if project_addresses or project_chain_addresses:
            processed_count += 1

            # Add chain-specific addresses
            for chain, addresses in project_chain_addresses.items():
                for addr in addresses:
                    all_addresses[chain].add(addr)
                    project_data.append({
                        'Type': 'Contract',
                        'Application_Name': project_name,
                        'Contract_Name': '',
                        'Address': addr,
                        'Source': 'DefiLlama',
                        'Category': 'DeFi',
                        'Chain': chain
                    })

            # Add general addresses (mark as 'unknown' if not chain-specific)
            for addr in project_addresses:
                # Skip if already added as chain-specific
                if any(addr in addrs for addrs in project_chain_addresses.values()):
                    continue
                all_addresses['unknown'].add(addr)
                project_data.append({
                    'Type': 'Contract',
                    'Application_Name': project_name,
                    'Contract_Name': '',
                    'Address': addr,
                    'Source': 'DefiLlama',
                    'Category': 'DeFi',
                    'Chain': 'unknown'
                })

            if processed_count % 100 == 0:
                print(f"  Processed {processed_count} projects...")

    print(f"\n{'='*60}")
    print(f"Total projects processed: {processed_count}")
    print(f"\nAddresses by chain:")
    for chain in sorted(all_addresses.keys()):
        print(f"  {chain}: {len(all_addresses[chain])} unique addresses")
    print(f"\nTotal entries: {len(project_data)}")
    print(f"{'='*60}\n")

    # Write all results to CSV
    csv_file_path = os.path.join(SCRIPT_DIR, 'defillama_combined.csv')
    write_to_csv(project_data, csv_file_path)
    print(f"Results written to: {csv_file_path}")

    # Also write chain-specific files
    for chain in CHAINS:
        if chain in all_addresses and all_addresses[chain]:
            chain_data = [entry for entry in project_data if entry['Chain'] == chain]
            chain_file = os.path.join(SCRIPT_DIR, f'defillama_{chain}.csv')
            write_to_csv(chain_data, chain_file)
            print(f"  {chain} addresses written to: {chain_file}")

if __name__ == "__main__":
    main()
