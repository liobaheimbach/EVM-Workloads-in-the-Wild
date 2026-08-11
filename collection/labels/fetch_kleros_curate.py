#!/usr/bin/env python3
"""
Fetch Kleros Curate address labels from the Envio indexer.

Address format is CAIP-2 (eip155:chainId:0xaddress). Only "Registered" items are returned.
"""
import requests
import csv
import re

ENVIO_ENDPOINT = "https://indexer.hyperindex.xyz/1a2f51c/v1/graphql"
TAG_REGISTRY = "0x66260c69d03837016d88c9877e61e08ef74c59f2"


def fetch_all_registered_items(registry_address=TAG_REGISTRY, limit=1000):
    """
    Fetch all registered (accepted) items from Kleros Curate via Envio.

    Args:
        registry_address: Registry contract address
        limit: Number of items to fetch per query
    """
    all_items = []
    offset = 0

    print(f"Querying Envio endpoint: {ENVIO_ENDPOINT}")
    print(f"Registry: {registry_address}")
    print(f"Fetching registered items...")

    while True:
        # Envio uses Hasura-style queries
        query = """
        query GetRegisteredItems($limit: Int!, $offset: Int!, $registry: String!) {
          LItem(
            limit: $limit
            offset: $offset
            where: {
              status: {_eq: "Registered"}
              registryAddress: {_eq: $registry}
            }
            order_by: {id: asc}
          ) {
            id
            status
            key0
            key1
            key2
            key3
            key4
            registryAddress
          }
        }
        """

        variables = {
            "limit": limit,
            "offset": offset,
            "registry": registry_address.lower()
        }

        try:
            response = requests.post(
                ENVIO_ENDPOINT,
                json={"query": query, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code != 200:
                print(f"Error: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                break

            data = response.json()

            if "errors" in data:
                print(f"GraphQL errors: {data['errors']}")
                break

            items = data.get("data", {}).get("LItem", [])

            if not items:
                print(f"No more items found at offset {offset}")
                break

            all_items.extend(items)
            print(f"  Fetched {len(items)} items (total: {len(all_items)})")

            # If we got fewer items than limit, we've reached the end
            if len(items) < limit:
                break

            offset += limit

        except Exception as e:
            print(f"Error fetching data: {e}")
            break

    print(f"\nTotal items fetched: {len(all_items)}")
    return all_items


def parse_caip2_address(caip2_str):
    """
    Parse CAIP-2 format address: eip155:chainId:0xaddress

    Returns: (chain_id, address) or (None, None) if invalid
    """
    if not caip2_str:
        return None, None

    # Match eip155:chainId:0xaddress
    match = re.match(r'eip155:(\d+):(0x[a-fA-F0-9]{40})', caip2_str)
    if match:
        return int(match.group(1)), match.group(2).lower()

    return None, None


def parse_item_metadata(item):
    """
    Parse Kleros Curate item metadata.

    The keys are:
    key0: CAIP-2 address (eip155:chainId:0xaddress)
    key1: project name
    key2: contract name
    key3: website
    key4: notes (optional)
    """
    chain_id, address = parse_caip2_address(item.get("key0", ""))

    return {
        "id": item.get("id"),
        "status": item.get("status"),
        "chain_id": chain_id,
        "address": address,
        "project_name": item.get("key1", ""),
        "contract_name": item.get("key2", ""),
        "website": item.get("key3", ""),
        "notes": item.get("key4", ""),
        "registry": item.get("registryAddress")
    }


def save_to_csv(items, output_path, chain_filter=None):
    """Save parsed items to CSV."""
    if not items:
        print("No items to save")
        return []

    # Parse and optionally filter by chain
    parsed_items = [parse_item_metadata(item) for item in items]

    # Filter out items without valid addresses
    parsed_items = [item for item in parsed_items if item["address"]]

    if chain_filter:
        parsed_items = [item for item in parsed_items if item["chain_id"] == chain_filter]
        print(f"Filtered to {len(parsed_items)} items for chain {chain_filter}")

    if not parsed_items:
        print("No items after filtering")
        return []

    fieldnames = ["address", "project_name", "contract_name", "website", "notes", "chain_id", "registry", "id", "status"]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(parsed_items)

    print(f"Saved {len(parsed_items)} items to {output_path}")
    return parsed_items


def main():
    print("="*80)
    print("KLEROS CURATE LABEL FETCHER (ENVIO)")
    print("="*80)
    print()

    # Fetch all items from the main address tag registry
    all_items = fetch_all_registered_items(TAG_REGISTRY)

    if not all_items:
        print("No items fetched")
        return

    # Save all items to labels directory
    from pathlib import Path
    labels_dir = Path(__file__).parent
    output_path = labels_dir / "kleros_curate_all_chains.csv"

    all_parsed = save_to_csv(all_items, str(output_path))

    # Print statistics by chain
    print("\n" + "="*80)
    print("STATISTICS BY CHAIN")
    print("="*80)

    chain_names = {
        1: "Ethereum",
        8453: "Base",
    }

    chain_counts = {}
    for item in all_parsed:
        chain_id = item["chain_id"]
        if chain_id:
            chain_counts[chain_id] = chain_counts.get(chain_id, 0) + 1

    for chain_id, count in sorted(chain_counts.items()):
        chain_name = chain_names.get(chain_id, f"Chain {chain_id}")
        print(f"{chain_name} ({chain_id}): {count:,} addresses")

    print("="*80)


if __name__ == "__main__":
    main()
