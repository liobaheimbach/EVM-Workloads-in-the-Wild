#!/usr/bin/env python3
"""
Fetch Etherscan/Basescan labels incrementally — saves every N addresses so progress isn't lost.
"""

import argparse
import os
import re
import time
from pathlib import Path
from typing import Optional, Dict

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup


_DOMAIN_MAP = {
    "ethereum": "etherscan.io",
    "base": "basescan.org",
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def fetch_etherscan_label(address: str, chain: str = "ethereum") -> Optional[Dict]:
    """Scrape a contract page on Etherscan/Basescan and extract its label + category."""
    if not address.startswith("0x"):
        address = f"0x{address}"
    address = address.lower()

    domain = _DOMAIN_MAP.get(chain, "etherscan.io")
    url = f"https://{domain}/address/{address}"

    try:
        response = requests.get(url, headers=_HEADERS, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {address}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    result = {"address": address, "chain": chain, "label": None,
              "contract_name": None, "category": None, "url": url}

    title_elem = soup.find("title")
    if title_elem:
        title_text = title_elem.get_text(strip=True)
        if "|" in title_text:
            name_part = title_text.split("|")[0].strip()
            name_part = re.sub(r'^Contract\s*[:|-]?\s*', '', name_part, flags=re.IGNORECASE)
            if name_part and "Address" not in name_part:
                result["label"] = name_part

    for span in soup.find_all("span", class_=re.compile("text-break")):
        text = span.get_text(strip=True)
        if text and len(text) < 100 and "0x" not in text.lower():
            if not result["label"] or len(text) > len(result["label"]):
                result["label"] = text

    if result["label"]:
        label_lower = result["label"].lower()
        if any(x in label_lower for x in ["router", "swap", "dex", "exchange", "uniswap", "1inch", "pancake"]):
            result["category"] = "DEX"
        elif any(x in label_lower for x in ["bridge", "portal"]):
            result["category"] = "Bridge"
        elif any(x in label_lower for x in ["lending", "aave", "compound", "borrow"]):
            result["category"] = "Lending"
        elif any(x in label_lower for x in ["token", "usdt", "usdc", "dai", "weth"]):
            result["category"] = "Token"
        elif any(x in label_lower for x in ["staking", "validator"]):
            result["category"] = "Staking"
        elif any(x in label_lower for x in ["nft", "erc721", "erc1155"]):
            result["category"] = "NFT"
        elif any(x in label_lower for x in ["mev", "flashbot"]):
            result["category"] = "MEV"

    return result

def get_top_unlabeled_addresses(db_path, limit=100, existing_csv=None, min_tx_count=0, exclude_addresses=None):
    """Get top unlabeled to_addresses by transaction count, excluding those in existing CSV
    and in exclude_addresses (e.g. addresses already fetched into the output CSV)."""
    min_tx_filter = f" with >{min_tx_count:,} txs" if min_tx_count > 0 else ""
    print(f"Querying database for top {limit} unlabeled addresses{min_tx_filter}...")

    # Addresses to skip (lowercased): skip CSV + already-fetched addresses
    skip_addresses = {addr.lower() for addr in (exclude_addresses or set())}
    if existing_csv and os.path.exists(existing_csv):
        print(f"Loading existing addresses from {existing_csv}...")
        existing_df = pd.read_csv(existing_csv)
        if 'Address' in existing_df.columns:
            skip_addresses.update(existing_df['Address'].str.lower())
        if 'address' in existing_df.columns:
            skip_addresses.update(existing_df['address'].str.lower())
    if skip_addresses:
        print(f"Skipping {len(skip_addresses):,} already-known addresses")

    con = duckdb.connect(db_path, read_only=True)

    # Check if to_label column exists
    columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
    has_to_label = 'to_label' in columns
    has_to_category = 'to_category' in columns

    # Build query with optional filters
    where_conditions = [
        "receiver IS NOT NULL",
        "receiver != ''"
    ]

    if has_to_label:
        where_conditions.append("to_label IS NULL")
    elif has_to_category:
        where_conditions.append("to_category IS NULL")

    where_clause = " AND ".join(where_conditions)
    having_clause = f"HAVING COUNT(*) >= {min_tx_count}" if min_tx_count > 0 else ""

    # Page through candidates until `limit` NEW addresses are collected
    # or the candidates are exhausted
    page_size = max(limit, 1000)
    offset = 0
    selected = []
    while len(selected) < limit:
        query = f"""
            SELECT
                receiver,
                COUNT(*) as tx_count
            FROM transactions
            WHERE {where_clause}
            GROUP BY receiver
            {having_clause}
            ORDER BY tx_count DESC, receiver
            LIMIT {page_size}
            OFFSET {offset}
        """
        page = con.execute(query).fetchall()
        if not page:
            break
        for addr, count in page:
            if addr.lower() in skip_addresses:
                continue
            skip_addresses.add(addr.lower())
            selected.append((addr, count))
            if len(selected) >= limit:
                break
        offset += page_size

    con.close()

    if len(selected) < limit:
        print(f"WARNING: candidates exhausted — only found {len(selected)} of {limit} requested new addresses")

    return selected

def main():
    parser = argparse.ArgumentParser(description="Fetch Etherscan labels incrementally")
    parser.add_argument("--db", required=True, help="Path to tx_metadata DuckDB database")
    parser.add_argument("--chain", default="ethereum", help="Chain name (ethereum, base, etc.)")
    parser.add_argument("--limit", type=int, default=1000, help="Total number of addresses to fetch")
    parser.add_argument("--min-tx-count", type=int, default=0, help="Minimum transaction count (only fetch addresses with >N transactions)")
    parser.add_argument("--batch-size", type=int, default=50, help="Save every N addresses")
    parser.add_argument("--output", help="Output CSV file (default: etherscan_labels_incremental_CHAIN.csv)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests (seconds)")
    parser.add_argument("--resume", action="store_true", help="Resume from existing output file")
    parser.add_argument("--skip-csv", help="CSV file with addresses to skip (e.g., addresses_with_categories.csv)")

    args = parser.parse_args()

    if not args.output:
        args.output = f"etherscan_labels_incremental_{args.chain}.csv"

    print("="*80)
    print(f"FETCH ETHERSCAN LABELS INCREMENTALLY")
    print("="*80)
    print(f"Database: {args.db}")
    print(f"Chain: {args.chain}")
    print(f"Output: {args.output}")
    print(f"Total limit: {args.limit}")
    if args.min_tx_count > 0:
        print(f"Min tx count: >{args.min_tx_count:,} (only high-traffic addresses)")
    print(f"Batch size: {args.batch_size} (saves every {args.batch_size} addresses)")
    print(f"Delay: {args.delay}s between requests")
    print("="*80)
    print()

    # Check if resuming
    start_idx = 0
    existing_results = []
    fetched_addresses = set()
    if args.resume and os.path.exists(args.output):
        print(f"Resuming from existing file: {args.output}")
        existing_df = pd.read_csv(args.output)
        existing_results = existing_df.to_dict('records')
        if 'address' in existing_df.columns:
            fetched_addresses = set(existing_df['address'].astype(str).str.lower())
        start_idx = len(existing_results)
        print(f"Found {start_idx} existing results, will continue from there")
        print()

    # Get addresses to fetch
    remaining = args.limit - start_idx
    if remaining <= 0:
        print("Already fetched all requested addresses!")
        return

    addresses = get_top_unlabeled_addresses(args.db, limit=remaining, existing_csv=args.skip_csv,
                                            min_tx_count=args.min_tx_count,
                                            exclude_addresses=fetched_addresses)

    print(f"Found {len(addresses)} unlabeled addresses to fetch")
    print()

    if not addresses:
        print("No unlabeled addresses found!")
        return

    print(f"Top 10 addresses to fetch:")
    for i, (addr, count) in enumerate(addresses[:10]):
        print(f"  {i+1}. {addr}: {count:,} txs")
    print()

    # Fetch labels with incremental saves
    print(f"Fetching labels from Etherscan...")
    print(f"Will save to {args.output} every {args.batch_size} addresses")
    print("-"*80)

    results = existing_results.copy()
    start_time = time.time()
    batch_start = time.time()

    for i, (addr, tx_count) in enumerate(addresses, 1):
        absolute_idx = start_idx + i

        # Fetch label
        label_info = fetch_etherscan_label(addr, args.chain)
        if label_info:
            label_info['tx_count'] = tx_count

            # Filter out ALL .eth ENS names (personal wallets)
            # Legitimate contracts with .eth names should be added manually to the CSV
            label = label_info.get('label', '')
            if label and label.endswith('.eth'):
                print(f"  Skipping ENS name: {label}")
                # Record the attempt with an empty label so resume offsets stay correct
                label_info['label'] = None
                label_info['category'] = None

            label_preview = label_info.get('label') or 'No Label'
            category_preview = label_info.get('category') or 'Unknown'
        else:
            label_preview = "ERROR"
            category_preview = "ERROR"
            # Record failed fetches too so resume offsets stay correct
            label_info = {"address": addr, "chain": args.chain, "label": None,
                          "contract_name": None, "category": None, "url": None,
                          "tx_count": tx_count}

        results.append(label_info)

        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        remaining_count = len(addresses) - i
        eta = remaining_count / rate if rate > 0 else 0

        print(f"[{absolute_idx:4d}/{args.limit}] {absolute_idx/args.limit*100:5.1f}% | "
              f"{rate:.2f} addr/s | ETA: {eta/60:.1f}m | "
              f"{addr[:10]}... → {label_preview[:40]} ({category_preview})")

        # Save every batch_size addresses
        if i % args.batch_size == 0 or i == len(addresses):
            df = pd.DataFrame(results)
            df.to_csv(args.output, index=False, quoting=1)  # QUOTE_ALL to handle commas in labels
            batch_time = time.time() - batch_start
            print(f"  Saved {len(results)} results to {args.output} (batch took {batch_time:.1f}s)")
            batch_start = time.time()

        # Rate limiting
        if i < len(addresses):
            time.sleep(args.delay)

    print("-"*80)

    total_time = time.time() - start_time

    # Flush any results not yet saved by the periodic batch save
    df = pd.DataFrame(results)
    df.to_csv(args.output, index=False, quoting=1)

    print(f"\n{'='*80}")
    print(f"Completed in {total_time/60:.1f} minutes ({total_time:.0f}s)")
    print(f"Final save: {len(results)} results to {args.output}")

    # Summary
    print(f"\nSummary:")
    print(f"  Total addresses queried: {len(results)}")
    if len(results) > 0:
        labeled_count = df['label'].notna().sum()
        print(f"  With labels: {labeled_count} ({labeled_count/len(results)*100:.1f}%)")

        # Category breakdown
        if labeled_count > 0:
            category_counts = df['category'].value_counts()
            print(f"\nCategories found:")
            for category, count in category_counts.head(10).items():
                print(f"  {category or 'Unknown'}: {count}")

    print("="*80)

if __name__ == "__main__":
    main()
