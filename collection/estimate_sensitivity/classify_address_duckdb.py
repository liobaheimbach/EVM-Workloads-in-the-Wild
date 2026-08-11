#!/usr/bin/env python3
"""
Classify addresses from DuckDB as EOA (Externally Owned Account) or Contract.

Reads unique to_address values from transactions table and writes classification
to a separate address_classification table in the same database.
"""

import duckdb
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple
import time
import json
from pathlib import Path
import argparse

# Session pool for connection reuse (per-process)
_session_pool = {}

def get_session():
    """Get or create a requests.Session for the current process"""
    import os
    pid = os.getpid()
    if pid not in _session_pool:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _session_pool[pid] = session
    return _session_pool[pid]

def load_rpc_config() -> dict:
    """Load RPC configuration from rpc_config.json"""
    config_path = Path(__file__).parent / 'rpc_config.json'
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"rpc_config.json not found at {config_path}")

def get_rpc_url(chain: str) -> str:
    """Get RPC URL for chain from rpc_config.json"""
    config = load_rpc_config()
    if chain not in config:
        raise ValueError(f"Chain '{chain}' not found in rpc_config.json")
    rpc_url = config[chain].get('rpc_url')
    if not rpc_url:
        raise ValueError(f"No rpc_url configured for chain '{chain}'")
    return rpc_url

def check_address_type(address: str, rpc_url: str) -> Tuple[str, str]:
    """
    Check if an address is an EOA or contract.

    Args:
        address: Ethereum address (with or without 0x prefix)
        rpc_url: RPC endpoint URL

    Returns:
        Tuple of (address, type) where type is 'EOA', 'Contract', 'Unknown', or 'error'
    """
    session = get_session()

    if not address.startswith('0x'):
        address = '0x' + address

    # Use eth_getCode to check if address has bytecode
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getCode",
        "params": [address, "latest"],
        "id": 1
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=(30, 120))
        result = response.json()

        if "error" in result:
            error_msg = result["error"].get("message", "Unknown error")
            print(f"Error checking {address}: {error_msg}")
            return (address, 'error')

        if "result" not in result:
            return (address, 'Unknown')

        code = result["result"]

        # eth_getCode returns "0x" for EOAs; anything else is bytecode
        if code == "0x" or not code:
            return (address, 'EOA')
        else:
            return (address, 'Contract')

    except Exception as e:
        print(f"Error checking {address}: {e}")
        return (address, 'error')


def classify_addresses_in_duckdb(
    db_path: str,
    chain: str = 'base',
    rpc_url: str = None,
    num_workers: int = 50,
    batch_size: int = 1000
):
    """
    Read unique to_address values from transactions table and classify them.
    Write results to address_classification table in the same database.
    Only processes addresses that haven't been classified yet.

    Args:
        db_path: Path to DuckDB database
        chain: Chain name (for RPC config)
        rpc_url: Custom RPC URL (optional)
        num_workers: Number of parallel workers
        batch_size: Commit results every N addresses
    """
    if rpc_url is None:
        rpc_url = get_rpc_url(chain)

    print("="*80)
    print("ADDRESS CLASSIFICATION")
    print("="*80)
    print(f"Database: {db_path}")
    print(f"Chain: {chain}")
    print(f"RPC: {rpc_url}")
    print(f"Workers: {num_workers}")
    print(f"Batch size: {batch_size}")
    print("="*80)
    print()

    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS address_classification (
            address VARCHAR PRIMARY KEY,
            type VARCHAR,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("address_classification table created/verified")

    # (rows with type='error' are excluded from the join so they get retried)
    print("\nFetching unique addresses from transactions table...")
    result = conn.execute("""
        SELECT DISTINCT t.to_address
        FROM transactions t
        LEFT JOIN address_classification ac
          ON LOWER(CASE WHEN starts_with(t.to_address, '0x') THEN t.to_address ELSE '0x' || t.to_address END) = LOWER(ac.address)
          AND ac.type != 'error'
        WHERE t.to_address IS NOT NULL
          AND t.to_address != ''
          AND ac.address IS NULL
        ORDER BY t.to_address
    """).fetchall()

    addresses_to_process = [row[0] for row in result]

    if not addresses_to_process:
        print("All addresses already classified!")

        summary = conn.execute("""
            SELECT type, COUNT(*) as count
            FROM address_classification
            GROUP BY type
            ORDER BY count DESC
        """).fetchall()

        print("\nClassification Summary:")
        for addr_type, count in summary:
            print(f"  {addr_type}: {count:,}")

        conn.close()
        return

    print(f"Found {len(addresses_to_process):,} unique addresses to classify")
    print()

    start_time = time.time()
    processed_count = 0
    results_buffer = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_addr = {
            executor.submit(check_address_type, addr, rpc_url): addr
            for addr in addresses_to_process
        }

        for i, future in enumerate(as_completed(future_to_addr), 1):
            address, addr_type = future.result()
            results_buffer.append((address, addr_type))
            processed_count += 1

            if i % 100 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                remaining = len(addresses_to_process) - i
                eta_seconds = remaining / rate if rate > 0 else 0
                print(f"[{i:,}/{len(addresses_to_process):,}] "
                      f"Rate: {rate:.1f} addr/s, "
                      f"ETA: {eta_seconds/60:.1f} min")

            if len(results_buffer) >= batch_size:
                conn.executemany(
                    "INSERT OR REPLACE INTO address_classification (address, type) VALUES (?, ?)",
                    results_buffer
                )
                conn.commit()
                results_buffer = []

    if results_buffer:
        conn.executemany(
            "INSERT OR REPLACE INTO address_classification (address, type) VALUES (?, ?)",
            results_buffer
        )
        conn.commit()

    elapsed = time.time() - start_time

    summary = conn.execute("""
        SELECT type, COUNT(*) as count
        FROM address_classification
        GROUP BY type
        ORDER BY count DESC
    """).fetchall()

    conn.close()

    print()
    print("="*80)
    print("COMPLETED")
    print("="*80)
    print(f"Total processed: {processed_count:,} addresses")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Average rate: {processed_count/elapsed:.1f} addresses/second")
    print()
    print("Classification Summary:")
    for addr_type, count in summary:
        print(f"  {addr_type}: {count:,}")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="Classify addresses in DuckDB as EOA or Contract"
    )

    parser.add_argument("--db", required=True,
                       help="Path to DuckDB database")
    parser.add_argument("--chain", default="base",
                       help="Chain name for RPC config (default: base)")
    parser.add_argument("--rpc-url", default=None,
                       help="Custom RPC URL (overrides chain config)")
    parser.add_argument("--num-workers", type=int, default=50,
                       help="Number of parallel workers (default: 50)")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Commit results every N addresses (default: 1000)")

    args = parser.parse_args()

    classify_addresses_in_duckdb(
        db_path=args.db,
        chain=args.chain,
        rpc_url=args.rpc_url,
        num_workers=args.num_workers,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
