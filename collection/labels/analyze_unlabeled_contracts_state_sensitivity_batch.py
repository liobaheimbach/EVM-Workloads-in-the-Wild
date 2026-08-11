#!/usr/bin/env python3
"""
Analyze state sensitivity for top unlabeled contracts on Base chain.
Uses async batch RPC requests for much faster processing.

For each contract address:
1. Randomly sample up to 100 transactions
2. Execute debug_traceTransaction with prestateTracer (diffMode=True) in batches
3. Check if state changes are only gas accounting
4. Save results to CSV

Usage:
    python analyze_unlabeled_contracts_state_sensitivity_batch.py [--limit N] [--batch-size N] [--concurrency N]
"""

import os
import random
import csv
import json
import hashlib
import time
import asyncio
import aiohttp
import argparse
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import duckdb
from dotenv import load_dotenv

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

env_path = Path(SCRIPT_DIR) / '.env'
load_dotenv(env_path)

# Configuration - use environment variables
DB_BASE_DIR = os.getenv('DB_BASE_DIR', '/path/to/data')
DB_PATH = os.path.join(DB_BASE_DIR, 'tx_metadata_base_snapshot.duckdb')
SAMPLE_SIZE = 100
TIMEOUT = 120
CHAIN = 'base'

def load_rpc_config():
    """Load RPC configuration from rpc_config.json."""
    config_path = os.path.join(SCRIPT_DIR, 'rpc_config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"rpc_config.json not found at {config_path}")

def get_rpc_url(chain: str) -> str:
    """Get RPC URL for chain from rpc_config.json."""
    config = load_rpc_config()
    if chain not in config:
        raise ValueError(f"Chain '{chain}' not found in rpc_config.json")
    rpc_url = config[chain].get('rpc_url')
    if not rpc_url:
        raise ValueError(f"No rpc_url configured for chain '{chain}'")
    return rpc_url

# Get RPC URL
RPC_URL = get_rpc_url(CHAIN)


def is_only_gas_accounting(trace_result: Dict) -> Tuple[bool, str]:
    """
    Check if state changes are only gas accounting.

    Gas-only transactions have:
    - One sender with nonce+balance changes (paying gas)
    - Zero or more addresses with only balance changes (fee recipients)
      - For Base L2: system contracts starting with 0x42000000000000000000000000000000000000
    - No storage changes on any address
    - No code changes on any address

    Returns:
        (is_only_gas_accounting, reason)
    """
    if not trace_result or 'post' not in trace_result:
        return False, "No 'post' in trace result"

    post = trace_result['post']

    if len(post) == 0:
        return False, "No addresses in post"

    sender_count = 0
    fee_recipient_count = 0

    for addr, changes in post.items():
        # Check for storage or code changes (disqualifies gas-only)
        if 'code' in changes:
            return False, f"Code changed for {addr}"
        if 'storage' in changes and changes['storage']:
            return False, f"Storage changed for {addr}"

        # Determine change pattern
        has_nonce = 'nonce' in changes
        has_balance = 'balance' in changes

        if has_nonce and has_balance:
            # This is the sender (nonce increment + balance decrease)
            sender_count += 1
        elif has_balance and not has_nonce:
            # This is a fee recipient (balance increase only)
            # For Base L2, verify it's a system contract
            if addr.lower().startswith('0x42000000000000000000000000000000000000'):
                fee_recipient_count += 1
            else:
                # Balance change to non-system address means state change
                return False, f"Balance change to non-system address {addr}"
        elif has_nonce and not has_balance:
            # Nonce change without balance change is unusual
            return False, f"Nonce change without balance for {addr}"
        else:
            # Empty change object (no balance, no nonce, no storage, no code)
            # This can happen for some system contracts that are touched but unchanged
            # Check if it's a system contract
            if not addr.lower().startswith('0x42000000000000000000000000000000000000'):
                return False, f"Non-system address {addr} with no changes"

    # Should have exactly one sender
    if sender_count != 1:
        return False, f"Expected 1 sender, got {sender_count}"

    # Fee recipients can be 0 or more (L1 has 1, L2s may have multiple)
    return True, f"Only gas accounting (1 sender, {fee_recipient_count} fee recipients)"


async def trace_transactions_batch(session, rpc_url, tx_hashes, semaphore):
    """Trace multiple transactions in a single batch request."""
    async with semaphore:
        # Create batch payload
        batch_payload = [
            {
                "jsonrpc": "2.0",
                "method": "debug_traceTransaction",
                "params": [
                    tx_hash,
                    {
                        "tracer": "prestateTracer",
                        "tracerConfig": {
                            "diffMode": True
                        }
                    }
                ],
                "id": i
            }
            for i, tx_hash in enumerate(tx_hashes)
        ]

        try:
            async with session.post(
                rpc_url,
                json=batch_payload,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            ) as response:
                if response.status == 200:
                    results = await response.json()
                    # Map results back to tx hashes by response id
                    # (JSON-RPC 2.0 allows responses in any order)
                    results_by_id = {result.get('id'): result for result in results}
                    trace_results = []
                    for i, tx_hash in enumerate(tx_hashes):
                        result = results_by_id.get(i)
                        if result and 'result' in result and result['result']:
                            trace_results.append((tx_hash, result['result'], None))
                        else:
                            error = (result or {}).get('error', {}).get('message', 'No result')
                            trace_results.append((tx_hash, None, error))
                    return trace_results
                else:
                    # If batch fails, return all as errors
                    return [(tx, None, f"HTTP {response.status}") for tx in tx_hashes]
        except asyncio.TimeoutError:
            return [(tx, None, "Timeout") for tx in tx_hashes]
        except Exception as e:
            return [(tx, None, str(e)) for tx in tx_hashes]


async def analyze_address_async(to_address: str, conn: duckdb.DuckDBPyConnection,
                               session, rpc_url, semaphore, batch_size: int) -> Dict:
    """
    Analyze transactions for a given to_address using async batch requests.
    """
    # Get all transactions to this address
    query = """
        SELECT tx_hash
        FROM transactions
        WHERE receiver = ?
        ORDER BY block_number DESC
    """

    tx_hashes = [row[0] for row in conn.execute(query, [to_address]).fetchall()]
    total_txs = len(tx_hashes)

    # Sample up to SAMPLE_SIZE transactions
    if total_txs > SAMPLE_SIZE:
        sampled_hashes = random.sample(tx_hashes, SAMPLE_SIZE)
    else:
        sampled_hashes = tx_hashes

    # Split into batches
    tx_batches = [sampled_hashes[i:i + batch_size]
                  for i in range(0, len(sampled_hashes), batch_size)]

    only_gas_accounting = 0
    has_state_changes = 0
    traced_txs = 0
    trace_errors = []

    # Process batches
    for batch in tx_batches:
        trace_results = await trace_transactions_batch(session, rpc_url, batch, semaphore)

        for tx_hash, trace_result, error in trace_results:
            if error or trace_result is None:
                trace_errors.append((tx_hash, error or "No result"))
                continue

            traced_txs += 1
            is_gas_only, reason = is_only_gas_accounting(trace_result)

            if is_gas_only:
                only_gas_accounting += 1
            else:
                has_state_changes += 1

    if trace_errors:
        first_hash, first_error = trace_errors[0]
        raise RuntimeError(f"{len(trace_errors)}/{len(sampled_hashes)} traces failed; "
                           f"first: {first_hash}: {first_error}")

    gas_only_pct = (only_gas_accounting / traced_txs * 100) if traced_txs > 0 else 0

    result = {
        'to_address': to_address,
        'total_txs': total_txs,
        'sampled_txs': len(sampled_hashes),
        'traced_txs': traced_txs,
        'only_gas_accounting': only_gas_accounting,
        'has_state_changes': has_state_changes,
        'gas_only_pct': gas_only_pct
    }

    return result


def load_unlabeled_contracts(csv_path: str, limit: Optional[int] = None, db_path: Optional[str] = None, min_tx_count: int = 10000) -> List[str]:
    """
    Load contract addresses - either from CSV file or directly from database.

    If db_path is provided, queries database for unlabeled contracts with ≥min_tx_count transactions.
    Otherwise, loads from CSV file.
    """
    if db_path and os.path.exists(db_path):
        # Query database directly
        print(f"Querying database for unlabeled contracts with ≥{min_tx_count:,} txs...")

        # Check if to_label column exists
        columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'transactions'"
        try:
            columns = [row[0] for row in duckdb.connect(db_path, read_only=True).execute(columns_query).fetchall()]
        except Exception:
            # Fallback for DuckDB versions without information_schema.columns
            columns = [row[0] for row in duckdb.connect(db_path, read_only=True).execute("DESCRIBE transactions").fetchall()]

        has_to_label = 'to_label' in columns
        has_to_category = 'to_category' in columns

        # Build query
        where_conditions = ["receiver IS NOT NULL", "receiver != ''"]
        if has_to_label:
            where_conditions.append("to_label IS NULL")
        elif has_to_category:
            where_conditions.append("to_category IS NULL")

        where_clause = " AND ".join(where_conditions)

        query = f"""
        SELECT receiver as address, COUNT(*) as tx_count
        FROM transactions
        WHERE {where_clause}
        GROUP BY receiver
        HAVING COUNT(*) >= {min_tx_count}
        ORDER BY tx_count DESC
        """

        # Only apply limit if explicitly specified
        if limit:
            query += f"\nLIMIT {limit}"

        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute(query).fetchall()
        conn.close()

        addresses = [row[0] for row in result]
        print(f"Found {len(addresses):,} unlabeled contracts from database (≥{min_tx_count:,} txs)")
        return addresses

    # Fallback to CSV
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Neither database nor CSV found: db={db_path}, csv={csv_path}")

    addresses = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            addresses.append(row['address'])
    return addresses


def save_results(results: List[Dict], output_path: str):
    """Save results to CSV file."""
    fieldnames = [
        'to_address', 'total_txs', 'sampled_txs', 'traced_txs',
        'only_gas_accounting', 'has_state_changes', 'gas_only_pct'
    ]

    temp_path = output_path + '.tmp'
    with open(temp_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    os.replace(temp_path, output_path)

    print(f"\nResults saved to: {output_path}")


def run_manifest(db_path: str, addresses: List[str], limit: Optional[int],
                 min_tx_count: int) -> Dict:
    """Describe the inputs that determine a resumable analysis population."""
    db = Path(db_path).resolve()
    stat = db.stat()
    normalized = sorted(address.lower() for address in addresses)
    script_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    rpc_digest = hashlib.sha256(RPC_URL.encode()).hexdigest()
    address_digest = hashlib.sha256("\n".join(normalized).encode()).hexdigest()
    return {
        'version': 1,
        'chain': CHAIN,
        'database': str(db),
        'database_size': stat.st_size,
        'database_mtime_ns': stat.st_mtime_ns,
        'limit': limit,
        'min_tx_count': min_tx_count,
        'sample_size': SAMPLE_SIZE,
        'address_count': len(normalized),
        'address_sha256': address_digest,
        'script_sha256': script_digest,
        'rpc_url_sha256': rpc_digest,
    }


def write_manifest(path: str, manifest: Dict):
    temp_path = path + '.tmp'
    with open(temp_path, 'w') as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write('\n')
    os.replace(temp_path, path)


def validate_manifest(path: str, expected: Dict):
    if not os.path.exists(path):
        raise RuntimeError(f"Partial checkpoint has no provenance manifest: {path}")
    with open(path, 'r') as f:
        actual = json.load(f)
    if actual != expected:
        raise RuntimeError(
            "Partial checkpoint does not match this run's database or population; "
            "use a different --output path or remove the checkpoint after reviewing it"
        )


async def process_contracts(addresses: List[str], conn: duckdb.DuckDBPyConnection,
                           partial_csv: str, concurrency: int, batch_size: int,
                           results: Optional[List[Dict]] = None):
    """Process all contracts with async batch requests."""
    if results is None:
        results = []
    failed_addresses = []
    start_time = time.time()

    # Create semaphore to limit concurrent batch requests
    semaphore = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency * 2, limit_per_host=concurrency * 2)

    async with aiohttp.ClientSession(connector=connector) as session:
        for i, address in enumerate(addresses, 1):
            print(f"[{i}/{len(addresses)}] Analyzing {address}...")

            try:
                result = await analyze_address_async(address, conn, session, RPC_URL,
                                                     semaphore, batch_size)
                results.append(result)

                print(f"  Total txs: {result['total_txs']}, "
                      f"Sampled: {result['sampled_txs']}, "
                      f"Traced: {result['traced_txs']}, "
                      f"Gas-only: {result['only_gas_accounting']} ({result['gas_only_pct']:.1f}%), "
                      f"State changes: {result['has_state_changes']}")

                # Save intermediate results every 50 contracts
                if i % 50 == 0:
                    save_results(results, partial_csv)
                    elapsed = time.time() - start_time
                    rate = i / elapsed
                    remaining = len(addresses) - i
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds < 3600 else f"{eta_seconds/3600:.1f}h"
                    print(f"\n  Progress checkpoint: {i}/{len(addresses)} analyzed, "
                          f"Rate: {rate:.2f} contracts/s, ETA: {eta_str}\n")

            except Exception as e:
                print(f"  Error analyzing {address}: {e}")
                failed_addresses.append(address)
                continue

    print(f"\nFailed addresses: {len(failed_addresses)}/{len(addresses)}")
    if failed_addresses:
        print(f"WARNING: {len(failed_addresses)} addresses failed; the run will remain partial")

    return results, time.time() - start_time, failed_addresses


def main():
    """Main analysis function."""
    parser = argparse.ArgumentParser(
        description="Analyze state sensitivity for unlabeled contracts on Base chain (async batch)"
    )
    parser.add_argument("--limit", type=int, default=None,
                       help="Number of contracts to analyze (default: all)")
    parser.add_argument("--output", type=str, default=None,
                       help="Output CSV file path (default: auto-generated based on limit, written next to this script)")
    parser.add_argument("--concurrency", type=int, default=10,
                       help="Number of concurrent batch requests (default: 10)")
    parser.add_argument("--batch-size", type=int, default=20,
                       help="Number of transactions per batch (default: 20)")
    parser.add_argument("--db", type=str, default=None,
                       help="Database path to query for unlabeled contracts (overrides CSV)")
    parser.add_argument("--min-tx-count", type=int, default=10000,
                       help="Minimum transaction count for unlabeled contracts (default: 10000)")

    args = parser.parse_args()

    # Use provided db path or default
    db_path_to_use = args.db if args.db else DB_PATH

    # Generate output filename based on limit
    if args.output:
        output_csv = args.output
    else:
        output_suffix = f"top{args.limit}" if args.limit else "all"
        output_csv = os.path.join(SCRIPT_DIR, f'unlabeled_contracts_state_sensitivity_base_{output_suffix}.csv')

    print("="*80)
    print("STATE SENSITIVITY ANALYSIS FOR UNLABELED CONTRACTS (ASYNC BATCH)")
    print("="*80)
    print(f"Database: {db_path_to_use}")
    print(f"Output: {output_csv}")
    print(f"Chain: {CHAIN}")
    print(f"RPC: {RPC_URL}")
    print(f"Limit: {args.limit if args.limit else 'all'} contracts")
    print(f"Min tx count: {args.min_tx_count:,}")
    print(f"Sample size: {SAMPLE_SIZE} transactions per contract")
    print(f"Concurrency: {args.concurrency} batch requests")
    print(f"Batch size: {args.batch_size} transactions per batch")
    print("="*80)
    print()


    # Check if database exists
    if not os.path.exists(db_path_to_use):
        print(f"Error: Database not found: {db_path_to_use}")
        return 1

    # Load contract addresses from database
    print(f"Loading unlabeled contract addresses...")
    addresses = load_unlabeled_contracts(None, limit=args.limit, db_path=db_path_to_use, min_tx_count=args.min_tx_count)

    print(f"Loaded {len(addresses)} contract addresses")
    if not addresses:
        print("No unlabeled contracts found. Exiting.")
        return 0


    # Resume from partial checkpoint file if present
    partial_csv = output_csv + '.partial'
    manifest_path = partial_csv + '.manifest.json'
    final_manifest_path = output_csv + '.manifest.json'
    expected_manifest = run_manifest(db_path_to_use, addresses, args.limit,
                                     args.min_tx_count)

    if os.path.exists(output_csv):
        try:
            validate_manifest(final_manifest_path, expected_manifest)
        except (OSError, ValueError, RuntimeError) as e:
            print(f"Error: completed output provenance mismatch: {e}")
            return 1

        print(f"Validated completed output: {output_csv}")
        print("Checking for MEV contracts (gas_only_pct > 50%)...")
        import pandas as pd
        results_df = pd.read_csv(output_csv)
        if 'gas_only_pct' in results_df.columns:
            mev_contracts = results_df[results_df['gas_only_pct'] > 50.0]
            print(f"Found {len(mev_contracts)} contracts with >50% gas-only transactions (MEV)")
            if len(mev_contracts) > 0:
                mev_csv = output_csv.replace('.csv', '_mev.csv')
                mev_contracts[['to_address']].rename(
                    columns={'to_address': 'address'}
                ).to_csv(mev_csv, index=False)
                print(f"Saved MEV contract addresses to: {mev_csv}")
        return 0

    prior_results = []
    if os.path.exists(partial_csv):
        try:
            validate_manifest(manifest_path, expected_manifest)
        except (OSError, ValueError, RuntimeError) as e:
            print(f"Error: {e}")
            return 1
        with open(partial_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                prior_results.append({
                    'to_address': row['to_address'],
                    'total_txs': int(row['total_txs']),
                    'sampled_txs': int(row['sampled_txs']),
                    'traced_txs': int(row['traced_txs']),
                    'only_gas_accounting': int(row['only_gas_accounting']),
                    'has_state_changes': int(row['has_state_changes']),
                    'gas_only_pct': float(row['gas_only_pct'])
                })
        done_addresses = {r['to_address'].lower() for r in prior_results}
        addresses = [a for a in addresses if a.lower() not in done_addresses]
        print(f"Found partial results: {partial_csv}")
        print(f"  Resuming: {len(prior_results)} contracts already analyzed, {len(addresses)} remaining")
    else:
        write_manifest(manifest_path, expected_manifest)


    print("Connecting to database...")
    conn = duckdb.connect(db_path_to_use, read_only=True)
    print("Connected")
    print()

    # Process contracts
    results, elapsed, failed_addresses = asyncio.run(
        process_contracts(addresses, conn, partial_csv, args.concurrency,
                          args.batch_size, results=prior_results)
    )

    # Save results, but promote only a complete run.
    save_results(results, partial_csv)
    if failed_addresses:
        conn.close()
        print("Run incomplete; checkpoint retained for retry.")
        print("Failed addresses:")
        for address in failed_addresses:
            print(f"  {address}")
        return 1

    os.replace(partial_csv, output_csv)
    os.replace(manifest_path, final_manifest_path)
    print(f"Final results moved to: {output_csv}")

    # Print summary
    print()
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total contracts analyzed: {len(results)}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Average rate: {len(results)/elapsed:.2f} contracts/second")
    print()

    if results:
        total_txs = sum(r['total_txs'] for r in results)
        total_traced = sum(r['traced_txs'] for r in results)
        total_gas_only = sum(r['only_gas_accounting'] for r in results)
        total_state_changes = sum(r['has_state_changes'] for r in results)

        print(f"Total transactions: {total_txs:,}")
        print(f"Traced transactions: {total_traced:,}")
        print(f"Gas-only accounting: {total_gas_only:,} ({total_gas_only/total_traced*100:.1f}%)")
        print(f"Has state changes: {total_state_changes:,} ({total_state_changes/total_traced*100:.1f}%)")

        # Top 10 highest gas-only percentage
        sorted_results = sorted(results, key=lambda x: x['gas_only_pct'], reverse=True)
        print()
        print("Top 10 contracts with highest gas-only percentage:")
        for i, r in enumerate(sorted_results[:10], 1):
            print(f"  {i}. {r['to_address']}: {r['gas_only_pct']:.1f}% "
                  f"({r['only_gas_accounting']}/{r['traced_txs']} traced)")

        # Extract MEV contracts (>50% gas-only)
        print()
        print("="*80)
        print("MEV CONTRACT EXTRACTION")
        print("="*80)

        import pandas as pd
        results_df = pd.DataFrame(results)
        mev_contracts = results_df[results_df['gas_only_pct'] > 50.0]

        print(f"Contracts with >50% gas-only transactions (MEV): {len(mev_contracts)}")

        if len(mev_contracts) > 0:
            # Save MEV contracts to separate CSV
            mev_csv = output_csv.replace('.csv', '_mev.csv')
            mev_contracts[['to_address']].rename(columns={'to_address': 'address'}).to_csv(mev_csv, index=False)
            print(f"Saved MEV contract addresses to: {mev_csv}")
            print()
            print("Sample MEV contracts:")
            for _, row in mev_contracts.head(10).iterrows():
                print(f"  {row['to_address']}: {row['gas_only_pct']:.1f}% gas-only")

    conn.close()
    print("="*80)

    return 0


if __name__ == '__main__':
    exit(main())
