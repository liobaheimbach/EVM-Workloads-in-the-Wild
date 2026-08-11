#!/usr/bin/env python3
"""
Identify pools and tokens among unlabeled contracts by calling specific functions.

For pools, checks:
- Uniswap V2: getReserves(), token0(), token1()
- Uniswap V3: slot0(), token0(), token1()
- Aerodrome/Velodrome: stable(), token0(), token1(), metadata()
- Curve: coins(0), get_dy()
- Balancer: getPoolId(), getVault()

For tokens, checks ERC20 functions:
- totalSupply(), decimals(), symbol(), name(), balanceOf(address)

Usage:
    python identify_pools_and_tokens.py --db ~/data.duckdb --limit 10000
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import duckdb
import pandas as pd
from web3 import Web3
from web3.exceptions import ContractLogicError
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Rate limiting configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds

# Setup paths
SCRIPT_DIR = Path(__file__).parent

# Function signatures (4-byte)
SIGNATURES = {
    # Uniswap V2 Pool
    'getReserves': '0x0902f1ac',  # getReserves() returns (uint112,uint112,uint32)
    'token0': '0x0dfe1681',        # token0() returns address
    'token1': '0xd21220a7',        # token1() returns address

    # Uniswap V3 Pool
    'slot0': '0x3850c7bd',         # slot0() returns (uint160,int24,uint16,uint16,uint16,uint8,bool)

    # Aerodrome/Velodrome Pool (Solidly fork)
    'stable': '0x22be3de8',        # stable() returns bool

    # Curve Pool
    'coins': '0xc6610657',         # coins(uint256) returns address

    # Balancer V2 Pool
    'getPoolId': '0x38fff2d0',    # getPoolId() returns bytes32
    'getVault': '0x8d928af8',     # getVault() returns address

    # ERC20 Token
    'totalSupply': '0x18160ddd',  # totalSupply() returns uint256
    'decimals': '0x313ce567',     # decimals() returns uint8
}

def is_rate_limit_error(exception: Exception) -> bool:
    """Check if an exception is a rate limit error."""
    error_msg = str(exception).lower()
    rate_limit_indicators = [
        'too many requests',
        '429',
        'rate limit',
        'exceeded',
        'throttle',
        'quota'
    ]
    return any(indicator in error_msg for indicator in rate_limit_indicators)

def rpc_call_with_retry(w3: Web3, call_params: dict[str, str], max_retries: int = MAX_RETRIES) -> Optional[bytes]:
    """Make an RPC call with exponential backoff on rate limit errors."""
    backoff = INITIAL_BACKOFF

    for attempt in range(max_retries):
        try:
            result = w3.eth.call(call_params)
            return result
        except ContractLogicError:
            # Contract doesn't have this function - not an error, just return None
            return None
        except Exception as e:
            if is_rate_limit_error(e):
                if attempt < max_retries - 1:
                    logger.warning(f"Rate limit hit, backing off for {backoff}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                else:
                    logger.error(f"Rate limit exceeded after {max_retries} retries")
                    raise
            else:
                # Other error - log and return None
                logger.debug(f"RPC call failed: {e}")
                return None

    return None

def load_rpc_config(rpc_config_path: Path, chain: str = None) -> str:
    """Load RPC URL from config file."""
    if not rpc_config_path.exists():
        raise FileNotFoundError(f"RPC config not found: {rpc_config_path}")

    with open(rpc_config_path) as f:
        config = json.load(f)

    # If chain is specified and config has nested structure, try chain-specific config
    if chain and chain in config and isinstance(config[chain], dict):
        chain_config = config[chain]
        rpc_url = chain_config.get('rpc_url') or chain_config.get('url')
        if rpc_url:
            return rpc_url

    # Try different field names that might contain the RPC URL at top level
    return config.get('url') or config.get('base_rpc') or config.get('ethereum_rpc') or config.get('rpc_url')

def get_all_receivers(db_path: str, min_tx_count: int = 100, skip_already_identified_csv: str = None) -> List[Tuple[str, int]]:
    """Get receiver addresses from database with at least min_tx_count transactions."""
    logger.info(f"Querying receivers with >= {min_tx_count} transactions from {db_path}...")

    con = duckdb.connect(db_path, read_only=True)

    # Get ALL receivers with their transaction counts, filtered by minimum tx count
    # We'll filter out already-labeled addresses using the CSV (from Spellbook, DefiLlama, Kleros, etc.)
    query = f'''
        SELECT DISTINCT
            t.receiver as address,
            COUNT(*) as tx_count
        FROM transactions t
        WHERE t.receiver IS NOT NULL
        GROUP BY t.receiver
        HAVING COUNT(*) >= {min_tx_count}
        ORDER BY tx_count DESC
    '''

    result = con.execute(query).fetchall()
    con.close()

    logger.info(f"Found {len(result):,} total receiver addresses with >= {min_tx_count} transactions")

    # Filter out addresses that already have labels in the main CSV
    # (from Spellbook, DefiLlama, Kleros, manual labels, etc.)
    if skip_already_identified_csv and Path(skip_already_identified_csv).exists():
        identified_df = pd.read_csv(skip_already_identified_csv)
        identified_addresses = set(identified_df['Address'].str.lower())
        original_count = len(result)
        result = [(addr, count) for addr, count in result if addr.lower() not in identified_addresses]
        filtered_count = original_count - len(result)
        logger.info(f"Filtered out {filtered_count:,} addresses already in {Path(skip_already_identified_csv).name}")

    logger.info(f"Final count: {len(result):,} unlabeled receiver addresses to identify via RPC")
    return result

def check_uniswap_v2_pool(w3: Web3, address: str) -> bool:
    """Check if address is a Uniswap V2 style pool."""
    try:
        # Try calling getReserves() - signature: getReserves() returns (uint112,uint112,uint32)
        result = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['getReserves']
        })
        if result and len(result) > 0:
            # Also check token0() and token1()
            token0 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token0']
            })
            token1 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token1']
            })
            return bool(token0 and token1)
    except Exception as e:
        logger.debug(f"Error checking Uniswap V2 pool {address}: {e}")
    return False

def check_uniswap_v3_pool(w3: Web3, address: str) -> bool:
    """Check if address is a Uniswap V3 pool."""
    try:
        # Try calling slot0()
        result = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['slot0']
        })
        if result and len(result) >= 32:
            # Also verify token0() and token1()
            token0 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token0']
            })
            token1 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token1']
            })
            return bool(token0 and token1)
    except Exception as e:
        logger.debug(f"Error checking Uniswap V3 pool {address}: {e}")
    return False

def check_curve_pool(w3: Web3, address: str) -> bool:
    """Check if address is a Curve pool."""
    try:
        # Try calling coins(0) - needs to encode uint256 argument
        coins_call = SIGNATURES['coins'] + '0' * 64  # coins(0)
        result = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': coins_call
        })
        return bool(result and len(result) > 0)
    except Exception as e:
        logger.debug(f"Error checking Curve pool {address}: {e}")
    return False

def check_balancer_pool(w3: Web3, address: str) -> bool:
    """Check if address is a Balancer V2 or V3 pool."""
    try:
        # Try calling getPoolId() first (Balancer V2)
        try:
            result = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['getPoolId']
            })
            if result and len(result) == 32:
                return True
        except Exception:
            pass

        # Balancer V3 pools don't have getPoolId() but have getVault()
        # Check for getVault() as fallback
        vault = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['getVault']
        })
        if vault and len(vault) == 32:
            # Verify vault address is non-zero to avoid false positives
            vault_addr = vault.hex()
            if vault_addr != '0x' + '0' * 64:
                return True
    except Exception as e:
        logger.debug(f"Error checking Balancer pool {address}: {e}")
    return False

def check_aerodrome_pool(w3: Web3, address: str) -> bool:
    """Check if address is an Aerodrome/Velodrome pool (Solidly fork)."""
    try:
        # Try calling stable() - returns bool
        stable = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['stable']
        })
        if stable:
            # Also verify token0() and token1()
            token0 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token0']
            })
            token1 = rpc_call_with_retry(w3, {
                'to': Web3.to_checksum_address(address),
                'data': SIGNATURES['token1']
            })
            return bool(token0 and token1)
    except Exception as e:
        logger.debug(f"Error checking Aerodrome pool {address}: {e}")
    return False

def check_erc20_token(w3: Web3, address: str) -> bool:
    """Check if address is an ERC20 token by calling standard functions."""
    try:
        # Check totalSupply()
        total_supply = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['totalSupply']
        })
        if not total_supply or len(total_supply) == 0:
            return False

        # Check decimals()
        decimals = rpc_call_with_retry(w3, {
            'to': Web3.to_checksum_address(address),
            'data': SIGNATURES['decimals']
        })
        if not decimals:
            return False

        # If totalSupply and decimals work, it's likely an ERC20
        return True
    except Exception as e:
        logger.debug(f"Error checking ERC20 token {address}: {e}")
    return False

def is_contract(w3: Web3, address: str) -> bool:
    """Check if an address is a contract (has code) or an EOA."""
    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            code = w3.eth.get_code(Web3.to_checksum_address(address))
            # EOAs have no code (0x or empty), contracts have bytecode
            return len(code) > 2  # More than just '0x'
        except Exception as e:
            if is_rate_limit_error(e):
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Rate limit hit checking contract {address}, backing off for {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
            logger.debug(f"Error checking if {address} is contract: {e}")
            return False
    return False

def identify_contract_type(w3: Web3, address: str) -> Tuple[Optional[str], Optional[str], bool, bool]:
    """
    Identify contract type by calling various functions.

    Returns:
        (category, application_name, is_eoa, is_contract) tuple

    Returns ('EOA', 'EOA', True, False) for EOAs.
    Returns (None, None, False, False) for unidentified contracts.
    """
    # First check if it's an EOA or contract
    if not is_contract(w3, address):
        return ('EOA', 'EOA', True, False)

    # It's a contract - try to identify what type
    # Check DEX pools first (most specific checks)
    if check_uniswap_v3_pool(w3, address):
        return ('DEX', 'Uniswap V3 Pool', False, True)

    if check_aerodrome_pool(w3, address):
        return ('DEX', 'Aerodrome Pool', False, True)

    if check_uniswap_v2_pool(w3, address):
        return ('DEX', 'Uniswap V2 Pool', False, True)

    if check_curve_pool(w3, address):
        return ('DEX', 'Curve Pool', False, True)

    if check_balancer_pool(w3, address):
        return ('DEX', 'Balancer Pool', False, True)

    # Check if it's a generic ERC20 token (least specific)
    if check_erc20_token(w3, address):
        return ('Token', 'ERC20 Token', False, True)

    # Unidentified contract - skip it
    return (None, None, False, False)

def batch_identify_contracts(w3: Web3, addresses: List[Tuple[str, int]], batch_size: int = 100, max_workers: int = 10, chain: str = 'unknown') -> List[Dict]:
    """Identify contract types in batches with parallel processing."""
    import concurrent.futures
    identified = []
    total = len(addresses)

    logger.info(f"Processing {total:,} addresses with {max_workers} workers...")

    for i in range(0, total, batch_size):
        batch = addresses[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (total + batch_size - 1)//batch_size
        logger.info(f"Processing batch {batch_num}/{total_batches} ({i:,}/{total:,} addresses)")

        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(identify_contract_type, w3, address): (address, tx_count)
                       for address, tx_count in batch}

            for future in concurrent.futures.as_completed(futures):
                address, tx_count = futures[future]
                try:
                    category, app_name, is_eoa, is_contract_flag = future.result()

                    # Record EOAs and identified pool/token types
                    if category:
                        identified.append({
                            'Type': 'EOA' if is_eoa else 'Contract',
                            'Application_Name': app_name,
                            'Contract_Name': '',
                            'Address': address.lower(),
                            'Source': 'RPC-Function-Call',
                            'Category': category,
                            'Chain': chain,
                            'is_eoa': is_eoa,
                            'is_contract': is_contract_flag,
                            'tx_count': tx_count
                        })
                        logger.info(f"  {address}: {app_name} ({category})")
                    else:
                        # Skip unidentified contracts
                        logger.debug(f"  - {address}: Skipped (not an EOA, pool, or token)")
                except Exception as e:
                    logger.warning(f"  FAILED: {address}: Error - {e}")
                    continue

        if batch_num % 10 == 0:
            logger.info(f"  Progress: {len(identified):,} addresses identified so far")

    return identified

def main():
    parser = argparse.ArgumentParser(description='Identify EOAs/contracts and pool/token types for receiver addresses')
    parser.add_argument('--db', required=True, help='Path to DuckDB database')
    parser.add_argument('--limit', type=int, default=None, help='Number of top addresses to check (default: ALL)')
    parser.add_argument('--min-tx-count', type=int, default=100, help='Minimum transaction count for addresses (default: 100)')
    parser.add_argument('--batch-size', type=int, default=1, help='Batch size for processing (default: 1)')
    parser.add_argument('--max-workers', type=int, default=2, help='Number of parallel workers (default: 2)')
    parser.add_argument('--output', default='identified_pools_tokens.csv', help='Output CSV file')
    parser.add_argument('--rpc-config', help='Path to RPC config JSON file (default: rpc_config.json)')
    parser.add_argument('--chain', default='base', help='Chain name (base, ethereum, etc.)')
    parser.add_argument('--skip-csv', help='CSV file with addresses to skip (e.g., addresses_with_categories.csv from Spellbook/DefiLlama/Kleros)')

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("IDENTIFY EOAs/CONTRACTS AND POOL/TOKEN TYPES")
    logger.info("="*80)
    logger.info(f"Database: {args.db}")
    logger.info(f"Min TX count: {args.min_tx_count}")
    limit_str = 'ALL' if args.limit is None else f'{args.limit:,}'
    logger.info(f"Limit: {limit_str}")
    logger.info(f"Chain: {args.chain}")
    logger.info(f"Workers: {args.max_workers}")
    logger.info(f"Output: {args.output}")
    logger.info("="*80)

    # Load RPC config
    if args.rpc_config:
        rpc_config_path = Path(args.rpc_config)
    else:
        rpc_config_path = SCRIPT_DIR / "rpc_config.json"

    rpc_url = load_rpc_config(rpc_config_path, args.chain)

    if not rpc_url:
        logger.error(f"Failed to load RPC URL from {rpc_config_path} for chain {args.chain}")
        logger.error(f"Make sure the config file has the correct structure")
        return 1

    logger.info(f"Using RPC: {rpc_url[:50]}...")

    # Connect to Web3
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        logger.error("Failed to connect to RPC endpoint")
        return 1

    logger.info(f"Connected to chain (Chain ID: {w3.eth.chain_id})")

    # Get receiver addresses with minimum tx count
    all_addresses = get_all_receivers(args.db, args.min_tx_count, args.skip_csv)

    if not all_addresses:
        logger.info(f"No receiver addresses found with >= {args.min_tx_count} transactions")
        return 0

    # Apply limit if specified
    if args.limit:
        addresses = all_addresses[:args.limit]
        logger.info(f"Processing top {len(addresses):,} of {len(all_addresses):,} addresses")
    else:
        addresses = all_addresses
        logger.info(f"Processing ALL {len(addresses):,} addresses with >= {args.min_tx_count} transactions")

    # Identify contract types
    logger.info(f"\nIdentifying EOAs, contracts, pools, and tokens...")
    identified = batch_identify_contracts(w3, addresses, args.batch_size, args.max_workers, args.chain)

    logger.info("="*80)
    logger.info(f"RESULTS")
    logger.info("="*80)
    logger.info(f"Total addresses checked: {len(addresses):,}")
    logger.info(f"Identified: {len(identified):,}")
    logger.info(f"Unidentified: {len(addresses) - len(identified):,}")

    if identified:
        # Save to CSV
        df = pd.DataFrame(identified)
        output_path = SCRIPT_DIR / args.output
        df.to_csv(output_path, index=False)
        logger.info(f"\nSaved identified addresses to: {output_path}")

        # Show breakdown
        logger.info(f"\nEOA vs Contract breakdown:")
        logger.info(f"  EOAs: {df['is_eoa'].sum():,}")
        logger.info(f"  Contracts: {df['is_contract'].sum():,}")

        logger.info(f"\nCategory breakdown:")
        for category, count in df['Category'].value_counts().items():
            logger.info(f"  {category}: {count:,}")

        logger.info(f"\nApplication breakdown (top 20):")
        for app, count in df['Application_Name'].value_counts().head(20).items():
            logger.info(f"  {app}: {count:,}")
    else:
        logger.info("\nNo addresses could be identified")

    return 0

if __name__ == '__main__':
    exit(main())
