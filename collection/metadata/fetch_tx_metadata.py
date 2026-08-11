"""Fetch tx and block metadata for blocks referenced by opcode breakdown parquet files."""
import os
import sys
import duckdb
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

def load_rpc_config():
    """Load RPC endpoints from rpc_config.json."""
    config_path = os.path.join(SCRIPT_DIR, 'rpc_config.json')

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"RPC config file not found: {config_path}\n"
            "Expected rpc_config.json in the same directory as this script"
        )

    with open(config_path, 'r') as f:
        config = json.load(f)

    return {
        'base': config['base']['rpc_url'],
        'ethereum': config['ethereum']['rpc_url']
    }

RPC_ENDPOINTS = load_rpc_config()
print(f"Loaded RPC endpoints for chains: {', '.join(RPC_ENDPOINTS.keys())}")

DB_BASE_DIR = os.getenv('DB_BASE_DIR')
if not DB_BASE_DIR:
    print("ERROR: DB_BASE_DIR not set in .env file")
    sys.exit(1)

DATA_ROOT = os.path.join(DB_BASE_DIR, "opcode_breakdown")
DB_DIR = DB_BASE_DIR

def get_tx_db_path(chain):
    """Get chain-specific transaction database path."""
    return os.path.join(DB_DIR, f"tx_metadata_{chain}.duckdb")

def get_block_db_path(chain):
    """Get chain-specific block metadata database path."""
    return os.path.join(DB_DIR, f"block_metadata_{chain}.duckdb")

class RPCClient:
    def __init__(self, endpoint, chain_name):
        self.endpoint = endpoint
        self.chain_name = chain_name
        self.request_count = 0
        self.error_count = 0

    def get_block_by_number(self, block_number):
        """Fetch block data with all transactions via RPC."""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), True],  # True = include full transaction objects
            "id": 1
        }

        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            self.request_count += 1

            if response.status_code == 200:
                result = response.json()
                if 'result' in result and result['result']:
                    return result['result']
                else:
                    self.error_count += 1
                    return None
            else:
                self.error_count += 1
                print(f"  HTTP {response.status_code} for block {block_number}")
                return None

        except Exception as e:
            self.error_count += 1
            print(f"  Error fetching block {block_number}: {e}")
            return None

def extract_block_numbers_from_parquet(chain):
    """Extract all unique block numbers from parquet files for a chain."""
    con = duckdb.connect()

    data_path = os.path.join(DATA_ROOT, chain)

    print(f"\nScanning parquet files in {data_path}...")

    query = f"""
        SELECT DISTINCT block_number
        FROM '{data_path}/**/data.parquet'
        WHERE block_number IS NOT NULL
        ORDER BY block_number
    """

    try:
        result = con.execute(query).fetchall()
        block_numbers = [row[0] for row in result]
        print(f"  Found {len(block_numbers)} unique blocks for {chain}")
        con.close()
        return block_numbers
    except Exception as e:
        con.close()
        raise RuntimeError(f"failed to read opcode parquet for {chain}: {e}") from e

def setup_tx_database(chain):
    """Create transaction metadata database and table schema."""
    db_path = get_tx_db_path(chain)
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_blocks (
            block_number BIGINT PRIMARY KEY
        )
    """)

    # Transactions table
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            tx_hash VARCHAR PRIMARY KEY,
            block_number BIGINT NOT NULL,
            tx_index INTEGER,
            sender VARCHAR,
            receiver VARCHAR,
            function_selector VARCHAR,
            gas_used BIGINT,
            gas_limit BIGINT,
            gas_price BIGINT,
            tx_type INTEGER,
            receiver_is_eoa BOOLEAN,
            receiver_is_contract BOOLEAN,
            simple_transfer BOOLEAN,
            from_label VARCHAR,
            from_category VARCHAR,
            to_label VARCHAR,
            to_category VARCHAR
        )
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_block_number ON transactions(block_number)
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_sender ON transactions(sender)
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_receiver ON transactions(receiver)
    """)

    print(f"\nTransaction database initialized at: {db_path}")
    con.close()

def setup_block_database(chain):
    """Create block metadata database and table schema."""
    db_path = get_block_db_path(chain)
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS block_metadata (
            block_number BIGINT PRIMARY KEY,
            timestamp BIGINT,
            base_fee_per_gas BIGINT,
            gas_used BIGINT,
            gas_limit BIGINT,
            miner VARCHAR,
            difficulty VARCHAR,
            total_difficulty VARCHAR,
            size BIGINT,
            extra_data VARCHAR,
            fetched_at TIMESTAMP
        )
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp ON block_metadata(timestamp)
    """)

    print(f"Block metadata database initialized at: {db_path}")
    con.close()

def fetch_and_store_metadata(chain, block_numbers, batch_size=100):
    """Fetch all transactions and block metadata from blocks and store in separate databases."""
    rpc = RPCClient(RPC_ENDPOINTS[chain], chain)

    tx_db_path = get_tx_db_path(chain)
    block_db_path = get_block_db_path(chain)

    tx_con = duckdb.connect(tx_db_path)
    block_con = duckdb.connect(block_db_path)

    # Check which blocks we already have
    existing_blocks = tx_con.execute("SELECT block_number FROM processed_blocks").fetchall()
    existing_set = {row[0] for row in existing_blocks}

    existing_block_meta = block_con.execute("SELECT block_number FROM block_metadata").fetchall()
    existing_block_meta_set = {row[0] for row in existing_block_meta}

    to_fetch = [bn for bn in block_numbers if bn not in existing_set or bn not in existing_block_meta_set]

    print(f"\n{chain.upper()}: {len(existing_set)} blocks in tx database, {len(existing_block_meta_set)} in block database, {len(to_fetch)} blocks to fetch")

    if not to_fetch:
        tx_con.close()
        block_con.close()
        return

    total = len(to_fetch)
    fetched_blocks = 0
    fetched_txs = 0
    fetched_block_meta = 0
    tx_parse_errors = 0
    blocks_with_tx_errors = 0
    tx_batch = []
    block_meta_batch = []
    processed_blocks_batch = []
    start_time = time.time()

    for i, block_number in enumerate(to_fetch, 1):
        block_data = rpc.get_block_by_number(block_number)

        if block_data and 'transactions' in block_data:
            fetched_blocks += 1

            try:
                block_record = {
                    'block_number': block_number,
                    'timestamp': int(block_data.get('timestamp', '0x0'), 16),
                    'base_fee_per_gas': int(block_data.get('baseFeePerGas', '0x0'), 16) if block_data.get('baseFeePerGas') else None,
                    'gas_used': int(block_data.get('gasUsed', '0x0'), 16),
                    'gas_limit': int(block_data.get('gasLimit', '0x0'), 16),
                    'miner': block_data.get('miner', '').lower() if block_data.get('miner') else None,
                    'difficulty': block_data.get('difficulty'),
                    'total_difficulty': block_data.get('totalDifficulty'),
                    'size': int(block_data.get('size', '0x0'), 16) if block_data.get('size') else None,
                    'extra_data': block_data.get('extraData'),
                    'fetched_at': datetime.now()
                }
                block_meta_batch.append(block_record)
                fetched_block_meta += 1
            except Exception as e:
                print(f"  Error parsing block metadata for {block_number}: {e}")

            transactions = block_data['transactions']
            fetched_txs += len(transactions)
            block_tx_errors = 0

            for tx in transactions:
                try:
                    tx_record = {
                        'tx_hash': tx.get('hash', '').lower() if tx.get('hash') else None,
                        'block_number': block_number,
                        'tx_index': int(tx.get('transactionIndex', '0x0'), 16) if tx.get('transactionIndex') else None,
                        'sender': tx.get('from', '').lower() if tx.get('from') else None,
                        'receiver': tx.get('to', '').lower() if tx.get('to') else None,
                        'function_selector': tx.get('input', '')[:10] if tx.get('input') and len(tx.get('input', '')) >= 10 else None,
                        'gas_used': None,  # Not available from eth_getBlockByNumber
                        'gas_limit': int(tx.get('gas', '0x0'), 16) if tx.get('gas') else None,
                        'gas_price': int(tx.get('gasPrice', '0x0'), 16) if tx.get('gasPrice') else None,
                        'tx_type': int(tx.get('type', '0x0'), 16) if tx.get('type') else 0,
                        'receiver_is_eoa': None,
                        'receiver_is_contract': None,
                        'simple_transfer': None,
                        'from_label': None,
                        'from_category': None,
                        'to_label': None,
                        'to_category': None
                    }

                    tx_batch.append(tx_record)

                except Exception as e:
                    print(f"  Error parsing tx in block {block_number}: {e}")
                    block_tx_errors += 1
                    continue

            # Only mark the block processed if every tx parsed, so a re-run retries it
            if block_tx_errors == 0:
                processed_blocks_batch.append((block_number,))
            else:
                tx_parse_errors += block_tx_errors
                blocks_with_tx_errors += 1

        if len(tx_batch) >= batch_size or i == total:
            if tx_batch:
                tx_con.executemany("""
                    INSERT OR IGNORE INTO transactions VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, [(
                    r['tx_hash'], r['block_number'], r['tx_index'], r['sender'], r['receiver'],
                    r['function_selector'], r['gas_used'], r['gas_limit'], r['gas_price'],
                    r['tx_type'], r['receiver_is_eoa'], r['receiver_is_contract'], r['simple_transfer'],
                    r['from_label'], r['from_category'], r['to_label'], r['to_category']
                ) for r in tx_batch])

                tx_batch = []

            if processed_blocks_batch:
                tx_con.executemany("""
                    INSERT OR IGNORE INTO processed_blocks VALUES (?)
                """, processed_blocks_batch)

                processed_blocks_batch = []

            if block_meta_batch:
                block_con.executemany("""
                    INSERT OR IGNORE INTO block_metadata VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, [(
                    r['block_number'], r['timestamp'], r['base_fee_per_gas'],
                    r['gas_used'], r['gas_limit'], r['miner'], r['difficulty'],
                    r['total_difficulty'], r['size'], r['extra_data'], r['fetched_at']
                ) for r in block_meta_batch])

                block_meta_batch = []

        if i % 10 == 0 or i == total:
            success_rate = (fetched_blocks / i) * 100 if i > 0 else 0
            avg_txs_per_block = fetched_txs / fetched_blocks if fetched_blocks > 0 else 0

            elapsed_time = time.time() - start_time
            blocks_per_sec = i / elapsed_time if elapsed_time > 0 else 0
            remaining_blocks = total - i
            eta_seconds = remaining_blocks / blocks_per_sec if blocks_per_sec > 0 else 0

            if eta_seconds < 60:
                eta_str = f"{eta_seconds:.0f}s"
            elif eta_seconds < 3600:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds/3600:.1f}h"

            print(f"  Progress: {i}/{total} blocks ({i/total*100:.1f}%) | "
                  f"Success: {fetched_blocks} ({success_rate:.1f}%) | "
                  f"Txs: {fetched_txs:,} (avg {avg_txs_per_block:.1f}/block) | "
                  f"Errors: {rpc.error_count} | ETA: {eta_str}")

    tx_con.close()
    block_con.close()

    print(f"\n{chain.upper()} Summary:")
    print(f"  Total block requests: {rpc.request_count}")
    print(f"  Successful blocks: {fetched_blocks}")
    print(f"  Block metadata records: {fetched_block_meta}")
    print(f"  Total transactions: {fetched_txs}")
    print(f"  Errors: {rpc.error_count}")
    if tx_parse_errors > 0:
        print(f"  WARNING: {tx_parse_errors} transactions failed to parse across "
              f"{blocks_with_tx_errors} blocks; those blocks were left unmarked and will be retried on re-run")

    missing_blocks = total - fetched_blocks
    missing_block_metadata = fetched_blocks - fetched_block_meta
    if missing_blocks or missing_block_metadata or tx_parse_errors:
        raise RuntimeError(
            f"metadata collection incomplete for {chain}: "
            f"{missing_blocks} blocks not fetched, {missing_block_metadata} block records "
            f"not stored, {tx_parse_errors} transaction parse errors; rerun to retry"
        )
def print_statistics(chains):
    """Print database statistics."""
    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)

    for chain in chains:
        tx_db_path = get_tx_db_path(chain)
        block_db_path = get_block_db_path(chain)

        if not os.path.exists(tx_db_path):
            continue

        # Transaction stats
        tx_con = duckdb.connect(tx_db_path, read_only=True)

        result = tx_con.execute("""
            SELECT
                COUNT(*) as total_txs,
                COUNT(DISTINCT block_number) as total_blocks
            FROM transactions
        """).fetchone()

        total_txs, total_blocks = result
        avg_txs = total_txs / total_blocks if total_blocks > 0 else 0

        print(f"\n{chain.upper()} TRANSACTIONS:")
        print(f"  Database: {tx_db_path}")
        print(f"  Total transactions: {total_txs:,}")
        print(f"  Total blocks: {total_blocks:,}")
        print(f"  Avg txs/block: {avg_txs:.1f}")

        # Address statistics
        result = tx_con.execute("""
            SELECT
                COUNT(DISTINCT sender) as unique_from,
                COUNT(DISTINCT receiver) as unique_to
            FROM transactions
        """).fetchone()

        print(f"  Unique senders: {result[0]:,}")
        print(f"  Unique receivers: {result[1]:,}")

        tx_con.close()

        # Block metadata stats
        if os.path.exists(block_db_path):
            block_con = duckdb.connect(block_db_path, read_only=True)

            print(f"\n{chain.upper()} BLOCK METADATA:")
            print(f"  Database: {block_db_path}")

            try:
                result = block_con.execute("""
                    SELECT
                        COUNT(*) as total_blocks,
                        MIN(timestamp) as min_timestamp,
                        MAX(timestamp) as max_timestamp,
                        AVG(gas_used) as avg_gas_used,
                        AVG(base_fee_per_gas) as avg_base_fee
                    FROM block_metadata
                """).fetchone()

                if result and result[0] > 0:
                    print(f"  Total blocks: {result[0]:,}")
                    print(f"  Timestamp range: {result[1]} to {result[2]}")
                    print(f"  Avg gas used: {result[3]:,.0f}" if result[3] else "  Avg gas used: N/A")
                    print(f"  Avg base fee: {result[4]:,.0f} wei" if result[4] else "  Avg base fee: N/A")
            except Exception as e:
                print(f"  Error: {e}")

            block_con.close()

    print("="*60)

def main():
    print("Transaction & Block Metadata Fetcher")
    print("="*60)

    if len(sys.argv) > 1:
        chains = [sys.argv[1].lower()]
        print(f"Processing only: {chains[0]}")
    else:
        chains = ['ethereum', 'base']
        print("Processing both chains (use 'python fetch_tx_metadata.py base' or 'ethereum' to run single chain)")

    for chain in chains:
        chain_path = os.path.join(DATA_ROOT, chain)

        if not os.path.exists(chain_path):
            raise RuntimeError(f"opcode directory not found for {chain}: {chain_path}")

        print(f"\n{'='*60}")
        print(f"Processing {chain.upper()}")
        print('='*60)

        setup_tx_database(chain)
        setup_block_database(chain)

        block_numbers = extract_block_numbers_from_parquet(chain)

        if not block_numbers:
            raise RuntimeError(f"no opcode blocks found for {chain}")

        fetch_and_store_metadata(chain, block_numbers)

    print_statistics(chains)

    print(f"\nDatabases created at:")
    print(f"  Transaction metadata: {DB_DIR}/tx_metadata_{{chain}}.duckdb")
    print(f"  Block metadata: {DB_DIR}/block_metadata_{{chain}}.duckdb")

if __name__ == "__main__":
    main()
