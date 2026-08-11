#!/usr/bin/env python3
"""
Run state sensitivity collection for Ethereum blocks from September 2025 only.
Reads blocks from ethereum_blocks_summary.csv and filters for September 2025.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from utils.shared import run_september_collection

load_dotenv()
RAW_BASE_DIR = os.getenv('RAW_BASE_DIR')
DB_BASE_DIR = os.getenv('DB_BASE_DIR')

if not RAW_BASE_DIR:
    print("ERROR: RAW_BASE_DIR not set in .env file")
    sys.exit(1)

if not DB_BASE_DIR:
    print("ERROR: DB_BASE_DIR not set in .env file")
    sys.exit(1)

# Ethereum September 2025 (found via binary search on RPC timestamp)
# Sept 1, 2025 00:00:00 UTC = timestamp 1756684800
# Oct 1, 2025 00:00:00 UTC = timestamp 1759276800
SEPT_START_BLOCK = 23264566  # First block >= Sept 1 00:00 UTC
SEPT_END_BLOCK = 23479243     # Last block < Oct 1 00:00 UTC (Sep 30 23:59:59 UTC)

CSV_FILE = Path(RAW_BASE_DIR) / 'opcode_breakdown' / 'ethereum_blocks_summary.csv'
OUTPUT_DB = Path(DB_BASE_DIR) / 'estimate_sensitivity' / 'state_sensitivity_analysis_ethereum_september_2025.duckdb'

run_september_collection(
    title="ETHEREUM SEPTEMBER 2025 STATE SENSITIVITY COLLECTION",
    chain='ethereum',
    csv_file=CSV_FILE,
    start_block=SEPT_START_BLOCK,
    end_block=SEPT_END_BLOCK,
    output_db=OUTPUT_DB,
    collection_script=str(Path(__file__).parent / 'collect_from_block_list.py'),
)
