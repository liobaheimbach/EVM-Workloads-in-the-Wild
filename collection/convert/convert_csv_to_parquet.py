#!/usr/bin/env python3
"""
Convert per-block opcode gas CSV files to Parquet format.
Uses date-based partitioning for efficient storage and querying.
"""

import csv
import pandas as pd
import pyarrow.csv as pa_csv
from pathlib import Path
import argparse
import re
import json
import requests
import gc
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

# Columns where a blank means "could not be determined", not zero. The exact
# account classifier emits nothing when a prestate fallback or a missing state
# diff leaves existence unestablished, so these stay NULL through to Parquet.
NULLABLE_NUMERIC_COLUMNS = {'account_births', 'account_deaths'}


def get_block_number_from_filename(filename: str) -> int:
    """Extract block number from filename like 'block_32214921_opcode_gas.csv' or 'block_32214921_opcode_breakdown.csv'"""
    match = re.search(r'block_(\d+)_opcode_\w+\.csv', filename)
    if match:
        return int(match.group(1))
    return 0


def load_rpc_config() -> dict:
    """Load RPC configuration from rpc_config.json"""
    config_path = Path(__file__).parent / 'rpc_config.json'
    with open(config_path, 'r') as f:
        return json.load(f)


def fetch_timestamp_batch_worker(args):
    """Worker function to fetch a single batch of timestamps."""
    rpc_url, batch, batch_idx = args

    requests_list = []
    for idx, block_num in enumerate(batch):
        requests_list.append({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_num), False],
            "id": idx
        })

    try:
        response = requests.post(rpc_url, json=requests_list, timeout=30)
        response.raise_for_status()
        results = response.json()

        if not isinstance(results, list):
            return {}, f"non-batch JSON-RPC response: {results}"

        timestamps = {}
        errors = []
        for result in results:
            if not isinstance(result, dict):
                errors.append(f"unexpected batch entry: {result!r}")
                continue
            if result.get('error'):
                errors.append(str(result['error']))
                continue
            if result.get('result'):
                block_data = result['result']
                block_num = int(block_data['number'], 16)
                timestamp = int(block_data['timestamp'], 16)
                date_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d')
                timestamps[block_num] = {
                    'date': date_str,
                    'timestamp': timestamp
                }
        return timestamps, ('; '.join(errors) if errors else None)
    except Exception as e:
        return {}, str(e)


def get_block_timestamps_batch(rpc_url: str, block_numbers: list[int], batch_size: int = 100, max_workers: int = 8) -> dict[int, dict[str, str | int]]:
    """
    Fetch timestamps for multiple blocks using parallel batch RPC calls.

    Returns dict mapping block_number -> {'date': 'YYYY-MM-DD', 'timestamp': unix_timestamp}
    """
    # Create batches
    batches = []
    for i in range(0, len(block_numbers), batch_size):
        batch = block_numbers[i:i + batch_size]
        batches.append((rpc_url, batch, i // batch_size))

    total_batches = len(batches)
    print(f"  Fetching timestamps: {len(block_numbers)} blocks in {total_batches} batches (parallel workers: {max_workers})")

    timestamps = {}
    completed = 0

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_timestamp_batch_worker, batch) for batch in batches]

        for future in as_completed(futures):
            batch_timestamps, error = future.result()
            timestamps.update(batch_timestamps)
            completed += 1

            if error:
                print(f"  Error fetching batch: {error}")

            if completed % 10 == 0 or completed == total_batches:
                print(f"  Progress: {completed}/{total_batches} batches ({len(timestamps)} blocks done)")

    return timestamps


def load_timestamp_cache(cache_file: Path) -> dict[int, dict[str, str | int]]:
    """Load cached timestamps from JSON file."""
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            # JSON keys are strings, convert back to int
            return {int(k): v for k, v in json.load(f).items()}
    return {}


def save_timestamp_cache(cache_file: Path, timestamps: dict[int, dict[str, str | int]]):
    """Save timestamps to JSON cache file."""
    with open(cache_file, 'w') as f:
        json.dump(timestamps, f)


def expand_date_selectors(selectors: list[str] | None) -> set[str] | None:
    """Expand YYYY-MM-DD and START..END date selectors into a date set."""
    if not selectors:
        return None

    dates: set[str] = set()
    for selector in selectors:
        if '..' in selector:
            start_s, end_s = selector.split('..', 1)
            start = datetime.strptime(start_s, '%Y-%m-%d').date()
            end = datetime.strptime(end_s, '%Y-%m-%d').date()
            if end < start:
                raise ValueError(f"Invalid date range '{selector}': end is before start")
            day = start
            while day <= end:
                dates.add(day.isoformat())
                day += timedelta(days=1)
        else:
            day = datetime.strptime(selector, '%Y-%m-%d').date()
            dates.add(day.isoformat())
    return dates


def collect_all_columns(csv_files: list[Path]) -> set[str]:
    """
    Collect all unique column names by reading only the header line of every
    CSV file, so all parquet files share a consistent schema.

    Args:
        csv_files: List of CSV file paths

    Returns:
        Set of all unique column names found
    """
    all_columns = set()
    print(f"  Collecting schema from {len(csv_files)} files...")

    for i, csv_file in enumerate(csv_files):
        try:
            # Parse quoted headers exactly as the CSV data reader does; splitting
            # the raw line would retain quotes and create spurious columns.
            with open(csv_file, 'r', newline='') as f:
                columns = next(csv.reader(f), [])
                all_columns.update(c.strip() for c in columns if c.strip())

            if (i + 1) % 1000 == 0 or (i + 1) == len(csv_files):
                print(f"    Scanned {i + 1}/{len(csv_files)} files, found {len(all_columns)} columns", flush=True)
        except Exception as e:
            print(f"  Error reading header from {csv_file}: {e}")
            continue

    print(f"  Found {len(all_columns)} unique columns across all files")
    return all_columns


def standardize_dataframe_schema(df: pd.DataFrame, expected_columns: set[str], priority_cols: list[str]) -> tuple[pd.DataFrame, int]:
    """
    Ensure dataframe has all expected columns with correct types.
    Missing columns are added with default values (0 for numeric, '' for string).

    Args:
        df: Input dataframe
        expected_columns: Set of all columns that should exist
        priority_cols: List of priority columns to put first

    Returns:
        Tuple of (dataframe with standardized schema, number of non-numeric
        values coerced to 0 in numeric columns)
    """
    # Add missing columns with appropriate defaults
    for col in expected_columns:
        if col not in df.columns:
            if col in ['tx_hash', 'error', 'chain', 'date']:
                df[col] = ''
            elif col == 'success':
                df[col] = False
            elif col == 'timestamp':
                df[col] = 0
            elif col in NULLABLE_NUMERIC_COLUMNS:
                # Absent from older CSVs means "never measured", not "zero".
                df[col] = pd.NA
            else:
                df[col] = 0

    # Convert to proper types
    coerced_count = 0
    for col in df.columns:
        if col in ['tx_hash', 'error', 'chain', 'date']:
            df[col] = df[col].fillna('').astype(str)
        elif col == 'success':
            df[col] = df[col].fillna(False).astype(bool)
        else:
            # Numeric columns
            was_na = df[col].isna()
            converted = pd.to_numeric(df[col], errors='coerce')
            coerced_count += int((converted.isna() & ~was_na).sum())
            if col in NULLABLE_NUMERIC_COLUMNS:
                # A blank here means the value could not be determined. Filling it
                # with 0 would assert "no births", which is a different claim.
                df[col] = converted.astype('Int64')
            else:
                df[col] = converted.fillna(0)

    # Reorder columns: priority columns first, then alphabetically
    other_cols = sorted([c for c in df.columns if c not in priority_cols])
    df = df[priority_cols + other_cols]

    return df, coerced_count

def convert_chain_csvs_to_parquet(
    input_dir: Path,
    output_dir: Path,
    chain_name: str = None,
    append_mode: bool = True,
    rpc_url: str = None,
    only_dates: set[str] | None = None,
    max_block: int | None = None,
    strict: bool = False,
    replace_existing: bool = False
):
    """
    Convert CSV files to Parquet with date-based partitioning.

    Fetches block timestamps from RPC and groups by date for efficient
    storage and querying.

    Args:
        input_dir: Directory containing CSV files (e.g., ethereum/)
        output_dir: Directory for output Parquet files
        chain_name: Chain name to add as column (extracted from dir name if None)
        append_mode: If True, merge with existing parquet files (default: True)
        rpc_url: RPC URL for fetching block timestamps
        only_dates: Optional YYYY-MM-DD date partitions to rebuild
    """
    only_dates = set(only_dates or [])

    if not input_dir.exists():
        raise RuntimeError(f"input directory not found: {input_dir}")

    # Extract chain name from directory if not provided
    if chain_name is None:
        dir_name = input_dir.name
        # Use directory name as-is (e.g., 'ethereum', 'base')
        chain_name = dir_name

    # Get RPC URL from config if not provided
    if rpc_url is None:
        config = load_rpc_config()
        if chain_name not in config:
            raise KeyError(
                f"Chain '{chain_name}' not found in rpc_config.json "
                f"(available chains: {sorted(config)})"
            )
        rpc_url = config[chain_name].get('rpc_url')

        if not rpc_url:
            raise RuntimeError(f"no RPC URL found for chain '{chain_name}' in rpc_config.json")

    # Find all CSV files (both opcode_gas and opcode_breakdown patterns)
    csv_files = list(input_dir.glob('block_*_opcode_*.csv'))

    # The glob matches both naming patterns, so a block collected under both
    # would be read twice and double-counted in the parquet. Reject instead.
    by_block = defaultdict(list)
    for p in csv_files:
        m = re.match(r'block_(\d+)_opcode_', p.name)
        if m:
            by_block[int(m.group(1))].append(p.name)
    dupes = {b: sorted(n) for b, n in by_block.items() if len(n) > 1}
    if dupes:
        sample = list(dupes.items())[:5]
        detail = '; '.join(f"block {b}: {', '.join(n)}" for b, n in sample)
        raise RuntimeError(
            f"{len(dupes)} block(s) have multiple opcode CSVs in {input_dir} "
            f"({detail}). Remove the stale variant before converting."
        )

    if not csv_files:
        if strict:
            raise RuntimeError(f"no CSV files found in {input_dir}")
        return

    print(f"Found {len(csv_files)} CSV files for chain '{chain_name}'")

    # Apply the max_block cutoff before schema collection so only in-scope files are read.
    if max_block is not None:
        before = len(csv_files)
        csv_files = [f for f in csv_files
                     if get_block_number_from_filename(f.name) <= max_block]
        print(f"  max_block cutoff {max_block:,}: kept {len(csv_files):,} of {before:,} CSVs "
              f"(skipped {before - len(csv_files):,} over-cutoff)")
        if not csv_files:
            if strict:
                raise RuntimeError(f"no CSV files <= max_block {max_block} for chain '{chain_name}'")
            return

    # Collect all possible columns for schema consistency
    # (scans in-scope files, headers only, so it's fast)
    all_csv_columns = collect_all_columns(csv_files)
    # Add our metadata columns
    all_csv_columns.update(['block_number', 'chain', 'date', 'timestamp'])

    # Get all block numbers - store as strings to reduce memory.
    block_to_file = {}
    for csv_file in csv_files:
        block_num = get_block_number_from_filename(csv_file.name)
        block_to_file[block_num] = str(csv_file)

    # Free csv_files list
    del csv_files
    gc.collect()

    block_numbers = sorted(block_to_file.keys())

    # Load cached timestamps
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_file = output_dir / f"{chain_name}_timestamps.json"
    cached_timestamps = load_timestamp_cache(cache_file)
    print(f"Loaded {len(cached_timestamps)} cached timestamps")

    # Find blocks that need timestamps
    missing_blocks = [b for b in block_numbers if b not in cached_timestamps]

    if missing_blocks:
        print(f"Fetching timestamps for {len(missing_blocks)} new blocks...")
        new_timestamps = get_block_timestamps_batch(rpc_url, missing_blocks)
        cached_timestamps.update(new_timestamps)

        # Save updated cache
        save_timestamp_cache(cache_file, cached_timestamps)
        print(f"Saved {len(cached_timestamps)} timestamps to cache")
    else:
        print("All timestamps found in cache")

    # Use only timestamps for our blocks
    timestamps = {b: cached_timestamps[b] for b in block_numbers if b in cached_timestamps}

    if not timestamps:
        if strict:
            raise RuntimeError("failed to get any block timestamps")
        return

    print(f"Got timestamps for {len(timestamps)} blocks")

    # Group blocks by date
    date_groups = defaultdict(list)
    blocks_missing_timestamp = 0
    for block_num in block_numbers:
        if block_num in timestamps:
            date_str = timestamps[block_num]['date']
            date_groups[date_str].append(block_num)
        else:
            blocks_missing_timestamp += 1
            print(f"  Warning: No timestamp for block {block_num}")

    print(f"Grouped into {len(date_groups)} dates")

    if only_dates:
        available_dates = set(date_groups)
        requested_dates = sorted(only_dates)
        filtered_date_groups = defaultdict(list)
        for date_str, blocks in date_groups.items():
            if date_str in only_dates:
                filtered_date_groups[date_str] = blocks
        date_groups = filtered_date_groups
        missing_requested = sorted(set(requested_dates) - available_dates)
        print(f"Filtered to {len(date_groups)} requested dates ({requested_dates[0]}..{requested_dates[-1]})")
        if missing_requested:
            print(f"  Requested dates with no blocks: {', '.join(missing_requested)}")
            if strict:
                raise RuntimeError(f"requested dates have no blocks: {', '.join(missing_requested)}")
        if not date_groups:
            if strict:
                raise RuntimeError("no blocks matched --only-dates")
            return

    # Create output directory for this chain
    chain_output_dir = output_dir / chain_name
    chain_output_dir.mkdir(parents=True, exist_ok=True)

    # Process each date one at a time to avoid memory issues
    total_rows = 0
    csv_read_failures = 0
    coerced_values_total = 0
    files_read = 0
    sorted_dates = sorted(date_groups.keys())

    for date_idx, date_str in enumerate(sorted_dates):
        blocks = date_groups[date_str]

        print(f"\n[{date_idx + 1}/{len(sorted_dates)}] Processing {date_str} ({len(blocks)} blocks)...", flush=True)

        # Avoid reading a day's CSVs when append output already covers every block.
        if append_mode:
            _existing = chain_output_dir / f"date={date_str}" / "data.parquet"
            if _existing.exists():
                try:
                    _have = set(pd.read_parquet(_existing, columns=['block_number'])['block_number'].unique())
                    if set(blocks).issubset(_have):
                        print(f"  [early-skip] all {len(blocks)} blocks already in parquet for {date_str}", flush=True)
                        continue
                except Exception as _e:
                    print(f"  [early-skip] could not read existing parquet ({_e}); falling through to full read", flush=True)

        # Read and concatenate all CSVs for this date
        sorted_blocks = sorted(blocks)

        # Read CSVs in smaller batches to avoid memory issues
        batch_size = 50

        all_dfs = []

        for batch_start in range(0, len(sorted_blocks), batch_size):
            batch_end = min(batch_start + batch_size, len(sorted_blocks))
            batch_blocks = sorted_blocks[batch_start:batch_end]

            batch_dfs = []
            for block_num in batch_blocks:
                csv_file = block_to_file[block_num]
                try:
                    # Print every 50th file to track progress
                    files_read += 1
                    if files_read % 50 == 0:
                        print(f"    Reading block {block_num}...", flush=True)

                    # Use PyArrow for more robust CSV reading (avoids pandas segfaults)
                    # Falls back to pandas if PyArrow fails
                    try:
                        table = pa_csv.read_csv(csv_file, parse_options=pa_csv.ParseOptions(ignore_empty_lines=True))
                        df = table.to_pandas()
                    except Exception:
                        # engine='python' does NOT support low_memory -> omit it
                        df = pd.read_csv(csv_file, na_values=['', 'NA', 'null'], keep_default_na=True, engine='python')
                    df['block_number'] = block_num
                    df['chain'] = chain_name
                    df['date'] = date_str
                    df['timestamp'] = timestamps[block_num]['timestamp']
                    batch_dfs.append(df)
                except Exception as e:
                    csv_read_failures += 1
                    print(f"  Error reading {csv_file}: {e}", flush=True)
                    continue

            if batch_dfs:
                batch_df = pd.concat(batch_dfs, ignore_index=True, sort=False)
                all_dfs.append(batch_df)
                del batch_dfs
                gc.collect()

            # Show progress
            if (batch_end % 200 == 0) or batch_end == len(sorted_blocks):
                print(f"  Reading CSVs: {batch_end}/{len(sorted_blocks)}", flush=True)

        if not all_dfs:
            print(f"  No valid data for {date_str}")
            continue

        # Final concat of batches - do it in chunks if we have many dfs
        print(f"  Concatenating {len(all_dfs)} batches...", flush=True)

        if len(all_dfs) > 10:
            # Concat in stages to avoid issues
            chunk_size = 5
            chunked_dfs = []
            for i in range(0, len(all_dfs), chunk_size):
                print(f"    Concat chunk {i//chunk_size + 1}/{(len(all_dfs) + chunk_size - 1)//chunk_size}", flush=True)
                chunk = all_dfs[i:i+chunk_size]
                chunked_df = pd.concat(chunk, ignore_index=True, sort=False)
                chunked_dfs.append(chunked_df)
                del chunk
                gc.collect()
            print(f"    Final concat of {len(chunked_dfs)} chunks...", flush=True)
            combined_df = pd.concat(chunked_dfs, ignore_index=True, sort=False)
            del chunked_dfs
        else:
            combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)

        del all_dfs
        gc.collect()
        print(f"  Concat complete: {len(combined_df)} rows", flush=True)

        # Standardize schema to include all possible columns
        print(f"  Standardizing schema...", flush=True)
        priority_cols = ['block_number', 'chain', 'date', 'timestamp']
        combined_df, n_coerced = standardize_dataframe_schema(combined_df, all_csv_columns, priority_cols)
        coerced_values_total += n_coerced
        print(f"  Schema standardization complete", flush=True)

        # Output file path (date-based)
        date_dir = chain_output_dir / f"date={date_str}"
        date_dir.mkdir(parents=True, exist_ok=True)
        output_file = date_dir / "data.parquet"
        rows_added = len(combined_df)

        # If append mode and file exists, merge with existing data
        if append_mode and output_file.exists():
            existing_df = pd.read_parquet(output_file)

            # Get existing block numbers to avoid duplicates
            existing_blocks = set(existing_df['block_number'].unique())
            new_blocks = set(combined_df['block_number'].unique())

            # Blocks present in both are kept from the parquet, so a block that
            # was RE-collected (e.g. after a collector fix) would keep its stale
            # rows and the new CSV would be discarded without a word.
            resupplied = new_blocks & existing_blocks
            if resupplied:
                if replace_existing:
                    existing_df = existing_df[~existing_df['block_number'].isin(resupplied)]
                    existing_blocks -= resupplied
                    print(f"  Replacing {len(resupplied)} recollected block(s) for {date_str}")
                elif strict:
                    raise RuntimeError(
                        f"{len(resupplied)} block(s) for {date_str} are already in the parquet "
                        f"but were supplied again; pass --replace-existing to overwrite them "
                        f"or remove the CSVs to keep the stored rows"
                    )
                else:
                    print(f"  WARNING: keeping stored rows for {len(resupplied)} resupplied "
                          f"block(s) for {date_str}; pass --replace-existing to overwrite")

            # Filter out blocks that already exist
            new_only_df = combined_df[~combined_df['block_number'].isin(existing_blocks)]

            if len(new_only_df) == 0:
                print(f"  All {len(new_blocks)} blocks already exist for {date_str}, skipping")
                del combined_df, existing_df
                gc.collect()
                continue

            # Append new data - standardize both dataframes first to ensure same schema
            priority_cols = ['block_number', 'chain', 'date', 'timestamp']
            existing_df, _ = standardize_dataframe_schema(existing_df, all_csv_columns, priority_cols)
            new_only_df, n_coerced = standardize_dataframe_schema(new_only_df, all_csv_columns, priority_cols)
            coerced_values_total += n_coerced

            rows_added = len(new_only_df)
            existing_only_cols = set(existing_df.columns) - set(new_only_df.columns)
            combined_df = pd.concat([existing_df, new_only_df], ignore_index=True, sort=False)
            del existing_df, new_only_df

            # Columns present in the existing parquet but absent from this CSV batch
            # follow the same NULL-vs-zero rules as standardize_dataframe_schema.
            for col in existing_only_cols:
                if col in ['tx_hash', 'error', 'chain', 'date']:
                    combined_df[col] = combined_df[col].fillna('').astype(str)
                elif col == 'success':
                    combined_df[col] = combined_df[col].fillna(False).astype(bool)
                elif col in NULLABLE_NUMERIC_COLUMNS:
                    combined_df[col] = combined_df[col].astype('Int64')
                else:
                    combined_df[col] = combined_df[col].fillna(0)

            combined_df = combined_df.sort_values('block_number')

            added_blocks = len(new_blocks - existing_blocks)
            print(f"  Appending {added_blocks} new blocks to existing {len(existing_blocks)} blocks")

        total_rows += rows_added

        # Write to Parquet
        print(f"  Saving parquet...", end='\r', flush=True)
        combined_df.to_parquet(output_file, index=False, compression='snappy')

        # Get file size
        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  Wrote {len(combined_df):,} rows to date={date_str}/ ({size_mb:.2f} MB)")

        # Clear memory after each date
        del combined_df
        gc.collect()

    print(f"\nTotal: {total_rows:,} rows converted for chain '{chain_name}'")
    if csv_read_failures or blocks_missing_timestamp or coerced_values_total:
        msg = (f"data dropped/coerced for chain '{chain_name}': "
               f"{csv_read_failures} CSV file(s) failed to read, "
               f"{blocks_missing_timestamp} block(s) dropped for missing timestamps, "
               f"{coerced_values_total} non-numeric value(s) coerced to 0")
        if strict:
            raise RuntimeError(f"STRICT ABORT: {msg}")
        print(f"WARNING: {msg}")
    else:
        print(f"Data quality: 0 CSV read failures, 0 blocks dropped for missing timestamps, 0 values coerced")

def main():
    load_dotenv()
    output_base_dir = os.getenv('OUTPUT_BASE_DIR')

    parser = argparse.ArgumentParser(description='Convert per-block CSV files to Parquet with date-based partitioning')
    parser.add_argument('--input-base', default=None,
                        help='Base directory containing block_opcode_gas_* folders '
                             '(default: $OUTPUT_BASE_DIR/opcode_breakdown)')
    parser.add_argument('--output-dir', default=None,
                        help='Output directory for Parquet files '
                             '(default: $OUTPUT_BASE_DIR/evm_workload_analysis_data/opcode_breakdown)')
    parser.add_argument('--chains', nargs='+',
                        help='Specific chains to convert (default: all)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing parquet files instead of appending')
    parser.add_argument('--only-dates', nargs='+', default=None, metavar='DATE',
                        help='Only convert selected date partitions; accepts YYYY-MM-DD or START..END')
    parser.add_argument('--max-block', type=int, default=None,
                        help='Hard cutoff: never ingest a block beyond this (keeps post-study-period '
                             'data out of parquet). Base=40218106, Ethereum=24136052')
    parser.add_argument('--strict', action='store_true',
                        help='Fail hard (nonzero exit) on any CSV read failure, missing timestamp, '
                             'or non-numeric coercion instead of warning. Use for production runs.')
    parser.add_argument('--replace-existing', action='store_true',
                        help='In append mode, overwrite stored rows for blocks that are supplied '
                             'again (use after recollecting a block). Default keeps the stored rows.')

    args = parser.parse_args()
    try:
        only_dates = expand_date_selectors(args.only_dates)
    except ValueError as e:
        parser.error(str(e))

    # OUTPUT_BASE_DIR is only needed to derive defaults for arguments not given
    if args.input_base is None or args.output_dir is None:
        if not output_base_dir:
            parser.error("OUTPUT_BASE_DIR is required when --input-base/--output-dir are not both given")
        if args.input_base is None:
            args.input_base = os.path.join(output_base_dir, 'opcode_breakdown')
        if args.output_dir is None:
            args.output_dir = os.path.join(output_base_dir, 'evm_workload_analysis_data', 'opcode_breakdown')

    input_base = Path(args.input_base)
    output_dir = Path(args.output_dir)

    print(f"Input base: {input_base}")
    print(f"Output dir: {output_dir}")
    print("Partitioning: date-based")
    if only_dates:
        print(f"Only dates: {min(only_dates)}..{max(only_dates)} ({len(only_dates)} date(s))")
    print("=" * 60)

    # Find all chain directories
    if args.chains:
        chain_dirs = [input_base / chain for chain in args.chains]
        missing_chain_dirs = [str(path) for path in chain_dirs if not path.is_dir()]
        if missing_chain_dirs:
            raise RuntimeError(f"requested chain directories not found: {', '.join(missing_chain_dirs)}")
    else:
        # Find all subdirectories in input_base that contain CSV files
        chain_dirs = [d for d in input_base.iterdir() if d.is_dir()]

    if not chain_dirs:
        raise RuntimeError("no chain directories found")

    print(f"Found {len(chain_dirs)} chain directories")

    # Convert each chain
    for chain_dir in sorted(chain_dirs):
        if not chain_dir.is_dir():
            continue

        print(f"\n{'=' * 60}")
        print(f"Converting: {chain_dir.name}")
        print("=" * 60)

        convert_chain_csvs_to_parquet(
            input_dir=chain_dir,
            output_dir=output_dir,
            append_mode=not args.overwrite,
            only_dates=only_dates,
            max_block=args.max_block,
            strict=args.strict,
            replace_existing=args.replace_existing
        )

    # Print summary
    print(f"\n{'=' * 60}")
    print("CONVERSION COMPLETE")
    print("=" * 60)

    # Show output directory size
    if output_dir.exists():
        parquet_files = list(output_dir.glob('**/data.parquet'))
        total_size = sum(f.stat().st_size for f in parquet_files)
        print(f"Total Parquet files: {len(parquet_files)}")
        print(f"Total size: {total_size / (1024**3):.2f} GB")

if __name__ == '__main__':
    main()
