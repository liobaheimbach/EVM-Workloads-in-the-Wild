# Slot Sensitivity (Prestate/Poststate)

Collects storage slot pre/post state for transactions at multiple lookback points using `debug_traceCall` with `prestateTracer` (diffMode).

Default lookbacks: `0, 5, 10, 20`

## Usage

```bash
# September 2025 runner scripts (require RAW_BASE_DIR and DB_BASE_DIR in .env)
python run_ethereum_september_prestate.py
python run_base_september_prestate.py

# Direct collection
python collect_prestate_poststate.py \
  --block-list /path/to/blocks.csv \
  --chain ethereum \
  --block-range 23264566-23479243 \
  --num-workers 16 --tx-parallelism 4 \
  --lookbacks 0,5,10,20 \
  --output-file /path/to/output.duckdb
```

## Output

DuckDB with a `prestate_poststate` table: `(tx_hash, block_number, lookback, state_type, address, slot, value)`

Deterministic historical-state rejections, such as a transaction fee cap below
the selected block's base fee, produce no rows for that lookback and are counted
as terminal rejections in `processed_blocks`. Transport and RPC failures remain
retryable: their block is not marked processed, the command exits nonzero, and a
rerun retries it.

Incremental: already-processed blocks are skipped on re-run.
