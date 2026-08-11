# Estimate Sensitivity

Measures how `eth_estimateGas` varies when called against historical states at different lookback distances (0–20 blocks back).

## Usage

```bash
# Date range mode
python collect_tx_analysis_integrated.py \
  --chain base \
  --start-date 2025-09-01 --end-date 2025-09-30 \
  --blocks-per-day 3000 \
  --num-workers 4 --tx-parallelism 4 \
  --sample-points 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 \
  --output-file output.duckdb

# From block list CSV
python collect_from_block_list.py \
  --block-list blocks.csv \
  --chain base \
  --block-range 34947727-36243726 \
  --num-workers 16 --tx-parallelism 4 \
  --output-file output.duckdb

# September 2025 runner scripts (require RAW_BASE_DIR and DB_BASE_DIR in .env)
python run_base_september.py
python run_ethereum_september.py
```

## Output

Two DuckDB files per run:
- `output.duckdb` — transactions with at least one successful lookback
- `output_errors.duckdb` — transactions where no lookback succeeded

Deterministic EVM/transaction-validation rejections are stored as failed lookbacks. Transport errors, malformed responses, missing receipts, and per-transaction exceptions fail the block and the command exits nonzero. If a block-list supplies `paper_tx_count`, resume skips only blocks whose combined main-and-error row count matches it; incomplete blocks are cleared and recollected.

Schema: one `gas_estimate_lookback_N` column per sample point, plus `gas_used`, `max_lookback_success`, `first_fail_block`, `first_fail_error`.
