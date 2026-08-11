# CSV to Parquet Converter

## Prerequisites

- `rpc_config.json` in this directory (used to fetch block timestamps for date partitioning). Replace the `YOUR_*_RPC_URL` placeholders with real archive-node URLs. Chains are looked up by exact key (e.g. `ethereum`, `base`).
- `OUTPUT_BASE_DIR` (environment variable, or `.env` — see `.env.example`) is only required when `--input-base` or `--output-dir` is omitted; it supplies their defaults (`$OUTPUT_BASE_DIR/opcode_breakdown` and `$OUTPUT_BASE_DIR/evm_workload_analysis_data/opcode_breakdown`). If both flags are given, `OUTPUT_BASE_DIR` is not needed.

## Usage

```bash
python3 convert_csv_to_parquet.py \
  --input-base /path/to/opcode_breakdown_sensitive \
  --output-dir /path/to/opcode_sensitivity \
  --chains ethereum

python3 convert_csv_to_parquet.py \
  --input-base /path/to/opcode_breakdown \
  --output-dir /path/to/opcode_breakdown_out \
  --chains base
```

A per-chain summary line reports CSV read failures, blocks dropped for missing timestamps, and non-numeric values coerced to 0.
