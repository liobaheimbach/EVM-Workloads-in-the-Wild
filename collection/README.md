# EVM Workload Analysis

Data collection pipeline for opcode-level gas workload analysis on Ethereum and Base. Produces per-opcode gas breakdowns, transaction metadata, contract labels, and sensitivity measurements (storage slot, opcode trace, and `eth_estimateGas`) across historical blockchain states.

## Repository layout

```
├── opcode_breakdown/      Per-tx opcode gas via debug_traceTransaction (structLogs)
├── opcode_sensitivity/    Same trace at multiple lookback blocks (state sensitivity)
├── slot_sensitivity/      Storage slot pre/post-state via prestateTracer at lookbacks
├── estimate_sensitivity/  eth_estimateGas at multiple historical states
├── metadata/              Per-block + per-tx metadata (timestamps, gas, type)
├── labels/                Address labeling (Spellbook, DefiLlama, Kleros, scans, MEV)
├── convert/               CSV → Parquet partitioning
└── utils/                 Shared helpers (subprocess runner, Kleros label conversion, DuckDB label helpers, runner orchestration)
```

Each subdirectory contains its own `README.md` with usage details.

## Pipeline order

1. **opcode_breakdown** — collect per-opcode gas for sampled transactions (CSV per block).
2. **convert** — partition CSVs into date-keyed Parquet.
3. **metadata** — fetch tx and block metadata for the sampled blocks into DuckDB.
4. **labels** — apply address labels and categories to the metadata DuckDB.
5. **opcode_sensitivity / slot_sensitivity / estimate_sensitivity** — re-run traces / estimates at historical states.

## Requirements

- Python 3.12+
- An archive Ethereum node and an archive Base node, both with `debug_*` namespace enabled (Reth is what we used)
- DuckDB
- Python packages: `web3`, `requests`, `aiohttp`, `duckdb`, `pandas`, `pyarrow`, `python-dotenv`, `beautifulsoup4`

## Configuration

Each pipeline directory contains:

- `rpc_config.json` — RPC endpoints per chain. Replace `YOUR_*_RPC_URL` placeholders with archive-node URLs that expose the `debug` namespace.
- `.env.example` — copy to `.env` and set the directory's variables: most pipelines use `RAW_BASE_DIR` (CSV/parquet root) and/or `DB_BASE_DIR` (DuckDB root); `convert/` instead uses `OUTPUT_BASE_DIR` (base for its default input/output paths).

API keys (Etherscan, Basescan) are read from environment variables — see the relevant subdirectory README.

## Reproducing the paper dataset

The runner scripts in `slot_sensitivity/`, `estimate_sensitivity/`, and `opcode_sensitivity/` (`run_{chain}_september*.py`) reproduce the September 2025 measurement runs used in the submission. Each runner reads a block list, queries the configured archive node, and writes a DuckDB or Parquet output.

## Notes

- All RPC URLs in the repo are placeholders. You must supply your own archive nodes.
