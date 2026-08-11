# Transaction and Block Metadata

Fetches transaction and block metadata for all blocks referenced in the opcode breakdown parquet files.

## Usage

```bash
python fetch_tx_metadata.py           # both chains
python fetch_tx_metadata.py ethereum  # single chain
python fetch_tx_metadata.py base
```

Requires `DB_BASE_DIR` in `.env`, and `rpc_config.json` with RPC endpoints.

Reads parquet files from `$DB_BASE_DIR/opcode_breakdown/{chain}/**/data.parquet`.

## Output

- `$DB_BASE_DIR/tx_metadata_{chain}.duckdb` — `transactions` and `processed_blocks` tables
- `$DB_BASE_DIR/block_metadata_{chain}.duckdb` — `block_metadata` table

Incremental: re-running only fetches blocks not yet in the database.

The command exits nonzero if any requested block or transaction record is missing; rerun it to retry only incomplete blocks.

Note: `gas_used` per transaction is NULL (not available from `eth_getBlockByNumber`). Use `eth_getTransactionReceipt` if needed. Label columns (`from_label`, `to_label`, etc.) are populated separately by the labels workflow.
