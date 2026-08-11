# Opcode Sensitivity

Traces transactions at multiple historical state lookbacks and records per-opcode gas breakdown. Uses `debug_traceCall` with `structLogs`.

Lookback N means executing the transaction against the state at block `(original_block - 1 - N)`. Default points: `0, 5, 10, 20`.

## Usage

```bash
# From existing opcode breakdown CSVs
python state_opcode_breakdown.py \
  --from-source /path/to/opcode_breakdown/base \
  --start-block 34947727 --end-block 35868377 \
  --chain base --num-workers 16 --quiet

# Single transaction
python state_opcode_breakdown.py --tx-hash 0x... --chain base

# Single block
python state_opcode_breakdown.py --block 31000000 --chain base

# Block range
python state_opcode_breakdown.py --block-range \
  --start-block 34947727 --end-block 34948000 --chain base
```

## Key arguments

| Argument | Default |
|----------|---------|
| `--lookback-points` | `0,5,10,20` |
| `--num-workers` | `10` |
| `--gas-chunk-size` | `20000000` |
| `--output-dir` | `$RAW_BASE_DIR/opcode_breakdown_sensitive` |

## Output

`$RAW_BASE_DIR/opcode_breakdown_sensitive/block_{N}_opcode_breakdown.csv` (one file per block; single-transaction mode writes `{tx_hash}_opcode_breakdown.csv`)

Wide format: one row per transaction × lookback. Columns:

- Identity: `tx_hash`, `original_block`, `state_block`, `lookback`, `success`, `error`, `gas_used`
- Intrinsic gas: `intrinsic_gas`, `calldata_zero_gas`, `calldata_nonzero_gas`, `creation_gas`, `access_list_gas`, `authorization_list_gas`, `eip3860_init_gas`
- Opcode totals: `total_opcode_gas`, `uncapped_refund`, `refunds_effective`, `net_gas`, `opcode_count`, `storage_reads`, `storage_writes`, `storage_slots_modified`
- State changes: `storage_slots_created`, `storage_slots_deleted`, `storage_slots_updated`, `net_storage_slots_written`, `accounts_created`, `accounts_deleted`, `bytecode_bytes_allocated`, `bytecode_bytes_freed`, `net_bytecode_bytes`
- Per-opcode gas: `{OP}_gas` for every opcode observed in the block (e.g. `SLOAD_gas`, `CALL_gas`)
- Cold-access counts: `{OP}_cold_access_count` for account-access ops (`BALANCE`, `CALL`, `DELEGATECALL`, `STATICCALL`, `EXTCODESIZE`, `EXTCODECOPY`, ...) and storage ops (`SLOAD`, `SSTORE`)

Deterministic historical-state validation rejections are retained as unsuccessful rows. Missing or transient trace results abort the block so a rerun retries it.

Requires an archive node with `debug_traceCall` support.
