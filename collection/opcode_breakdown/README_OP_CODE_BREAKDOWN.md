# Opcode Breakdown

Per-opcode gas breakdown for Ethereum/Base transactions using `debug_traceTransaction`. Compatible with Reth.

## Setup

Set `RAW_BASE_DIR` in `.env` and configure RPC endpoints in `rpc_config.json`.

## Usage

```bash
# Single transaction
python op_code_breakdown.py single 0xabc123... --chain ethereum

# Date range (recommended)
python op_code_breakdown.py daterange 2025-09-01 2025-09-30 \
  --chains base \
  --blocks-per-day 100 \
  --workers 32 \
  --batch-size 25  # use 20-25 for nodes with 100MB response limit
```

Output: `$RAW_BASE_DIR/opcode_breakdown/{chain}/block_{N}_opcode_gas.csv`

Resumable: processed blocks are tracked in `{chain}_blocks_summary.csv` and skipped on re-run.

## Fork support

- **Cancun** (March 2024): EIP-4844, BLOBBASEFEE
- **Pectra** (May 2025): EIP-7623 calldata floor, EIP-7702 auth lists — Ethereum block 22,431,084 / Base block 30,008,527

## Exact account transitions

`account_births` and `account_deaths` use Reth’s code-enabled `prestateTracer` diff. The collector reconstructs sparse post-state over pre-state, applies the EIP-161 empty-account definition, and refuses to write a block if either count is unavailable.
