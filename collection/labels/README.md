# Transaction Labeling

Workflows for labeling Ethereum and Base transactions from multiple sources.

## Usage

```bash
# Ethereum
python workflow_label_transactions.py --chain ethereum --db /path/to/tx_metadata_ethereum.duckdb

# Base
python workflow_label_transactions_base.py --db /path/to/tx_metadata_base.duckdb

# Resume from a specific step
python workflow_label_transactions.py --chain ethereum --db ... --start-from 6

# Skip Etherscan/Basescan fetching
python workflow_label_transactions.py --chain ethereum --db ... --skip-etherscan
python workflow_label_transactions_base.py --db ... --skip-basescan
```

## Label sources (priority order)

1. Manual labels (`manual_labels.csv`)
2. Spellbook + DefiLlama (`spellbook_scraper.py`, `defi_llama_scraper.py`)
3. Kleros Curate (`fetch_kleros_curate.py`)
4. RPC contract identification (`identify_pools_and_tokens.py`)
5. Etherscan / Basescan (`fetch_etherscan_incremental.py`)
6. MEV contracts from Dune queries (see `queries/`)
7. Keyword categorization (`apply_categories_to_csv.py`)
8. Transaction type rules (simple transfers, type-3, contract creations)

## Dune queries

MEV label inputs are produced by the queries in `queries/`:

- `cex_dex.sql` — CEX-DEX arbitrage contracts on Ethereum
- `atomic_mev.sql` — Atomic MEV contracts on Ethereum
- `cyclic_arb_base.sql` — Cyclic arbitrage contracts on Base

Run each query on Dune, export the result as JSON with a `result.rows` array, and pass the file path to the workflow via `--mev-json`.

## RPC configuration

- `rpc_config.json` — RPC endpoints for both Ethereum and Base (set `rpc_url` and `rpc_tracing` to your node URLs)

## Label columns added to `transactions` table

| Column | Description |
|--------|-------------|
| `from_label` / `from_category` | Sender label and category |
| `to_label` / `to_category` | Receiver label and category |
| `receiver_is_eoa` / `receiver_is_contract` | Address type flags |
| `simple_transfer` | TRUE for 21k gas non-blob transfers |

## Categories

Address categories produced by the pipeline (`categories.py` rule set, scrapers,
and RPC identification):

`Token`, `Infrastructure`, `CEX`, `NFT`, `DEX`, `Lending`, `Staking`, `Bridge`,
`Social`, `Gambling`, `Multi Level Marketing`, `DAO`, `MEV`, `Phishing`,
`Airdrop`, `System Contract`, `L2`, `DeFi`, `Gaming`, `EOA`, `Uncategorized`

(`Bots` and `Batch_Submitter` are merged into `MEV` and `L2` by
`apply_categories_to_csv.py`.)

Transaction-level rules additionally write `L2` (type-3 blob transactions) and
`Contract Creation` (transactions whose receiver is NULL) to `to_category`;
transactions that invoke CREATE or CREATE2 internally retain their ordinary
category. 21k-gas transfers are flagged via the `simple_transfer` column.
