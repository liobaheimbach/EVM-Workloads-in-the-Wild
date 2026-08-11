with base_fee as (
    select
        number as block_number,
        base_fee_per_gas as base_fee,
        miner
    from ethereum.blocks
    where time >= timestamp '2025-01-01 00:00:00'
      and time <  timestamp '2026-01-01 00:00:00'
),

mempool_distinct as (
    select distinct hash
    from dune.flashbots.dataset_mempool_dumpster
    where from_unixtime(cast(timestamp_ms as decimal) / 1000)
          >= timestamp '2025-01-01 00:00:00'
      and from_unixtime(cast(timestamp_ms as decimal) / 1000)
          <  timestamp '2026-01-01 00:00:00'
),

gas_fee_n_privacy as (
    select
        t.hash as tx_hash,
        t.to as to_addr,
        t.gas_used,
        lag(case when t."from" = b.miner then 1 else 0 end)
            over (partition by t.block_number order by t.index desc) as ofa
    from ethereum.transactions t
    join base_fee b
      on b.block_number = t.block_number
    left join mempool_distinct p
      on from_hex(p.hash) = t.hash
    where t.block_time >= timestamp '2025-01-01 00:00:00'
      and t.block_time <  timestamp '2026-01-01 00:00:00'
      and p.hash is null
      and t.to is not null
),

trades as (
    select distinct tx_hash
    from dex.trades
    where blockchain = 'ethereum'
      and block_time >= timestamp '2025-01-01 00:00:00'
      and block_time <  timestamp '2026-01-01 00:00:00'
),

liq as (
    select evt_tx_hash from aave_v3_ethereum.Pool_evt_ReserveDataUpdated
    union distinct
    select evt_tx_hash from aave_v2_ethereum.Lendingpool_evt_Reservedataupdated
    union distinct
    select evt_tx_hash from compound_v3_ethereum.Liquidator_evt_Absorb
),

final as (
    select
        g.tx_hash,
        g.to_addr,
        g.gas_used
    from gas_fee_n_privacy g
    join trades t on t.tx_hash = g.tx_hash
    where coalesce(g.ofa, 0) != 1
      and g.tx_hash not in (select * from liq)
)

-- Top 1000 addresses by gas consumed. This covers ~96% of CEX-DEX gas over the
-- window; the remaining long tail is deliberately excluded from the label set.
select
    to_addr,
    count(*)     as tx_count,
    sum(gas_used) as total_gas_used
from final
group by 1
order by total_gas_used desc
limit 1000
