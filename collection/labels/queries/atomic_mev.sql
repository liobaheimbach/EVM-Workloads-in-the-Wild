with gas_tx as (
    select
        t.hash as tx_hash,
        t.to as to_addr,
        t.gas_used
    from ethereum.transactions t
    where t.block_time >= timestamp '2025-01-01 00:00:00'
      and t.block_time <  timestamp '2026-01-01 00:00:00'
      and t.to is not null
),

atomic_arb as (
    select distinct tx_hash
    from query_3493305
),

sandwich as (
    select distinct cast(tx_hash as varbinary) as tx_hash
    from query_3375587
),

mev_union as (
    select tx_hash from atomic_arb
    union
    select tx_hash from sandwich
)

-- Top 1000 addresses by gas consumed, matching the cutoff in cex_dex.sql.
select
    g.to_addr,
    count(*)        as tx_count,
    sum(g.gas_used) as total_gas_used
from gas_tx g
join mev_union m on g.tx_hash = m.tx_hash
group by 1
order by total_gas_used desc
limit 1000
