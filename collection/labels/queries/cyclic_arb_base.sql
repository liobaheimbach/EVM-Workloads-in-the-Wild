with aggs as (
    select project_contract_address as tx_to
    from dex_aggregator.trades
    where blockchain = 'base'
      and project_contract_address is not null
    union
    select address as tx_to
    from dex.addresses
    where blockchain = 'base'
),

candidates as (
    select
        t.tx_hash,
        t.tx_to
    from dex.trades t
    where t.blockchain = 'base'
      and t.block_time >= timestamp '2025-01-01 00:00'
      and t.block_time <  timestamp '2026-01-01 00:00'
      and not exists (
          select 1 from aggs a where a.tx_to = t.tx_to
      )
    group by 1, 2
    having count(*) > 2
),

paths as (
    select
        t.tx_hash,
        t.tx_to,
        array_agg(t.token_sold_address order by t.evt_index)   as sold_tokens,
        array_agg(t.token_bought_address order by t.evt_index) as bought_tokens,
        map_filter(
            map_zip_with(
                multimap_agg(t.token_sold_address, -t.token_sold_amount),
                multimap_agg(t.token_bought_address, t.token_bought_amount),
                (k, v1, v2) ->
                    reduce(coalesce(v1, array[]), 0, (s, x) -> s + coalesce(x, 0), s -> s)
                  + reduce(coalesce(v2, array[]), 0, (s, x) -> s + coalesce(x, 0), s -> s)
            ),
            (k, v) -> v <> 0
        ) as balance_changes
    from dex.trades t
    join candidates c on t.tx_hash = c.tx_hash
    group by 1, 2
),

mevs as (
    select tx_hash, tx_to
    from paths
    where cardinality(balance_changes) > 0
      and reduce(map_values(balance_changes), 0, (s, x) -> s + x, s -> s) > 0
      and element_at(sold_tokens, 1) = element_at(bought_tokens, cardinality(bought_tokens))
),

gas_agg as (
    select
        m.tx_to,
        count(distinct m.tx_hash) as mev_tx_count,
        sum(tx.gas_used)          as total_gas_used,
        avg(tx.gas_used)          as avg_gas_used
    from mevs m
    join base.transactions tx on m.tx_hash = tx.hash
    group by 1
)

-- Top 1000 addresses by gas consumed, matching the cutoff in cex_dex.sql.
select *
from gas_agg
order by total_gas_used desc
limit 1000
