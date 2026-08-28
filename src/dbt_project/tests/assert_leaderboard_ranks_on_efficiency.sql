-- Rank 1 in a category must hold the highest median throughput per watt, and
-- must therefore carry an efficiency index of 100. If ranking ever gets wired
-- to raw throughput by accident, this fails.

with leaders as (

    select workload_category, efficiency_index
    from {{ ref('mart_config_leaderboard') }}
    where efficiency_rank = 1

)

select *
from leaders
where efficiency_index != 100.0
