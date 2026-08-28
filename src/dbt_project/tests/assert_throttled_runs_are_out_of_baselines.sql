-- Throttling moves a result further than the regressions being hunted, so a
-- throttled run must not reach the baseline model at all. This fails if one
-- leaks through.

select b.run_id
from {{ ref('fct_run_baselines') }} as b
join {{ ref('fct_benchmark_runs') }} as r on b.run_id = r.run_id
where r.thermal_throttled
   or not r.is_completed
   or r.throughput is null
