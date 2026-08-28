-- Ingestion and data quality per day, so the dashboard can show whether the
-- pipeline itself is healthy rather than only what the silicon did.
--
-- A dashboard that cannot tell "performance dropped" from "we stopped
-- receiving data" is not trustworthy, which is what this model is for.

select
    run_date,
    count(*) as runs,
    count(distinct config_id) as configs_reporting,
    count(distinct benchmark_name) as benchmarks_reporting,
    sum(case when is_completed then 1 else 0 end) as completed_runs,
    sum(case when run_status = 'failed' then 1 else 0 end) as failed_runs,
    sum(case when run_status = 'timeout' then 1 else 0 end) as timeout_runs,
    round(avg(case when is_completed then 0.0 else 1.0 end), 4) as failure_share,
    round(avg(case when thermal_throttled then 1.0 else 0.0 end), 4) as throttled_share,
    min(ingested_at) as first_ingested_at,
    max(ingested_at) as last_ingested_at
from {{ ref('fct_benchmark_runs') }}
group by all
order by run_date
