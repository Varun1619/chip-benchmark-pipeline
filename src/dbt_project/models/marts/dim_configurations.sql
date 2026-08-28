-- One row per configuration under test.
--
-- The lake denormalises hardware onto every run, so this is derived rather
-- than sourced. It exists to give the dashboard a short list to filter on and
-- to make the run counts per part obvious.

select
    config_id,
    soc_model,
    soc_family,
    segment,
    process_node_nm,
    big_cores,
    little_cores,
    total_cores,
    max_clock_ghz,
    gpu_cores,
    npu_tops,
    memory_gb,
    memory_type,
    nominal_tdp_w,
    count(*) as total_runs,
    count(distinct benchmark_name) as benchmarks_covered,
    min(run_started_at) as first_run_at,
    max(run_started_at) as last_run_at
from {{ ref('stg_benchmark_runs') }}
group by all
