-- Which configuration wins, ranked on efficiency rather than raw throughput.
--
-- Ranking on throughput alone rewards a part for drawing more power. Throughput
-- per watt is the comparison a silicon decision actually turns on, so it drives
-- the rank and raw throughput is carried alongside for context.
--
-- Grouping sets give per category rows and an all workloads row in one pass,
-- so the dashboard can switch between them without a second model.

with runs as (

    select *
    from {{ ref('fct_benchmark_runs') }}
    where is_completed
      and throughput_per_watt is not null

),

aggregated as (

    select
        config_id,
        max(soc_model) as soc_model,
        max(soc_family) as soc_family,
        max(segment) as segment,
        max(nominal_tdp_w) as nominal_tdp_w,
        coalesce(workload_category, 'all_workloads') as workload_category,
        count(*) as completed_runs,
        median(throughput_per_watt) as median_throughput_per_watt,
        median(power_avg_w) as median_power_w,
        median(latency_p99_ms) as median_latency_p99_ms,
        avg(case when thermal_throttled then 1.0 else 0.0 end) as throttled_share
    from runs
    group by grouping sets ((config_id, workload_category), (config_id))

),

failure_rates as (

    select
        config_id,
        avg(case when is_completed then 0.0 else 1.0 end) as failure_share
    from {{ ref('fct_benchmark_runs') }}
    group by config_id

)

select
    a.workload_category,
    rank() over (
        partition by a.workload_category
        order by a.median_throughput_per_watt desc
    ) as efficiency_rank,
    a.config_id,
    a.soc_model,
    a.soc_family,
    a.segment,
    a.nominal_tdp_w,
    a.completed_runs,
    round(a.median_throughput_per_watt, 4) as median_throughput_per_watt,
    round(a.median_power_w, 2) as median_power_w,
    round(a.median_latency_p99_ms, 3) as median_latency_p99_ms,
    round(a.throttled_share, 4) as throttled_share,
    round(f.failure_share, 4) as failure_share,

    -- Indexed against the category leader, which reads better on a chart than
    -- an absolute figure whose units differ per benchmark.
    round(
        100.0 * a.median_throughput_per_watt / max(a.median_throughput_per_watt) over (
            partition by a.workload_category
        ),
        1
    ) as efficiency_index

from aggregated as a
left join failure_rates as f on a.config_id = f.config_id
order by a.workload_category, efficiency_rank
