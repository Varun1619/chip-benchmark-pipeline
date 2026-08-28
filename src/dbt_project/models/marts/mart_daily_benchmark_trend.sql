-- Daily series per family and category, for the dashboard's trend charts.
--
-- Medians rather than means, because a throttled run or a stray tail would drag
-- a mean and make a healthy day look like a regression.

select
    run_date,
    soc_family,
    workload_category,
    count(*) as runs,
    count(distinct config_id) as configs,
    median(throughput_per_watt) as median_throughput_per_watt,
    median(power_avg_w) as median_power_w,
    median(latency_p99_ms) as median_latency_p99_ms,
    median(temperature_c) as median_temperature_c,
    avg(case when thermal_throttled then 1.0 else 0.0 end) as throttled_share,
    max(driver_version) as latest_driver_version
from {{ ref('fct_benchmark_runs') }}
where is_completed
  and throughput_per_watt is not null
group by all
order by run_date, soc_family, workload_category
