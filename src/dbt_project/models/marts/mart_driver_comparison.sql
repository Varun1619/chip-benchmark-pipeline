-- Each cell's performance on a driver version against its performance on the
-- previous one.
--
-- This exists because fct_run_baselines cannot answer the question on its own.
-- A rolling baseline detects a change, not a state: once about
-- {{ var('baseline_window') }} regressed runs have accumulated, the baseline has
-- itself dropped to the regressed level, the z-score returns to zero, and a
-- regression that is still costing throughput every day becomes invisible.
-- Measured on generated data, a 13 percent regression raised alerts only in the
-- runs immediately after the version landed, and none afterwards.
--
-- Comparing against a fixed reference, the previous driver version, answers the
-- question a performance team actually asks: what did this release break, and is
-- it still broken. The two models are complementary and both are kept.
--
-- Version ordering is lexicographic, which holds for the versions in use but
-- would misorder a 24.10.0 against a 24.9.0. A real deployment would carry a
-- release date or an ordinal.

with cell_driver as (

    select
        config_id,
        max(soc_model) as soc_model,
        max(soc_family) as soc_family,
        max(segment) as segment,
        max(workload_category) as workload_category,
        benchmark_name,
        precision_mode,
        batch_size,
        driver_version,
        count(*) as runs,
        median(throughput) as median_throughput,
        median(throughput_per_watt) as median_throughput_per_watt,
        max(throughput_unit) as throughput_unit,
        min(run_started_at) as first_run_at,
        max(run_started_at) as last_run_at
    from {{ ref('fct_benchmark_runs') }}
    where is_completed
      and throughput is not null
      and not thermal_throttled
    group by config_id, benchmark_name, precision_mode, batch_size, driver_version

),

sequenced as (

    select
        *,
        lag(driver_version) over cell as previous_driver_version,
        lag(median_throughput) over cell as previous_median_throughput,
        lag(runs) over cell as previous_runs
    from cell_driver
    window cell as (
        partition by config_id, benchmark_name, precision_mode, batch_size
        order by driver_version
    )

),

compared as (

    select
        *,
        100.0 * (median_throughput - previous_median_throughput)
            / nullif(previous_median_throughput, 0) as pct_change
    from sequenced
    where previous_median_throughput is not null
      and runs >= {{ var('min_cell_runs') }}
      and previous_runs >= {{ var('min_cell_runs') }}

)

select
    config_id,
    soc_model,
    soc_family,
    segment,
    workload_category,
    benchmark_name,
    precision_mode,
    batch_size,
    previous_driver_version,
    driver_version,
    previous_runs,
    runs,
    round(previous_median_throughput, 3) as previous_median_throughput,
    round(median_throughput, 3) as median_throughput,
    throughput_unit,
    round(pct_change, 2) as pct_change,
    first_run_at,
    last_run_at,

    case
        when pct_change <= -{{ var('version_change_pct') }} then 'regression'
        when pct_change >= {{ var('version_change_pct') }} then 'improvement'
        else 'stable'
    end as verdict

from compared
order by pct_change
