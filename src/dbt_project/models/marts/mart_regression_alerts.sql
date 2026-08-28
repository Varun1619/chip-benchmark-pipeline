-- Regressions worth a person's attention, one row per affected cell and driver.
--
-- A single breaching run is noise. An alert is raised only when a meaningful
-- share of runs in the same cell breach on the same driver version, which is
-- what a real software regression looks like: narrow, and persistent once the
-- version lands.

with per_cell as (

    select
        config_id,
        soc_model,
        soc_family,
        segment,
        workload_category,
        benchmark_name,
        precision_mode,
        batch_size,
        driver_version,
        count(*) as runs_evaluated,
        sum(case when is_regressed then 1 else 0 end) as regressed_runs,
        median(throughput_pct_deviation) as median_pct_deviation,
        median(throughput_zscore) as median_zscore,
        median(throughput) as median_throughput,
        median(baseline_throughput) as median_baseline_throughput,
        min(case when is_regressed then run_started_at end) as first_regressed_at,
        max(case when is_regressed then run_started_at end) as last_regressed_at,
        max(throughput_unit) as throughput_unit
    from {{ ref('fct_run_baselines') }}
    where baseline_sample_count >= {{ var('min_baseline_samples') }}
    group by all

),

scored as (

    select
        *,
        regressed_runs::double / nullif(runs_evaluated, 0) as regressed_share
    from per_cell

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
    driver_version,
    runs_evaluated,
    regressed_runs,
    round(regressed_share, 4) as regressed_share,
    round(median_pct_deviation, 2) as median_pct_deviation,
    round(median_zscore, 2) as median_zscore,
    round(median_throughput, 3) as median_throughput,
    round(median_baseline_throughput, 3) as median_baseline_throughput,
    throughput_unit,
    first_regressed_at,
    last_regressed_at,

    -- Ordering for a triage list: how bad, weighted by how consistent.
    case
        when regressed_share >= 0.5 and median_pct_deviation <= -10 then 'critical'
        when regressed_share >= 0.25 then 'warning'
        else 'watch'
    end as severity

from scored
where regressed_runs >= 3
  and regressed_share >= 0.10
order by median_pct_deviation asc
