-- Each run compared against a rolling baseline of its own recent history.
--
-- The grain is deliberate and was chosen by measuring the data. Batch size and
-- precision move throughput by more than a regression does, so a baseline that
-- mixes them compares populations rather than performance: a 13 percent
-- regression showed up as a 2.6 percent dip when aggregated across a family,
-- while a benchmark with nothing wrong with it swung 40 percent on batch mix
-- alone. Held at this grain, an unaffected benchmark is stable within about
-- 2 percent across driver versions.
--
-- Throttled runs are excluded from the baseline. Throttling moves a result
-- about 18 percent, further than the regression being hunted, so including it
-- would make the baseline track cooling rather than performance.
--
-- The window ends one row before the current run, so a run is never part of the
-- baseline it is judged against.

with comparable as (

    select *
    from {{ ref('fct_benchmark_runs') }}
    where is_completed
      and throughput is not null
      and not thermal_throttled

),

with_baseline as (

    select
        *,
        avg(throughput) over cell as baseline_throughput,
        stddev_samp(throughput) over cell as baseline_stddev,
        count(*) over cell as baseline_sample_count
    from comparable
    window cell as (
        partition by config_id, benchmark_name, precision_mode, batch_size
        order by run_started_at, run_id
        rows between {{ var('baseline_window') }} preceding and 1 preceding
    )

)

select
    run_id,
    run_started_at,
    run_date,
    config_id,
    soc_model,
    soc_family,
    segment,
    workload_category,
    benchmark_name,
    precision_mode,
    batch_size,
    driver_version,
    throughput,
    throughput_unit,
    throughput_per_watt,
    power_avg_w,

    baseline_throughput,
    baseline_stddev,
    baseline_sample_count,

    case
        when baseline_stddev > 0
            then (throughput - baseline_throughput) / baseline_stddev
    end as throughput_zscore,

    case
        when baseline_throughput > 0
            then 100.0 * (throughput - baseline_throughput) / baseline_throughput
    end as throughput_pct_deviation,

    -- A run counts as regressed only when it breaches both thresholds. The
    -- z-score alone fires on a benchmark that is merely very repeatable, and
    -- the percentage alone fires on a noisy one.
    coalesce(
        baseline_sample_count >= {{ var('min_baseline_samples') }}
        and baseline_stddev > 0
        and (throughput - baseline_throughput) / baseline_stddev
            <= {{ var('regression_zscore') }}
        and 100.0 * (throughput - baseline_throughput) / baseline_throughput
            <= {{ var('regression_pct') }},
        false
    ) as is_regressed

from with_baseline
