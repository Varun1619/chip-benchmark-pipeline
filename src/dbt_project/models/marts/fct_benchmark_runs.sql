-- One row per benchmark run, with the efficiency measures derived.
--
-- Throughput per watt is the number that matters for silicon selection. Raw
-- throughput flatters a part that simply draws more power, so both are carried
-- and the leaderboard ranks on the former.

select
    run_id,
    run_started_at,
    run_finished_at,
    run_date,
    ingested_at,

    config_id,
    soc_model,
    soc_family,
    segment,
    process_node_nm,
    total_cores,
    max_clock_ghz,
    gpu_cores,
    npu_tops,
    memory_gb,
    memory_type,
    nominal_tdp_w,

    workload_category,
    benchmark_name,
    precision_mode,
    batch_size,

    driver_version,
    runtime_version,
    harness_version,

    run_status,
    is_completed,
    throughput,
    throughput_unit,
    latency_p50_ms,
    latency_p95_ms,
    latency_p99_ms,
    power_avg_w,
    power_peak_w,
    energy_j,
    temperature_c,
    thermal_throttled,
    memory_peak_mb,
    duration_s,

    case
        when power_avg_w > 0 then throughput / power_avg_w
    end as throughput_per_watt,

    -- How much of the part's rated power the workload actually pulled. Well
    -- above 1 means the run was pushed past its nominal envelope.
    case
        when nominal_tdp_w > 0 then power_avg_w / nominal_tdp_w
    end as tdp_utilisation,

    -- Tail spread. A part can hold median throughput and still miss a latency
    -- target, so the ratio is kept as its own measure.
    case
        when latency_p50_ms > 0 then latency_p99_ms / latency_p50_ms
    end as latency_tail_ratio

from {{ ref('stg_benchmark_runs') }}
