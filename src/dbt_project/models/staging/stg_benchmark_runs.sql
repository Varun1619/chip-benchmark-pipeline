-- One row per benchmark run, deduplicated and typed.
--
-- The lake is a faithful record of what arrived, which means it holds
-- duplicates: Kafka delivers at least once, and a restarted producer replays
-- its seeded backfill. A measured lake held 366328 rows for 280518 distinct
-- run ids. Keeping the earliest arrival per run id makes the model idempotent
-- no matter how many times a record is redelivered.
--
-- `precision` is renamed because it is a SQL keyword and quoting it in every
-- downstream model is worse than renaming it once here.

with ranked as (

    select
        *,
        row_number() over (
            partition by run_id
            order by ingested_at asc, kafka_offset asc
        ) as arrival_rank
    from {{ source('lake', 'benchmark_runs') }}
    where run_id is not null

)

select
    run_id,
    schema_version,

    cast(run_started_at as timestamp) as run_started_at,
    cast(run_finished_at as timestamp) as run_finished_at,
    cast(run_date as date) as run_date,
    cast(ingested_at as timestamp) as ingested_at,

    config_id,
    soc_model,
    soc_family,
    segment,
    cast(process_node_nm as integer) as process_node_nm,
    cast(big_cores as integer) as big_cores,
    cast(little_cores as integer) as little_cores,
    cast(big_cores + little_cores as integer) as total_cores,
    cast(max_clock_ghz as double) as max_clock_ghz,
    cast(gpu_cores as integer) as gpu_cores,
    cast(npu_tops as double) as npu_tops,
    cast(memory_gb as integer) as memory_gb,
    memory_type,
    cast(nominal_tdp_w as double) as nominal_tdp_w,

    workload_category,
    benchmark_name,
    precision as precision_mode,
    cast(batch_size as integer) as batch_size,

    driver_version,
    runtime_version,
    harness_version,

    run_status,
    run_status = 'completed' as is_completed,
    cast(throughput as double) as throughput,
    throughput_unit,
    cast(latency_p50_ms as double) as latency_p50_ms,
    cast(latency_p95_ms as double) as latency_p95_ms,
    cast(latency_p99_ms as double) as latency_p99_ms,
    cast(power_avg_w as double) as power_avg_w,
    cast(power_peak_w as double) as power_peak_w,
    cast(energy_j as double) as energy_j,
    cast(temperature_c as double) as temperature_c,
    coalesce(cast(thermal_throttled as boolean), false) as thermal_throttled,
    cast(memory_peak_mb as double) as memory_peak_mb,
    cast(duration_s as double) as duration_s,

    cast(kafka_partition as integer) as kafka_partition,
    cast(kafka_offset as bigint) as kafka_offset

from ranked
where arrival_rank = 1
