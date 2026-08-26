"""The benchmark run record published to Kafka.

One message is one completed attempt at one benchmark on one configuration.
The record is deliberately wide and flat: the streaming consumer writes it
straight to Parquet without a join, and dbt does the modelling downstream.

`schema_version` is carried on every message so the consumer can keep reading
an older payload after a field is added.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

SCHEMA_VERSION = 1


class BenchmarkRun(BaseModel):
    """A single benchmark result with its configuration and software stack."""

    schema_version: int = Field(default=SCHEMA_VERSION)
    run_id: str = Field(description="UUID for this attempt.")
    run_started_at: datetime = Field(description="Event time. Partitions the lake.")
    run_finished_at: datetime

    # Configuration under test. Denormalised on purpose so a single Parquet
    # read answers questions about the hardware without a dimension lookup.
    config_id: str
    soc_model: str
    soc_family: str
    segment: str
    process_node_nm: int
    big_cores: int
    little_cores: int
    max_clock_ghz: float
    gpu_cores: int
    npu_tops: float
    memory_gb: int
    memory_type: str
    nominal_tdp_w: float

    # What was run.
    workload_category: str
    benchmark_name: str
    precision: str
    batch_size: int

    # Software stack. These are the fields a regression is usually attributed
    # to, so they are first class rather than metadata.
    driver_version: str
    runtime_version: str
    harness_version: str

    # Results. Every measurement is optional because a failed run has none,
    # and dropping failures at the producer would hide the failure rate.
    run_status: str = Field(description="completed, failed or timeout.")
    throughput: float | None = None
    throughput_unit: str
    latency_p50_ms: float | None = None
    latency_p95_ms: float | None = None
    latency_p99_ms: float | None = None
    power_avg_w: float | None = None
    power_peak_w: float | None = None
    energy_j: float | None = None
    temperature_c: float | None = None
    thermal_throttled: bool = False
    memory_peak_mb: float | None = None
    duration_s: float | None = None

    def to_kafka_value(self) -> bytes:
        """Serialise to the UTF-8 JSON payload published to the topic."""
        return self.model_dump_json().encode("utf-8")

    def to_kafka_key(self) -> bytes:
        """Partition key.

        Keying on the configuration keeps every run for a part on one
        partition, so its history stays ordered for the rolling baseline
        the analytics layer builds.
        """
        return self.config_id.encode("utf-8")
