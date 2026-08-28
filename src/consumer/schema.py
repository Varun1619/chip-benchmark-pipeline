"""Spark schema for the benchmark run payload.

Declared explicitly rather than inferred. Structured streaming cannot sample a
stream to guess types, and an explicit schema also means a producer that adds
a field does not silently change the shape of the lake.

Timestamps arrive as ISO 8601 strings with microseconds and a Z suffix. They
are read as strings here and converted in the transform, which keeps parsing
failures visible as nulls instead of failing a whole micro-batch.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

_STRING = StringType()
_INT = IntegerType()
_DOUBLE = DoubleType()
_BOOL = BooleanType()

BENCHMARK_RUN_SCHEMA = StructType(
    [
        StructField("schema_version", _INT, nullable=False),
        StructField("run_id", _STRING, nullable=False),
        StructField("run_started_at", _STRING, nullable=False),
        StructField("run_finished_at", _STRING, nullable=False),
        # Configuration under test.
        StructField("config_id", _STRING, nullable=False),
        StructField("soc_model", _STRING, nullable=False),
        StructField("soc_family", _STRING, nullable=False),
        StructField("segment", _STRING, nullable=False),
        StructField("process_node_nm", _INT, nullable=False),
        StructField("big_cores", _INT, nullable=False),
        StructField("little_cores", _INT, nullable=False),
        StructField("max_clock_ghz", _DOUBLE, nullable=False),
        StructField("gpu_cores", _INT, nullable=False),
        StructField("npu_tops", _DOUBLE, nullable=False),
        StructField("memory_gb", _INT, nullable=False),
        StructField("memory_type", _STRING, nullable=False),
        StructField("nominal_tdp_w", _DOUBLE, nullable=False),
        # What was run.
        StructField("workload_category", _STRING, nullable=False),
        StructField("benchmark_name", _STRING, nullable=False),
        StructField("precision", _STRING, nullable=False),
        StructField("batch_size", _INT, nullable=False),
        # Software stack.
        StructField("driver_version", _STRING, nullable=False),
        StructField("runtime_version", _STRING, nullable=False),
        StructField("harness_version", _STRING, nullable=False),
        # Results. Nullable, because a failed run reports none of them.
        StructField("run_status", _STRING, nullable=False),
        StructField("throughput", _DOUBLE, nullable=True),
        StructField("throughput_unit", _STRING, nullable=False),
        StructField("latency_p50_ms", _DOUBLE, nullable=True),
        StructField("latency_p95_ms", _DOUBLE, nullable=True),
        StructField("latency_p99_ms", _DOUBLE, nullable=True),
        StructField("power_avg_w", _DOUBLE, nullable=True),
        StructField("power_peak_w", _DOUBLE, nullable=True),
        StructField("energy_j", _DOUBLE, nullable=True),
        StructField("temperature_c", _DOUBLE, nullable=True),
        StructField("thermal_throttled", _BOOL, nullable=True),
        StructField("memory_peak_mb", _DOUBLE, nullable=True),
        StructField("duration_s", _DOUBLE, nullable=True),
    ]
)

# Columns the lake is partitioned by.
#
# Date only, deliberately. Adding workload_category as a second level multiplies
# the directory count by four and buys no pruning worth having at this volume:
# a measured run produced 1819 files averaging 33 KB across 234 directories, and
# writing that many tiny files took minutes per micro-batch. Category stays a
# normal column, so dbt and the dashboard still filter on it.
PARTITION_COLUMNS = ("run_date",)

CORRUPT_RECORD_COLUMN = "_corrupt_record"

# Used only to classify a rejected record, never written to the lake.
#
# from_json in its default permissive mode returns a struct of nulls for a
# payload that is not JSON at all, which is indistinguishable from valid JSON
# that happens to be missing every field. Parsing a second time against this
# schema, with the corrupt record column named, separates the two cases so
# quarantine can say which one it was.
PARSE_PROBE_SCHEMA = StructType(
    [
        StructField("run_id", _STRING, nullable=True),
        StructField(CORRUPT_RECORD_COLUMN, _STRING, nullable=True),
    ]
)
