"""Structured streaming from Redpanda into a partitioned Parquet lake.

The transforms are plain functions over DataFrames, so they can be tested on a
batch DataFrame without starting a stream. Only `start_*_query` touches the
streaming API.

Records that cannot be parsed go to a quarantine path rather than being
dropped. A silently discarded record is a silently wrong dashboard, and the
raw bytes are kept so the cause can be found later.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from consumer.schema import (
    BENCHMARK_RUN_SCHEMA,
    CORRUPT_RECORD_COLUMN,
    PARSE_PROBE_SCHEMA,
    PARTITION_COLUMNS,
)

logger = logging.getLogger(__name__)

# Bounds how much of a backfill one micro-batch takes on, so the first trigger
# after a restart does not try to read the whole topic at once.
MAX_OFFSETS_PER_TRIGGER = 50_000


def build_spark_session(app_name: str = "benchmark-consumer") -> SparkSession:
    """Create the local session the streaming job runs in.

    Timestamps are pinned to UTC so a run's date does not depend on the host's
    timezone, which would put the same run in different lake partitions on
    different machines.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, brokers: str, topic: str) -> DataFrame:
    """Read the topic from the earliest offset the checkpoint has not seen."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # A topic retention cut should not kill the job in a demo stack.
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_runs(raw: DataFrame) -> DataFrame:
    """Attach the parsed payload next to the Kafka metadata.

    The raw string is carried through so a record that fails to parse can be
    written to quarantine with its original bytes intact.
    """
    payload = F.col("value").cast("string")
    return raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        payload.alias("raw_value"),
        F.from_json(payload, BENCHMARK_RUN_SCHEMA).alias("run"),
        # Second parse whose only job is to tell "not JSON" apart from "JSON
        # with fields missing". See PARSE_PROBE_SCHEMA.
        F.from_json(
            payload,
            PARSE_PROBE_SCHEMA,
            {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN},
        ).alias("probe"),
    )


def _is_usable() -> Column:
    """Return true when a record parsed and carries the fields the lake needs.

    Wrapped in coalesce so the predicate is never null. A null predicate would
    drop the record from both the valid and the quarantine side.
    """
    parsed = (
        F.col("run").isNotNull()
        & F.col(f"probe.{CORRUPT_RECORD_COLUMN}").isNull()
        & F.col("run.run_id").isNotNull()
        & F.to_timestamp(F.col("run.run_started_at")).isNotNull()
    )
    return F.coalesce(parsed, F.lit(False))


def valid_runs(parsed: DataFrame) -> DataFrame:
    """Flatten usable records and add the partition and lineage columns."""
    return (
        parsed.where(_is_usable())
        .select("run.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("run_started_at", F.to_timestamp("run_started_at"))
        .withColumn("run_finished_at", F.to_timestamp("run_finished_at"))
        .withColumn("run_date", F.to_date("run_started_at"))
        .withColumn("ingested_at", F.current_timestamp())
    )


def quarantined_runs(parsed: DataFrame) -> DataFrame:
    """Return records that could not be used, with a reason and the raw bytes."""
    return parsed.where(~_is_usable()).select(
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
        F.when(
            F.col("run").isNull() | F.col(f"probe.{CORRUPT_RECORD_COLUMN}").isNotNull(),
            F.lit("unparseable_json"),
        )
        .when(F.col("run.run_id").isNull(), F.lit("missing_run_id"))
        .otherwise(F.lit("unparseable_run_started_at"))
        .alias("quarantine_reason"),
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingest_date"),
    )


def start_lake_query(
    runs: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str,
) -> StreamingQuery:
    """Write usable runs to the partitioned lake.

    Repartitioning on the partition columns first sends every row for a date to
    one task, so each date directory gets one file per micro-batch instead of
    one per task. Without it a backfill spanning 45 days writes hundreds of tiny
    files that are slow to write and slower to read.
    """
    return (
        runs.repartition(*PARTITION_COLUMNS)
        .writeStream.queryName("benchmark_runs_lake")
        .format("parquet")
        .outputMode("append")
        .partitionBy(*PARTITION_COLUMNS)
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )


def start_quarantine_query(
    rejects: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str,
) -> StreamingQuery:
    """Write rejected records to quarantine, partitioned by ingest date."""
    return (
        rejects.writeStream.queryName("benchmark_runs_quarantine")
        .format("parquet")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
