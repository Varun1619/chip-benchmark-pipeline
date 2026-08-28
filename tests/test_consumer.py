"""Tests for the streaming consumer's transforms.

The transforms are plain DataFrame functions, so they run here on a batch
DataFrame shaped exactly like the Kafka source. Payloads come from the real
producer, which means a schema drift between the two sides fails a test rather
than quietly producing null columns in the lake.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from producer.generator import BenchmarkRunGenerator
from producer.schema import BenchmarkRun

pytest.importorskip("pyspark", reason="the consumer extra is not installed")

from consumer.schema import BENCHMARK_RUN_SCHEMA, PARTITION_COLUMNS  # noqa: E402
from consumer.stream import parse_runs, quarantined_runs, valid_runs  # noqa: E402

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

KAFKA_SOURCE_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _a_run(seed: int = 1) -> BenchmarkRun:
    return BenchmarkRunGenerator(seed=seed).generate(NOW, now=NOW)


def _kafka_rows(payloads: list[tuple[bytes, bytes]]) -> list[tuple[Any, ...]]:
    return [
        (key, value, "benchmark.runs.raw", 0, offset, NOW)
        for offset, (key, value) in enumerate(payloads)
    ]


def _source(spark: Any, payloads: list[tuple[bytes, bytes]]) -> Any:
    return spark.createDataFrame(_kafka_rows(payloads), schema=KAFKA_SOURCE_SCHEMA)


def test_spark_schema_matches_the_producer_model() -> None:
    """The lake schema and the published record must not drift apart."""
    assert set(BENCHMARK_RUN_SCHEMA.fieldNames()) == set(BenchmarkRun.model_fields)


def test_valid_run_is_flattened_with_partition_and_lineage_columns(spark: Any) -> None:
    run = _a_run()
    parsed = parse_runs(_source(spark, [(run.to_kafka_key(), run.to_kafka_value())]))

    rows = valid_runs(parsed).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["run_id"] == run.run_id
    assert row["config_id"] == run.config_id
    assert row["soc_model"] == run.soc_model
    # Partition columns are derived, not published.
    for column in PARTITION_COLUMNS:
        assert column in row
    assert row["run_date"] == run.run_started_at.date()
    # Category is not a partition column, so it has to survive as a plain one.
    assert "workload_category" not in PARTITION_COLUMNS
    assert row["workload_category"] == run.workload_category
    # Kafka coordinates are kept so a row can be traced back to the topic.
    assert row["kafka_partition"] == 0
    assert row["kafka_offset"] == 0
    assert row["ingested_at"] is not None


def test_microsecond_timestamps_with_a_z_suffix_survive_parsing(spark: Any) -> None:
    run = _a_run()
    payload = json.loads(run.to_kafka_value())
    assert payload["run_started_at"].endswith("Z"), "producer should emit UTC with a Z suffix"

    parsed = parse_runs(_source(spark, [(run.to_kafka_key(), run.to_kafka_value())]))
    row = valid_runs(parsed).collect()[0]

    parsed_at = row["run_started_at"]
    assert parsed_at is not None
    # Microseconds survive, and the Z suffix does not shift the day, which is
    # what the lake partitions on.
    assert parsed_at.microsecond == run.run_started_at.microsecond
    assert parsed_at.date() == run.run_started_at.date()
    assert parsed_at.hour == run.run_started_at.hour


def test_failed_runs_keep_their_nulls(spark: Any) -> None:
    """A failed run has no measurements and still belongs in the lake."""
    generator = BenchmarkRunGenerator(seed=13)
    failures = [
        run
        for run in (generator.generate(NOW, now=NOW) for _ in range(3000))
        if run.run_status != "completed"
    ]
    assert failures, "expected the generator to produce failures"

    payloads = [(r.to_kafka_key(), r.to_kafka_value()) for r in failures[:20]]
    rows = valid_runs(parse_runs(_source(spark, payloads))).collect()

    assert len(rows) == len(payloads)
    for row in rows:
        assert row["throughput"] is None
        assert row["power_avg_w"] is None
        assert row["run_status"] in {"failed", "timeout"}
        # Still partitionable, which is what keeps failures countable per day.
        assert row["run_date"] is not None


def test_unparseable_json_is_quarantined_with_its_raw_bytes(spark: Any) -> None:
    parsed = parse_runs(_source(spark, [(b"some-key", b"{not json at all")]))

    assert valid_runs(parsed).count() == 0
    rows = quarantined_runs(parsed).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["quarantine_reason"] == "unparseable_json"
    assert row["raw_value"] == "{not json at all"
    assert row["kafka_offset"] == 0


def test_a_payload_missing_run_id_is_quarantined(spark: Any) -> None:
    run = _a_run()
    payload = json.loads(run.to_kafka_value())
    del payload["run_id"]
    broken = json.dumps(payload).encode()

    parsed = parse_runs(_source(spark, [(run.to_kafka_key(), broken)]))

    assert valid_runs(parsed).count() == 0
    assert quarantined_runs(parsed).collect()[0]["quarantine_reason"] == "missing_run_id"


def test_no_record_is_lost_between_the_two_sides(spark: Any) -> None:
    """Every input lands in exactly one of the lake or quarantine."""
    good = [_a_run(seed=s) for s in range(1, 6)]
    payloads = [(r.to_kafka_key(), r.to_kafka_value()) for r in good]
    payloads.append((b"k", b"not-json"))
    payloads.append((b"k", json.dumps({"schema_version": 1}).encode()))

    parsed = parse_runs(_source(spark, payloads))

    assert valid_runs(parsed).count() == len(good)
    assert quarantined_runs(parsed).count() == 2
    assert valid_runs(parsed).count() + quarantined_runs(parsed).count() == len(payloads)
