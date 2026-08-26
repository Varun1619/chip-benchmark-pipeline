"""Tests for the synthetic generator and the Kafka publisher.

The generator is asserted on relationships rather than exact values, because
the point of the physical model is that faster hardware scores higher and
lower precision runs faster. Where a statistic is compared, the seed is fixed,
so a failure means the model changed rather than that the test is flaky.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any

import pytest

from producer.catalog import SOCS_BY_CONFIG, WORKLOADS_BY_NAME
from producer.generator import (
    DRIVER_ROLLOUT_DAYS_AGO,
    RUN_STATUS_COMPLETED,
    BenchmarkRunGenerator,
    driver_for,
)
from producer.schema import SCHEMA_VERSION, BenchmarkRun

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def _runs(
    generator: BenchmarkRunGenerator, count: int, days_ago: float = 1.0
) -> list[BenchmarkRun]:
    at = NOW - timedelta(days=days_ago)
    return [generator.generate(at, now=NOW) for _ in range(count)]


def _completed_throughputs(runs: list[BenchmarkRun]) -> list[float]:
    return [r.throughput for r in runs if r.run_status == RUN_STATUS_COMPLETED and r.throughput]


def _single(config_id: str, benchmark: str, precision: str | None = None) -> dict[str, Any]:
    """Catalogue narrowed to one configuration and benchmark."""
    workload = WORKLOADS_BY_NAME[benchmark]
    if precision:
        workload = replace(workload, precisions=(precision,))
    return {"socs": (SOCS_BY_CONFIG[config_id],), "workloads": (workload,)}


def test_same_seed_reproduces_the_same_stream() -> None:
    left = _runs(BenchmarkRunGenerator(seed=7), 50)
    right = _runs(BenchmarkRunGenerator(seed=7), 50)

    assert [r.model_dump() for r in left] == [r.model_dump() for r in right]


def test_different_seeds_diverge() -> None:
    left = _runs(BenchmarkRunGenerator(seed=7), 50)
    right = _runs(BenchmarkRunGenerator(seed=8), 50)

    assert [r.run_id for r in left] != [r.run_id for r in right]


def test_config_count_counts_memory_variants_separately() -> None:
    # XN-7500 appears twice in the catalogue with 16 GB and 24 GB.
    assert BenchmarkRunGenerator(seed=1).config_count == 8


def test_payload_is_valid_json_carrying_the_schema_version() -> None:
    run = _runs(BenchmarkRunGenerator(seed=3), 1)[0]

    payload = json.loads(run.to_kafka_value())

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["config_id"] == run.config_id
    assert run.to_kafka_key() == run.config_id.encode()


def test_tail_latencies_are_ordered_and_derived_from_throughput() -> None:
    for run in _completed(BenchmarkRunGenerator(seed=11), 200):
        assert run.latency_p50_ms is not None
        assert run.latency_p95_ms is not None
        assert run.latency_p99_ms is not None
        assert run.latency_p50_ms <= run.latency_p95_ms <= run.latency_p99_ms
        # p50 is milliseconds per batch at the measured throughput.
        expected_p50 = 1000.0 * run.batch_size / run.throughput  # type: ignore[operator]
        assert run.latency_p50_ms == pytest.approx(expected_p50, rel=1e-3)


def _completed(generator: BenchmarkRunGenerator, count: int) -> list[BenchmarkRun]:
    return [r for r in _runs(generator, count) if r.run_status == RUN_STATUS_COMPLETED]


def test_faster_silicon_scores_higher_on_the_same_benchmark() -> None:
    small = BenchmarkRunGenerator(
        seed=5, **_single("xn-7100-8gb-lpddr5", "bert_base_infer", "int8")
    )
    large = BenchmarkRunGenerator(
        seed=5, **_single("dc-9400-144gb-hbm3", "bert_base_infer", "int8")
    )

    small_median = median(_completed_throughputs(_runs(small, 300)))
    large_median = median(_completed_throughputs(_runs(large, 300)))

    # DC-9400 has 400 TOPS against 18, so the ratio should be large.
    assert large_median > small_median * 15


def test_lower_precision_runs_faster() -> None:
    fp32 = BenchmarkRunGenerator(seed=9, **_single("eg-3300-32gb-ddr5", "bert_base_infer", "fp32"))
    int8 = BenchmarkRunGenerator(seed=9, **_single("eg-3300-32gb-ddr5", "bert_base_infer", "int8"))

    ratio = median(_completed_throughputs(_runs(int8, 300))) / median(
        _completed_throughputs(_runs(fp32, 300))
    )

    # PRECISION_SPEEDUP puts int8 at 3.1x fp32, before noise.
    assert 2.6 < ratio < 3.6


def test_driver_version_follows_the_rollout_schedule() -> None:
    oldest, second, third, newest = DRIVER_ROLLOUT_DAYS_AGO

    assert driver_for(NOW - timedelta(days=oldest), NOW) == "24.2.0"
    assert driver_for(NOW - timedelta(days=second), NOW) == "24.3.1"
    assert driver_for(NOW - timedelta(days=third), NOW) == "24.4.0"
    assert driver_for(NOW - timedelta(days=newest), NOW) == "24.5.2"
    assert driver_for(NOW, NOW) == "24.5.2"


def test_injected_regression_lowers_throughput_after_the_driver_bump() -> None:
    catalogue = _single("xn-7300-12gb-lpddr5x", "llm_decode_7b", "fp16")
    before = BenchmarkRunGenerator(seed=21, **catalogue)
    after = BenchmarkRunGenerator(seed=21, **catalogue)

    # 24.3.1 is in force 30 days ago, 24.4.0 from 14 days ago.
    healthy = median(_completed_throughputs(_runs(before, 300, days_ago=20)))
    regressed = median(_completed_throughputs(_runs(after, 300, days_ago=10)))

    ratio = regressed / healthy
    # STACK_EFFECTS applies a 0.87 factor to XN llm_decode_7b from 24.4.0.
    assert 0.80 < ratio < 0.94


def test_injected_improvement_raises_throughput_after_the_driver_bump() -> None:
    catalogue = _single("eg-3100-16gb-ddr5", "yolov8_detect", "int8")
    before = BenchmarkRunGenerator(seed=31, **catalogue)
    after = BenchmarkRunGenerator(seed=31, **catalogue)

    baseline = median(_completed_throughputs(_runs(before, 300, days_ago=20)))
    improved = median(_completed_throughputs(_runs(after, 300, days_ago=10)))

    assert 1.08 < improved / baseline < 1.26


def test_failed_runs_carry_no_measurements() -> None:
    runs = _runs(BenchmarkRunGenerator(seed=13), 3000)
    failures = [r for r in runs if r.run_status != RUN_STATUS_COMPLETED]

    assert failures, "expected some failures in 3000 runs"
    for run in failures:
        assert run.throughput is None
        assert run.power_avg_w is None
        assert run.latency_p50_ms is None
        # The unit is still known even when the measurement is missing.
        assert run.throughput_unit


def test_failure_rate_stays_near_two_percent() -> None:
    runs = _runs(BenchmarkRunGenerator(seed=17), 4000)
    rate = sum(r.run_status != RUN_STATUS_COMPLETED for r in runs) / len(runs)

    assert 0.010 < rate < 0.032


def test_throttled_runs_are_hot() -> None:
    # A sustained GPU workload on a mobile part is the throttling case.
    generator = BenchmarkRunGenerator(seed=23, **_single("xn-7100-8gb-lpddr5", "raytrace_1080p"))
    runs = _completed(generator, 400)
    throttled = [r for r in runs if r.thermal_throttled]
    clean = [r for r in runs if not r.thermal_throttled]

    assert throttled, "expected throttling on a mobile part running raytrace_1080p"
    assert clean, "expected some clean runs too"
    assert min(r.temperature_c or 0 for r in throttled) > max(r.temperature_c or 0 for r in clean)


def test_energy_is_power_times_duration() -> None:
    for run in _completed(BenchmarkRunGenerator(seed=29), 100):
        assert run.energy_j == pytest.approx(
            (run.power_avg_w or 0) * (run.duration_s or 0), rel=1e-2
        )
