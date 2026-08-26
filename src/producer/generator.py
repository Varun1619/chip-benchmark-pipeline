"""Synthetic benchmark result generator.

Results are not random numbers with a label attached. Throughput is derived
from the capability of the block that bottlenecks the benchmark, power from
the part's TDP and process node, and latency from throughput and batch size.
That means the analytics downstream find relationships that hold, which is
the point of the exercise.

Four effects are injected on purpose so the analytics layer has something
real to detect:

Run to run noise, so a single sample is never enough to call a regression.

Thermal throttling, more likely on mobile parts running sustained heavy
workloads, which lowers throughput while raising temperature and power.

Software regressions and improvements tied to a driver version, because that
is what a performance team actually chases.

Failed and timed out runs with no measurements, so the models have to handle
nulls rather than assuming every row has a score.

Everything is driven by a seeded PRNG, so the same seed reproduces the same
stream.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random

from producer.catalog import (
    DRIVER_VERSIONS,
    HARNESS_VERSION,
    PRECISION_SPEEDUP,
    REFERENCE_CPU_CAPABILITY,
    REFERENCE_GPU_CORES,
    REFERENCE_NPU_TOPS,
    RUNTIME_VERSIONS,
    SOCS,
    WORKLOADS,
    Bottleneck,
    SocSpec,
    WorkloadSpec,
)
from producer.schema import BenchmarkRun

# Days before now that each driver version in DRIVER_VERSIONS rolled out. A run
# gets the newest version whose rollout is on or before its own timestamp.
DRIVER_ROLLOUT_DAYS_AGO: tuple[int, ...] = (45, 30, 14, 5)

# Process node the power model treats as neutral.
REFERENCE_NODE_NM = 5

RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_TIMEOUT = "timeout"

FAILURE_RATE = 0.015
TIMEOUT_RATE = 0.005


@dataclass(frozen=True, slots=True)
class StackEffect:
    """A throughput change that arrives with a driver version.

    A factor below 1.0 is a regression, above 1.0 an improvement. It applies
    only to the named family and benchmark, which is how a real regression
    behaves: narrow, and invisible in an aggregate over everything.
    """

    soc_family: str
    benchmark_name: str
    from_driver: str
    factor: float
    note: str


STACK_EFFECTS: tuple[StackEffect, ...] = (
    StackEffect(
        soc_family="XN",
        benchmark_name="llm_decode_7b",
        from_driver="24.4.0",
        factor=0.87,
        note="Decode regression on mobile parts after the 24.4.0 scheduler change.",
    ),
    StackEffect(
        soc_family="DC",
        benchmark_name="resnet50_train",
        from_driver="24.5.2",
        factor=0.91,
        note="Training throughput regression on datacenter parts in 24.5.2.",
    ),
    StackEffect(
        soc_family="EG",
        benchmark_name="yolov8_detect",
        from_driver="24.4.0",
        factor=1.16,
        note="Detection improvement on edge parts from the 24.4.0 NPU compiler.",
    ),
    StackEffect(
        soc_family="XN",
        benchmark_name="h265_encode_4k",
        from_driver="24.5.2",
        factor=1.09,
        note="Encoder improvement on mobile parts in 24.5.2.",
    ),
)


def driver_for(run_started_at: datetime, now: datetime) -> str:
    """Return the driver version in force for a run at this timestamp."""
    days_ago = (now - run_started_at).total_seconds() / 86400.0
    version = DRIVER_VERSIONS[0]
    for candidate, rollout in zip(DRIVER_VERSIONS, DRIVER_ROLLOUT_DAYS_AGO, strict=True):
        if days_ago <= rollout:
            version = candidate
    return version


def _driver_index(version: str) -> int:
    return DRIVER_VERSIONS.index(version)


def _capability(soc: SocSpec, workload: WorkloadSpec) -> float:
    """Capability of the block that bottlenecks this benchmark, versus reference."""
    if workload.bottleneck is Bottleneck.NPU:
        return soc.npu_tops / REFERENCE_NPU_TOPS
    if workload.bottleneck is Bottleneck.GPU:
        return soc.gpu_cores / REFERENCE_GPU_CORES
    return (soc.big_cores * soc.max_clock_ghz) / REFERENCE_CPU_CAPABILITY


def _memory_factor(soc: SocSpec, workload: WorkloadSpec, batch_size: int) -> float:
    """Small throughput bonus when a large footprint workload has headroom."""
    if workload.category.value not in {"ai_ml", "computer_vision"} or batch_size < 16:
        return 1.0
    return 1.0 + 0.05 * math.log2(max(soc.memory_gb, 1) / 8)


def _stack_factor(soc: SocSpec, workload: WorkloadSpec, driver_version: str) -> float:
    """Return the combined effect of every stack change already rolled out."""
    factor = 1.0
    driver = _driver_index(driver_version)
    for effect in STACK_EFFECTS:
        matches = (
            effect.soc_family == soc.soc_family
            and effect.benchmark_name == workload.benchmark_name
            and driver >= _driver_index(effect.from_driver)
        )
        if matches:
            factor *= effect.factor
    return factor


class BenchmarkRunGenerator:
    """Produces `BenchmarkRun` records from a seeded PRNG."""

    def __init__(
        self,
        seed: int,
        socs: tuple[SocSpec, ...] = SOCS,
        workloads: tuple[WorkloadSpec, ...] = WORKLOADS,
    ) -> None:
        """Seed the PRNG and fix the catalogue this generator draws from."""
        self._rng = Random(seed)
        self._socs = socs
        self._workloads = workloads

    @property
    def config_count(self) -> int:
        """Number of distinct configurations in the catalogue."""
        return len({soc.config_id for soc in self._socs})

    def generate(self, run_started_at: datetime, now: datetime | None = None) -> BenchmarkRun:
        """Build one benchmark run as if it had started at the given time."""
        rng = self._rng
        now = now or datetime.now(tz=UTC)

        soc = rng.choice(self._socs)
        workload = rng.choice(self._workloads)
        precision = rng.choice(workload.precisions)
        batch_size = rng.choice(workload.batch_sizes)
        driver_version = driver_for(run_started_at, now)

        status = self._draw_status()
        duration_s = workload.duration_s * rng.uniform(0.94, 1.08)

        if status != RUN_STATUS_COMPLETED:
            return self._failed_run(
                soc,
                workload,
                precision,
                batch_size,
                driver_version,
                run_started_at,
                status,
                duration_s,
            )

        throughput, throttled = self._throughput(
            soc, workload, precision, batch_size, driver_version
        )
        power_avg = self._power(soc, workload, throttled)
        latencies = self._latencies(throughput, batch_size)

        return BenchmarkRun(
            run_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            run_started_at=run_started_at,
            run_finished_at=run_started_at + timedelta(seconds=duration_s),
            config_id=soc.config_id,
            soc_model=soc.soc_model,
            soc_family=soc.soc_family,
            segment=soc.segment,
            process_node_nm=soc.process_node_nm,
            big_cores=soc.big_cores,
            little_cores=soc.little_cores,
            max_clock_ghz=soc.max_clock_ghz,
            gpu_cores=soc.gpu_cores,
            npu_tops=soc.npu_tops,
            memory_gb=soc.memory_gb,
            memory_type=soc.memory_type,
            nominal_tdp_w=soc.nominal_tdp_w,
            workload_category=workload.category.value,
            benchmark_name=workload.benchmark_name,
            precision=precision,
            batch_size=batch_size,
            driver_version=driver_version,
            runtime_version=rng.choice(RUNTIME_VERSIONS),
            harness_version=HARNESS_VERSION,
            run_status=RUN_STATUS_COMPLETED,
            throughput=round(throughput, 3),
            throughput_unit=workload.throughput_unit,
            latency_p50_ms=latencies[0],
            latency_p95_ms=latencies[1],
            latency_p99_ms=latencies[2],
            power_avg_w=round(power_avg, 3),
            power_peak_w=round(power_avg * rng.uniform(1.08, 1.28), 3),
            energy_j=round(power_avg * duration_s, 2),
            temperature_c=self._temperature(soc, workload, throttled),
            thermal_throttled=throttled,
            memory_peak_mb=self._memory_peak(workload, batch_size),
            duration_s=round(duration_s, 2),
        )

    def _draw_status(self) -> str:
        roll = self._rng.random()
        if roll < FAILURE_RATE:
            return RUN_STATUS_FAILED
        if roll < FAILURE_RATE + TIMEOUT_RATE:
            return RUN_STATUS_TIMEOUT
        return RUN_STATUS_COMPLETED

    def _failed_run(
        self,
        soc: SocSpec,
        workload: WorkloadSpec,
        precision: str,
        batch_size: int,
        driver_version: str,
        run_started_at: datetime,
        status: str,
        duration_s: float,
    ) -> BenchmarkRun:
        """Build a run with no measurements, so the failure rate stays visible."""
        rng = self._rng
        elapsed = duration_s * (2.0 if status == RUN_STATUS_TIMEOUT else rng.uniform(0.05, 0.4))
        return BenchmarkRun(
            run_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            run_started_at=run_started_at,
            run_finished_at=run_started_at + timedelta(seconds=elapsed),
            config_id=soc.config_id,
            soc_model=soc.soc_model,
            soc_family=soc.soc_family,
            segment=soc.segment,
            process_node_nm=soc.process_node_nm,
            big_cores=soc.big_cores,
            little_cores=soc.little_cores,
            max_clock_ghz=soc.max_clock_ghz,
            gpu_cores=soc.gpu_cores,
            npu_tops=soc.npu_tops,
            memory_gb=soc.memory_gb,
            memory_type=soc.memory_type,
            nominal_tdp_w=soc.nominal_tdp_w,
            workload_category=workload.category.value,
            benchmark_name=workload.benchmark_name,
            precision=precision,
            batch_size=batch_size,
            driver_version=driver_version,
            runtime_version=rng.choice(RUNTIME_VERSIONS),
            harness_version=HARNESS_VERSION,
            run_status=status,
            throughput_unit=workload.throughput_unit,
            duration_s=round(elapsed, 2),
        )

    def _throughput(
        self,
        soc: SocSpec,
        workload: WorkloadSpec,
        precision: str,
        batch_size: int,
        driver_version: str,
    ) -> tuple[float, bool]:
        rng = self._rng
        value = workload.reference_throughput
        value *= _capability(soc, workload)
        value *= PRECISION_SPEEDUP[precision]
        value *= _memory_factor(soc, workload, batch_size)
        # Batching amortises fixed overhead, with diminishing returns.
        value *= math.pow(batch_size, 0.18)
        value *= _stack_factor(soc, workload, driver_version)

        throttled = self._draws_throttle(soc, workload)
        if throttled:
            value *= rng.uniform(0.78, 0.93)

        # Run to run noise. Lognormal keeps throughput positive and slightly
        # right skewed, which is how repeated benchmark runs actually land.
        value *= rng.lognormvariate(0.0, 0.035)
        return value, throttled

    def _draws_throttle(self, soc: SocSpec, workload: WorkloadSpec) -> bool:
        """Sustained heavy workloads throttle, and small packages throttle first."""
        segment_risk = {"mobile": 0.30, "edge": 0.08, "datacenter": 0.02}
        risk = segment_risk.get(soc.segment, 0.05)
        risk *= workload.power_factor
        risk *= min(workload.duration_s / 120.0, 2.0)
        return self._rng.random() < min(risk, 0.6)

    def _power(self, soc: SocSpec, workload: WorkloadSpec, throttled: bool) -> float:
        # A smaller node does the same work for less power. math.pow rather
        # than ** because the operator is typed as returning Any for float
        # exponents, which would leak through the whole power model.
        node_efficiency = math.pow(soc.process_node_nm / REFERENCE_NODE_NM, 0.35)
        power = soc.nominal_tdp_w * workload.power_factor * node_efficiency
        if throttled:
            # Throttling holds power at the limit while throughput falls.
            power = soc.nominal_tdp_w * self._rng.uniform(0.95, 1.02)
        return power * self._rng.lognormvariate(0.0, 0.04)

    def _latencies(self, throughput: float, batch_size: int) -> tuple[float, float, float]:
        """Tail latencies derived from throughput, not drawn independently."""
        rng = self._rng
        p50 = 1000.0 * batch_size / throughput
        p95 = p50 * rng.uniform(1.12, 1.34)
        p99 = p95 * rng.uniform(1.06, 1.22)
        # Six places, because a high throughput productivity benchmark lands
        # well under a microsecond per item and four places would round it flat.
        return round(p50, 6), round(p95, 6), round(p99, 6)

    def _temperature(self, soc: SocSpec, workload: WorkloadSpec, throttled: bool) -> float:
        if throttled:
            return round(self._rng.uniform(94.0, 103.0), 2)
        base = {"mobile": 42.0, "edge": 38.0, "datacenter": 34.0}.get(soc.segment, 40.0)
        return round(base + 38.0 * workload.power_factor + self._rng.uniform(-3.0, 4.0), 2)

    def _memory_peak(self, workload: WorkloadSpec, batch_size: int) -> float:
        footprint = {
            "ai_ml": 1450.0,
            "computer_vision": 520.0,
            "media_gaming": 780.0,
            "productivity": 240.0,
        }[workload.category.value]
        return round(footprint * math.pow(batch_size, 0.55) * self._rng.uniform(0.9, 1.15), 1)
