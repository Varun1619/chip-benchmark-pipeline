"""Reference data the generator draws from.

Part numbers and benchmark names are invented. They are shaped like the real
thing so the analytics are recognisable, but no measurement here comes from
real silicon.

Adding a part or a benchmark changes the generated data without touching
generation logic, which keeps the physical model in one place.

The two catalogues below are fenced off from the formatter. They are tables,
and one field per line hides the shape that makes them readable.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkloadCategory(StrEnum):
    """Top level grouping used for lake partitioning and dashboard filters."""

    AI_ML = "ai_ml"
    COMPUTER_VISION = "computer_vision"
    MEDIA_GAMING = "media_gaming"
    PRODUCTIVITY = "productivity"


class Bottleneck(StrEnum):
    """Which block on the die dominates a benchmark's score."""

    NPU = "npu"
    GPU = "gpu"
    CPU = "cpu"


@dataclass(frozen=True, slots=True)
class SocSpec:
    """One system under test: a part paired with a memory configuration."""

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

    @property
    def config_id(self) -> str:
        """Stable identifier for this part and memory pairing.

        Used as the Kafka message key, so every run for a configuration lands
        on the same partition and keeps its relative order.
        """
        return f"{self.soc_model.lower()}-{self.memory_gb}gb-{self.memory_type.lower()}"

    @property
    def total_cores(self) -> int:
        """CPU cores across both clusters."""
        return self.big_cores + self.little_cores


@dataclass(frozen=True, slots=True)
class WorkloadSpec:
    """One benchmark, with the throughput model the generator scales."""

    benchmark_name: str
    category: WorkloadCategory
    bottleneck: Bottleneck
    throughput_unit: str
    precisions: tuple[str, ...]
    batch_sizes: tuple[int, ...]
    # Throughput on the reference part defined below, before any scaling.
    reference_throughput: float
    # Fraction of nominal TDP this workload draws when it runs.
    power_factor: float
    # Wall clock seconds for one run of the benchmark harness.
    duration_s: float


# The part every throughput number is expressed relative to. Scaling a result
# for another part is then a ratio of capability, not an absolute guess.
REFERENCE_NPU_TOPS = 30.0
REFERENCE_GPU_CORES = 1024
REFERENCE_CPU_CAPABILITY = 8 * 3.0  # big cores times clock

_AI = WorkloadCategory.AI_ML
_CV = WorkloadCategory.COMPUTER_VISION
_MG = WorkloadCategory.MEDIA_GAMING
_PR = WorkloadCategory.PRODUCTIVITY

_NPU = Bottleneck.NPU
_GPU = Bottleneck.GPU
_CPU = Bottleneck.CPU

# fmt: off
SOCS: tuple[SocSpec, ...] = (
    #       model      family segment       node big lit clock  gpu    tops  mem  memory type  tdp
    SocSpec("XN-7100", "XN", "mobile",         5,  4,  4, 2.6,   640,  18.0,   8, "LPDDR5",    6.5),
    SocSpec("XN-7300", "XN", "mobile",         4,  6,  4, 3.0,   896,  26.0,  12, "LPDDR5X",   8.0),
    SocSpec("XN-7500", "XN", "mobile",         3,  8,  4, 3.4,  1280,  42.0,  16, "LPDDR5X",   9.5),
    # Same silicon, more memory. Isolates the memory effect from the part.
    SocSpec("XN-7500", "XN", "mobile",         3,  8,  4, 3.4,  1280,  42.0,  24, "LPDDR5X",   9.5),
    SocSpec("EG-3100", "EG", "edge",           5,  8,  0, 2.4,   512,  24.0,  16, "DDR5",     25.0),
    SocSpec("EG-3300", "EG", "edge",           4, 12,  0, 2.8,  1024,  48.0,  32, "DDR5",     40.0),
    SocSpec("DC-9200", "DC", "datacenter",     4, 48,  0, 2.9,  4096, 220.0,  96, "HBM3",    210.0),
    SocSpec("DC-9400", "DC", "datacenter",     3, 72,  0, 3.1,  6144, 400.0, 144, "HBM3",    280.0),
)

WORKLOADS: tuple[WorkloadSpec, ...] = (
    #            benchmark            cat  block  unit                precisions              batches   ref put  pwr  secs
    WorkloadSpec("llm_prefill_7b",     _AI, _NPU, "tokens_per_s",     ("fp16", "bf16", "int8"), (1, 8, 32),    850.0, 0.92, 120.0),
    WorkloadSpec("llm_decode_7b",      _AI, _NPU, "tokens_per_s",     ("fp16", "int8"),         (1, 4),         62.0, 0.74, 180.0),
    WorkloadSpec("bert_base_infer",    _AI, _NPU, "inferences_per_s", ("fp32", "fp16", "int8"), (1, 16, 64),  1450.0, 0.86,  90.0),
    WorkloadSpec("resnet50_train",     _AI, _GPU, "images_per_s",     ("fp32", "bf16"),         (32, 128),     410.0, 0.97, 600.0),
    WorkloadSpec("dlrm_recommend",     _AI, _NPU, "queries_per_s",    ("fp16", "int8"),         (64, 256),    9800.0, 0.68, 150.0),
    WorkloadSpec("yolov8_detect",      _CV, _NPU, "frames_per_s",     ("fp16", "int8"),         (1, 8),        245.0, 0.81,  75.0),
    WorkloadSpec("unet_segment",       _CV, _GPU, "frames_per_s",     ("fp16", "int8"),         (1, 4),         96.0, 0.88, 110.0),
    WorkloadSpec("stereo_depth",       _CV, _GPU, "frames_per_s",     ("fp32", "fp16"),         (1,),          142.0, 0.79,  65.0),
    WorkloadSpec("isp_pipeline",       _CV, _CPU, "frames_per_s",     ("fp32",),                (1,),          310.0, 0.52,  45.0),
    WorkloadSpec("h265_encode_4k",     _MG, _GPU, "frames_per_s",     ("fp32",),                (1,),          118.0, 0.83,  90.0),
    WorkloadSpec("av1_decode_8k",      _MG, _GPU, "frames_per_s",     ("fp32",),                (1,),           74.0, 0.71,  60.0),
    WorkloadSpec("raster_1440p",       _MG, _GPU, "frames_per_s",     ("fp32",),                (1,),           96.0, 0.95, 180.0),
    WorkloadSpec("raytrace_1080p",     _MG, _GPU, "frames_per_s",     ("fp32", "fp16"),         (1,),           48.0, 0.99, 180.0),
    WorkloadSpec("llvm_compile",       _PR, _CPU, "files_per_s",      ("fp32",),                (1,),           34.0, 0.76, 300.0),
    WorkloadSpec("spreadsheet_recalc", _PR, _CPU, "cells_per_s",      ("fp32",),                (1,),       185000.0, 0.44,  30.0),
    WorkloadSpec("browser_speedometer",_PR, _CPU, "runs_per_min",     ("fp32",),                (1,),           42.0, 0.58, 120.0),
    WorkloadSpec("pdf_render",         _PR, _CPU, "pages_per_s",      ("fp32",),                (1,),           88.0, 0.49,  40.0),
)
# fmt: on

# Software stack versions, oldest first. The generator walks this list over the
# generated time range so a regression can be attributed to a version bump.
DRIVER_VERSIONS: tuple[str, ...] = (
    "24.2.0",
    "24.3.1",
    "24.4.0",
    "24.5.2",
)

RUNTIME_VERSIONS: tuple[str, ...] = ("1.18.0", "1.19.1", "1.20.0")

HARNESS_VERSION = "2.4.0"

# Precision multipliers applied to throughput. Lower precision moves more work
# per cycle, at the cost of accuracy the harness does not measure here.
PRECISION_SPEEDUP: dict[str, float] = {
    "fp32": 1.0,
    "bf16": 1.75,
    "fp16": 1.9,
    "int8": 3.1,
}

SOCS_BY_CONFIG: dict[str, SocSpec] = {soc.config_id: soc for soc in SOCS}
WORKLOADS_BY_NAME: dict[str, WorkloadSpec] = {w.benchmark_name: w for w in WORKLOADS}
