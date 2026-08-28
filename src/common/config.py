"""Settings shared by every service in the pipeline.

All services read the same environment variables, so one .env file drives the
whole stack whether it runs under docker compose or a single component is
started directly on the host.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration resolved from the environment and .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    redpanda_brokers: str = Field(
        default="localhost:19092",
        description="Comma separated bootstrap servers. Use redpanda:9092 inside compose.",
    )
    benchmark_topic: str = Field(
        default="benchmark.runs.raw",
        description="Topic carrying raw synthetic benchmark runs.",
    )

    producer_events_per_second: float = Field(
        default=25.0,
        description="Target publish rate for the synthetic generator.",
    )
    producer_seed: int = Field(
        default=42,
        description="Seed for the generator so a run can be reproduced exactly.",
    )
    producer_max_events: int = Field(
        default=0,
        description="Stop after this many events. 0 produces until the process is stopped.",
    )
    producer_backfill_days: int = Field(
        default=45,
        description="History to generate at startup so rolling baselines have data at once.",
    )
    producer_backfill_runs_per_day: int = Field(
        default=400,
        description="Runs per simulated day during backfill. 0 skips the backfill.",
    )

    data_root: Path = Field(
        default=Path("data"),
        description="Root of the local data lake, warehouse and checkpoints.",
    )
    checkpoint_root: Path | None = Field(
        default=None,
        description=(
            "Where streaming checkpoints live. Defaults under DATA_ROOT. Point it at a "
            "container volume to keep the metadata log off a slow bind mount."
        ),
    )
    spark_trigger_interval: str = Field(
        default="30 seconds",
        description="Processing time trigger for the structured streaming query.",
    )

    dashboard_port: int = Field(default=8501, description="Port Streamlit binds to.")
    log_level: str = Field(default="INFO", description="Root log level for all services.")

    @field_validator("producer_events_per_second")
    @classmethod
    def _rate_is_positive(cls, value: float) -> float:
        if value <= 0:
            msg = "producer_events_per_second must be greater than 0"
            raise ValueError(msg)
        return value

    @field_validator(
        "producer_max_events",
        "producer_backfill_days",
        "producer_backfill_runs_per_day",
    )
    @classmethod
    def _count_is_not_negative(cls, value: int) -> int:
        if value < 0:
            msg = "counts must be 0 or greater"
            raise ValueError(msg)
        return value

    @field_validator("log_level")
    @classmethod
    def _log_level_is_known(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        normalised = value.strip().upper()
        if normalised not in allowed:
            msg = f"log_level must be one of {sorted(allowed)}"
            raise ValueError(msg)
        return normalised

    @property
    def broker_list(self) -> list[str]:
        """Bootstrap servers as a list, for clients that do not take a string."""
        return [host.strip() for host in self.redpanda_brokers.split(",") if host.strip()]

    @property
    def lake_root(self) -> Path:
        """Directory holding the Parquet data lake."""
        return self.data_root / "lake"

    @property
    def raw_runs_path(self) -> Path:
        """Partitioned Parquet output written by the streaming consumer."""
        return self.lake_root / "benchmark_runs"

    @property
    def checkpoints_dir(self) -> Path:
        """Base directory for streaming checkpoints."""
        return self.checkpoint_root or (self.data_root / "checkpoints")

    @property
    def checkpoint_path(self) -> Path:
        """Structured streaming checkpoint location for the raw runs query."""
        return self.checkpoints_dir / "benchmark_runs"

    @property
    def quarantine_path(self) -> Path:
        """Records the consumer could not parse, kept with their raw bytes."""
        return self.lake_root / "quarantine"

    @property
    def quarantine_checkpoint_path(self) -> Path:
        """Checkpoint for the quarantine query, separate from the lake query."""
        return self.checkpoints_dir / "quarantine"

    @property
    def duckdb_path(self) -> Path:
        """DuckDB file that dbt builds and the dashboard reads."""
        return self.data_root / "warehouse" / "benchmarks.duckdb"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process wide settings, parsed once per process."""
    return Settings()
