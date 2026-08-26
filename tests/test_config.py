"""Tests for the shared settings module."""

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from common.config import Settings, get_settings


class _IsolatedSettings(Settings):
    """Settings that skip .env so a developer's local file cannot change results."""

    model_config = SettingsConfigDict(env_file=None, case_sensitive=False, extra="ignore")


def _settings(**overrides: Any) -> Settings:
    return _IsolatedSettings(**overrides)


def test_defaults_target_a_host_run() -> None:
    settings = _settings()

    assert settings.redpanda_brokers == "localhost:19092"
    assert settings.benchmark_topic == "benchmark.runs.raw"
    assert settings.producer_max_events == 0


def test_derived_paths_hang_off_data_root() -> None:
    settings = _settings(data_root=Path("/data"))

    assert settings.lake_root == Path("/data/lake")
    assert settings.raw_runs_path == Path("/data/lake/benchmark_runs")
    assert settings.checkpoint_path == Path("/data/checkpoints/benchmark_runs")
    assert settings.duckdb_path == Path("/data/warehouse/benchmarks.duckdb")


def test_broker_list_splits_and_trims() -> None:
    settings = _settings(redpanda_brokers="a:9092, b:9092 ,")

    assert settings.broker_list == ["a:9092", "b:9092"]


def test_environment_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDPANDA_BROKERS", "redpanda:9092")
    monkeypatch.setenv("PRODUCER_EVENTS_PER_SECOND", "100")

    settings = _settings()

    assert settings.redpanda_brokers == "redpanda:9092"
    assert settings.producer_events_per_second == 100.0


def test_log_level_is_normalised() -> None:
    assert _settings(log_level="debug").log_level == "DEBUG"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("producer_events_per_second", 0),
        ("producer_events_per_second", -1),
        ("producer_max_events", -5),
        ("log_level", "chatty"),
    ],
)
def test_invalid_values_are_rejected(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        _settings(**{field: value})


def test_get_settings_is_cached() -> None:
    get_settings.cache_clear()

    assert get_settings() is get_settings()
