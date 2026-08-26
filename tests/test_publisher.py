"""Tests for the Kafka publisher.

The real producer is replaced with a fake, so these cover the wrapper's own
behaviour: what it sends, how it counts delivery outcomes, and whether it
recovers instead of dropping a message when the local queue fills.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from producer import publisher as publisher_module
from producer.generator import BenchmarkRunGenerator
from producer.publisher import BenchmarkPublisher

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


class _FakeMessage:
    def __init__(self, key: bytes) -> None:
        self._key = key

    def key(self) -> bytes:
        return self._key


class _FakeProducer:
    """Records what it was asked to send and fires callbacks on poll."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.sent: list[dict[str, Any]] = []
        self.flush_calls = 0
        self.fail_next_produce_with_buffer_error = False
        self.deliver_error: Any = None
        self._pending: list[Any] = []

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        if self.fail_next_produce_with_buffer_error:
            self.fail_next_produce_with_buffer_error = False
            raise BufferError("queue full")
        self.sent.append({"topic": topic, "key": key, "value": value})
        self._pending.append((on_delivery, key))

    def poll(self, _timeout: float) -> int:
        for callback, key in self._pending:
            callback(self.deliver_error, _FakeMessage(key))
        delivered = len(self._pending)
        self._pending.clear()
        return delivered

    def flush(self, _timeout: float = 0.0) -> int:
        self.flush_calls += 1
        self.poll(0)
        return 0


@pytest.fixture
def fake_producer(monkeypatch: pytest.MonkeyPatch) -> _FakeProducer:
    created: list[_FakeProducer] = []

    def factory(config: dict[str, Any]) -> _FakeProducer:
        created.append(_FakeProducer(config))
        return created[-1]

    monkeypatch.setattr(publisher_module, "Producer", factory)
    BenchmarkPublisher("broker:9092", "topic")
    return created[-1]


def _a_run() -> Any:
    return BenchmarkRunGenerator(seed=1).generate(NOW, now=NOW)


def test_producer_is_configured_for_idempotent_batched_delivery(
    fake_producer: _FakeProducer,
) -> None:
    config = fake_producer.config

    assert config["bootstrap.servers"] == "broker:9092"
    assert config["enable.idempotence"] is True
    assert config["compression.type"] == "lz4"
    assert config["linger.ms"] > 0


def test_publish_sends_the_run_keyed_by_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeProducer({})
    monkeypatch.setattr(publisher_module, "Producer", lambda _config: fake)
    run = _a_run()

    with BenchmarkPublisher("broker:9092", "benchmark.runs.raw") as pub:
        pub.publish(run)

    assert len(fake.sent) == 1
    sent = fake.sent[0]
    assert sent["topic"] == "benchmark.runs.raw"
    assert sent["key"] == run.config_id.encode()
    assert json.loads(sent["value"])["run_id"] == run.run_id
    assert pub.delivered == 1
    assert pub.failed == 0


def test_delivery_errors_are_counted_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeProducer({})
    fake.deliver_error = "broker rejected the batch"
    monkeypatch.setattr(publisher_module, "Producer", lambda _config: fake)

    with BenchmarkPublisher("broker:9092", "topic") as pub:
        pub.publish(_a_run())

    assert pub.failed == 1
    assert pub.delivered == 0


def test_a_full_local_queue_is_flushed_and_the_message_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeProducer({})
    fake.fail_next_produce_with_buffer_error = True
    monkeypatch.setattr(publisher_module, "Producer", lambda _config: fake)

    with BenchmarkPublisher("broker:9092", "topic") as pub:
        pub.publish(_a_run())

    # The run is sent rather than dropped, after a flush to make room.
    assert len(fake.sent) == 1
    assert fake.flush_calls >= 1
    assert pub.delivered == 1


def test_leaving_the_context_flushes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeProducer({})
    monkeypatch.setattr(publisher_module, "Producer", lambda _config: fake)

    with BenchmarkPublisher("broker:9092", "topic") as pub:
        pub.publish(_a_run())
        before = fake.flush_calls

    assert fake.flush_calls > before
