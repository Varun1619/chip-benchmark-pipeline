"""Kafka publishing for benchmark runs.

Wraps `confluent_kafka.Producer` so the generator stays free of transport
concerns and the delivery outcome is counted rather than discarded.
"""

from __future__ import annotations

import logging
from types import TracebackType

from confluent_kafka import KafkaError, Message, Producer

from producer.schema import BenchmarkRun

logger = logging.getLogger(__name__)


class BenchmarkPublisher:
    """Publishes benchmark runs to a topic and tracks delivery outcomes."""

    def __init__(self, brokers: str, topic: str, client_id: str = "benchmark-producer") -> None:
        """Configure the underlying producer.

        Idempotence is on, so a retry after a transient broker error does not
        append a duplicate. Batching is traded for a small latency cost, which
        is the right way round for a benchmark farm feeding an analytics lake.
        """
        self._topic = topic
        self._delivered = 0
        self._failed = 0
        self._producer = Producer(
            {
                "bootstrap.servers": brokers,
                "client.id": client_id,
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 50,
                "batch.num.messages": 1000,
                "retries": 5,
                "retry.backoff.ms": 200,
            }
        )

    @property
    def delivered(self) -> int:
        """Messages the broker acknowledged."""
        return self._delivered

    @property
    def failed(self) -> int:
        """Messages that exhausted their retries."""
        return self._failed

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self._failed += 1
            logger.error("delivery failed for key %s: %s", msg.key(), err)
        else:
            self._delivered += 1

    def publish(self, run: BenchmarkRun) -> None:
        """Queue one run for delivery.

        Serves the delivery callback queue on every call, and drains it when
        the local queue is full rather than dropping the message.
        """
        try:
            self._producer.produce(
                topic=self._topic,
                key=run.to_kafka_key(),
                value=run.to_kafka_value(),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            logger.warning("local queue full, waiting for the broker to catch up")
            self._producer.flush(10.0)
            self._producer.produce(
                topic=self._topic,
                key=run.to_kafka_key(),
                value=run.to_kafka_value(),
                on_delivery=self._on_delivery,
            )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until the queue drains. Returns the number still in flight."""
        return int(self._producer.flush(timeout))

    def __enter__(self) -> BenchmarkPublisher:
        """Enter a context that guarantees a final flush."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Flush on the way out so a shutdown does not lose queued runs."""
        remaining = self.flush()
        if remaining:
            logger.error("%d messages were still queued at shutdown", remaining)
