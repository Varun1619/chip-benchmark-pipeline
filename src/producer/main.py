"""Entry point for the synthetic benchmark producer.

Runs in two phases. A backfill publishes history with timestamps spread over
the past `PRODUCER_BACKFILL_DAYS`, so the analytics layer has rolling baselines
to work with the moment the stack starts. The live phase then publishes at
`PRODUCER_EVENTS_PER_SECOND` until stopped.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import UTC, datetime, timedelta

from common.config import Settings, get_settings
from common.logging_setup import configure_logging
from producer.generator import BenchmarkRunGenerator
from producer.publisher import BenchmarkPublisher

logger = logging.getLogger("producer")

_shutdown = threading.Event()

BACKFILL_LOG_EVERY = 2000
LIVE_LOG_EVERY = 500


def _install_signal_handlers() -> None:
    """Turn SIGINT and SIGTERM into a clean stop so queued runs still flush."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: _shutdown.set())


def _backfill_total(settings: Settings) -> int:
    total = settings.producer_backfill_days * settings.producer_backfill_runs_per_day
    if settings.producer_max_events:
        total = min(total, settings.producer_max_events)
    return total


def run_backfill(
    generator: BenchmarkRunGenerator,
    publisher: BenchmarkPublisher,
    settings: Settings,
    now: datetime,
) -> int:
    """Publish history as fast as the broker accepts it. Returns runs published.

    Timestamps ascend, so runs for a configuration arrive on its partition in
    the order they happened. The consumer and the baseline models both depend
    on that.
    """
    total = _backfill_total(settings)
    if total == 0:
        logger.info("backfill disabled")
        return 0

    start = now - timedelta(days=settings.producer_backfill_days)
    span_s = (now - start).total_seconds()
    logger.info(
        "backfilling %d runs over %d days from %s",
        total,
        settings.producer_backfill_days,
        start.date(),
    )

    published = 0
    began = time.monotonic()
    for index in range(total):
        if _shutdown.is_set():
            logger.warning("backfill interrupted after %d runs", published)
            break
        occurred_at = start + timedelta(seconds=span_s * index / total)
        publisher.publish(generator.generate(occurred_at, now=now))
        published += 1
        if published % BACKFILL_LOG_EVERY == 0:
            logger.info("backfill %d/%d", published, total)

    publisher.flush()
    elapsed = time.monotonic() - began
    logger.info(
        "backfill done: %d runs in %.1fs (%.0f/s), delivered=%d failed=%d",
        published,
        elapsed,
        published / elapsed if elapsed else 0.0,
        publisher.delivered,
        publisher.failed,
    )
    return published


def run_live(
    generator: BenchmarkRunGenerator,
    publisher: BenchmarkPublisher,
    settings: Settings,
    already_published: int,
) -> int:
    """Publish at the configured rate until stopped or the cap is reached."""
    interval_s = 1.0 / settings.producer_events_per_second
    logger.info(
        "live phase at %.1f runs/s (cap=%s)",
        settings.producer_events_per_second,
        settings.producer_max_events or "none",
    )

    published = already_published
    next_due = time.monotonic()
    while not _shutdown.is_set():
        if settings.producer_max_events and published >= settings.producer_max_events:
            logger.info("reached the %d event cap", settings.producer_max_events)
            break

        now = datetime.now(tz=UTC)
        publisher.publish(generator.generate(now, now=now))
        published += 1
        if published % LIVE_LOG_EVERY == 0:
            logger.info("published %d runs, delivered=%d", published, publisher.delivered)

        next_due += interval_s
        wait_s = next_due - time.monotonic()
        if wait_s > 0:
            _shutdown.wait(wait_s)
        else:
            # Behind schedule, so stop trying to catch up and reset the clock.
            next_due = time.monotonic()

    return published


def main() -> int:
    """Configure, run both phases, and report the delivery outcome."""
    settings = get_settings()
    configure_logging(settings.log_level)
    _install_signal_handlers()

    generator = BenchmarkRunGenerator(seed=settings.producer_seed)
    logger.info(
        "producer starting: brokers=%s topic=%s configs=%d seed=%d",
        settings.redpanda_brokers,
        settings.benchmark_topic,
        generator.config_count,
        settings.producer_seed,
    )

    now = datetime.now(tz=UTC)
    with BenchmarkPublisher(settings.redpanda_brokers, settings.benchmark_topic) as publisher:
        published = run_backfill(generator, publisher, settings, now)
        published = run_live(generator, publisher, settings, published)
        logger.info(
            "stopping: published=%d delivered=%d failed=%d",
            published,
            publisher.delivered,
            publisher.failed,
        )
        failed = publisher.failed

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
