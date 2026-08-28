"""Entry point for the streaming consumer.

Starts two queries against one topic read: usable runs into the lake, rejects
into quarantine. Each has its own checkpoint, so both recover independently
and neither can lose the other's progress.
"""

from __future__ import annotations

import logging
import signal
import threading

from pyspark.sql.streaming import StreamingQuery

from common.config import get_settings
from common.logging_setup import configure_logging
from consumer.stream import (
    build_spark_session,
    parse_runs,
    quarantined_runs,
    read_kafka_stream,
    start_lake_query,
    start_quarantine_query,
    valid_runs,
)

logger = logging.getLogger("consumer")

_shutdown = threading.Event()

# How often to report progress while waiting for the queries.
PROGRESS_INTERVAL_S = 30


def _install_signal_handlers() -> None:
    """Turn SIGINT and SIGTERM into a clean stop of both queries."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: _shutdown.set())


def _log_progress(queries: list[StreamingQuery]) -> None:
    for query in queries:
        progress = query.lastProgress
        if not progress:
            logger.info("%s: no batch completed yet", query.name)
            continue
        logger.info(
            "%s: batch=%s rows=%s rows/s=%.1f",
            query.name,
            progress.get("batchId"),
            progress.get("numInputRows"),
            progress.get("processedRowsPerSecond") or 0.0,
        )


def _stop(queries: list[StreamingQuery]) -> None:
    for query in queries:
        if query.isActive:
            logger.info("stopping %s", query.name)
            query.stop()


def main() -> int:
    """Start both queries and supervise them until stopped."""
    settings = get_settings()
    configure_logging(settings.log_level)
    _install_signal_handlers()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "consumer starting: spark=%s brokers=%s topic=%s lake=%s trigger=%s",
        spark.version,
        settings.redpanda_brokers,
        settings.benchmark_topic,
        settings.raw_runs_path,
        settings.spark_trigger_interval,
    )

    parsed = parse_runs(
        read_kafka_stream(spark, settings.redpanda_brokers, settings.benchmark_topic)
    )
    queries = [
        start_lake_query(
            valid_runs(parsed),
            str(settings.raw_runs_path),
            str(settings.checkpoint_path),
            settings.spark_trigger_interval,
        ),
        start_quarantine_query(
            quarantined_runs(parsed),
            str(settings.quarantine_path),
            str(settings.quarantine_checkpoint_path),
            settings.spark_trigger_interval,
        ),
    ]

    exit_code = 0
    try:
        while not _shutdown.is_set():
            if spark.streams.awaitAnyTermination(timeout=PROGRESS_INTERVAL_S):
                logger.error("a query terminated on its own, shutting the job down")
                exit_code = 1
                break
            _log_progress(queries)
    except Exception:
        logger.exception("streaming job failed")
        exit_code = 1
    finally:
        _stop(queries)
        _log_progress(queries)
        spark.stop()
        logger.info("consumer stopped")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
