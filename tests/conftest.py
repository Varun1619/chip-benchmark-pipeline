"""Shared test fixtures."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    """A local Spark session, shared across the module to amortise JVM startup.

    Building a session is not proof that Spark works. On an unsupported JVM, or
    with a JVM agent injected by another tool, the session starts and then every
    task dies with a socket reset. So this runs a real job before handing the
    session over, and skips on failure.

    It never skips on CI, where Spark has to work and a failure has to be loud.
    """
    pytest.importorskip("pyspark", reason="the consumer extra is not installed")
    from consumer.stream import build_spark_session

    session = None
    try:
        session = build_spark_session("chip-benchmark-tests")
        session.sparkContext.setLogLevel("ERROR")
        # This has to round trip through a Python worker, the way the tests do.
        # A JVM only job such as range().count() succeeds even on a broken
        # setup and would let the skip guard pass when it should not.
        probe = session.createDataFrame([(1,), (2,)], "n int")
        assert [row["n"] for row in probe.filter("n > 1").collect()] == [2]
    except Exception as exc:  # noqa: BLE001 - any JVM failure means skip locally
        if os.environ.get("CI"):
            raise
        if session is not None:
            session.stop()
        pytest.skip(f"Spark cannot execute a job on this machine: {exc}")

    yield session
    session.stop()
