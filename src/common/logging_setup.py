"""One logging configuration for every service.

Logs go to stdout in a single line format, which is what `docker compose logs`
and any log shipper expect.
"""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"


def configure_logging(level: str = "INFO") -> None:
    """Send logs to stdout at the given level, replacing any prior handlers."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=LOG_FORMAT,
        stream=sys.stdout,
        force=True,
    )
    # The Kafka client and Spark are chatty at INFO and drown the useful lines.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)
