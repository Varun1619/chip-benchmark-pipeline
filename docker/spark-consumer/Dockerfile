# syntax=docker/dockerfile:1

# PySpark 3.5 needs a JVM and does not support Python 3.13, so the Python and
# Java versions are both pinned here. The Debian release is pinned too: trixie
# dropped openjdk-17 for 21, and Spark 3.5 supports Java 8, 11 and 17 only.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

# procps gives Spark the ps binary it shells out to for executor bookkeeping.
RUN apt-get update \
 && apt-get install --no-install-recommends --yes \
      openjdk-17-jre-headless \
      curl \
      procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
RUN mkdir -p src/common src/consumer \
 && touch src/common/__init__.py src/consumer/__init__.py \
 && pip install ".[consumer]" \
 && pip uninstall --yes chip-benchmark-pipeline \
 && rm -rf src

# Vendor the Kafka connector jars into the PySpark install. Passing them as
# --packages instead would make every container start depend on Maven Central
# and on a writable Ivy cache.
RUN set -eux; \
    jars="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / "jars")')"; \
    maven="https://repo1.maven.org/maven2"; \
    curl -fsSL -o "${jars}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${maven}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${jars}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${maven}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${jars}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "${maven}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -o "${jars}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "${maven}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

COPY src/common ./src/common
COPY src/consumer ./src/consumer

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 4040

CMD ["python", "-m", "consumer.main"]
