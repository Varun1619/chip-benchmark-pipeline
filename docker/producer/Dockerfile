# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Install the producer extra from an empty package tree, then drop the project
# itself. Dependencies stay in this layer and only rebuild when pyproject.toml
# changes, so editing source code does not reinstall confluent-kafka.
COPY pyproject.toml README.md ./
RUN mkdir -p src/common src/producer \
 && touch src/common/__init__.py src/producer/__init__.py \
 && pip install ".[producer]" \
 && pip uninstall --yes chip-benchmark-pipeline \
 && rm -rf src

COPY src/common ./src/common
COPY src/producer ./src/producer

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "producer.main"]
