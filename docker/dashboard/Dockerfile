# syntax=docker/dockerfile:1

# Serves the Streamlit app and also runs dbt. Both talk to the same DuckDB
# file and neither needs Spark or a Kafka client, so they share one image.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY pyproject.toml README.md ./
RUN mkdir -p src/common src/dashboard \
 && touch src/common/__init__.py src/dashboard/__init__.py \
 && pip install ".[dashboard,analytics]" \
 && pip uninstall --yes chip-benchmark-pipeline \
 && rm -rf src

COPY src/common ./src/common
COPY src/dashboard ./src/dashboard

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request as r; r.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "src/dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0"]
