# =====================================================================
# Pipeline exporter image.
#
# Same shape as ingestion/Dockerfile: non-root, no compiler toolchain in
# the runtime image. A process that only reads Postgres and ClickHouse
# and serves /metrics has no business running as root.
# =====================================================================
FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .
RUN pip wheel --wheel-dir /wheels -r requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    EXPORTER_PORT=9105

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10002 exporter

WORKDIR /app
COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-index --find-links=/wheels -r requirements.txt && rm -rf /wheels

COPY pipeline_exporter ./pipeline_exporter

USER exporter

EXPOSE 9105

# curl rather than a Python check: exercises the actual HTTP path an
# operator (or Prometheus) uses, not just "the interpreter can import".
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -fs http://localhost:${EXPORTER_PORT}/metrics || exit 1

ENTRYPOINT ["python", "-m", "pipeline_exporter"]
