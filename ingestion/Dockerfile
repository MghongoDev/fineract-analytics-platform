# =====================================================================
# Ingestion service image.
#
# Multi-stage: wheels are built in a throwaway stage so the runtime image
# carries no compiler toolchain. Runs as a non-root user - a job that only
# needs to read an API and write to Postgres has no business being root.
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
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 ingest

WORKDIR /app
COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-index --find-links=/wheels -r requirements.txt && rm -rf /wheels

COPY pyproject.toml README.md ./
COPY fineract_ingest ./fineract_ingest
RUN pip install --no-deps -e .

USER ingest

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import fineract_ingest; print('ok')" || exit 1

ENTRYPOINT ["python", "-m", "fineract_ingest"]
CMD ["--help"]
