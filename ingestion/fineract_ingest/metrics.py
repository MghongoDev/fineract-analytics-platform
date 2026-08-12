"""Prometheus instrumentation for the ingestion job.

Ingestion is a *batch* process: it starts, works, exits. A pull-based
scraper would almost always find nothing listening, so the job pushes its
final metric values to a Pushgateway keyed by ``job=fineract_ingestion``
and ``entity=<name>``. Long-lived components (the pipeline exporter,
Airflow, ClickHouse, Kafka) are scraped normally - see
``observability/prometheus/prometheus.yml``.

The metric set is chosen to answer four operational questions:

1. Did it run?            -> ``..._last_success_timestamp_seconds``
2. Did it work?           -> ``..._rows_*`` / ``..._runs_total{status}``
3. Was the data good?     -> ``..._rejected_rows_total`` / ``..._expectation_failures``
4. Was the source slow?   -> ``..._api_request_duration_seconds`` / ``..._api_retries_total``
"""

from __future__ import annotations

import time
from typing import Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

from .logging_setup import get_logger

log = get_logger(__name__)

NAMESPACE = "fineract_ingest"


class IngestionMetrics:
    """One registry per process; pushed once at the end of the run."""

    def __init__(self, pushgateway_url: str = "", enabled: bool = True,
                 environment: str = "local"):
        self.registry = CollectorRegistry()
        self.pushgateway_url = pushgateway_url
        self.enabled = enabled and bool(pushgateway_url)
        self.environment = environment

        labels = ["entity", "environment"]

        self.rows_read = Counter(
            f"{NAMESPACE}_rows_read_total",
            "Records read from the Fineract API.", labels, registry=self.registry)
        self.rows_inserted = Counter(
            f"{NAMESPACE}_rows_inserted_total",
            "Records inserted into the OLTP landing tables.", labels,
            registry=self.registry)
        self.rows_updated = Counter(
            f"{NAMESPACE}_rows_updated_total",
            "Records whose payload changed and were updated.", labels,
            registry=self.registry)
        self.rows_unchanged = Counter(
            f"{NAMESPACE}_rows_unchanged_total",
            "Records skipped because the payload hash was identical "
            "(no WAL, no CDC event).", labels, registry=self.registry)
        self.rows_rejected = Counter(
            f"{NAMESPACE}_rows_rejected_total",
            "Records quarantined in meta.ingestion_reject.", labels,
            registry=self.registry)

        self.runs_total = Counter(
            f"{NAMESPACE}_runs_total", "Ingestion runs by outcome.",
            labels + ["status"], registry=self.registry)

        self.duration = Histogram(
            f"{NAMESPACE}_duration_seconds", "Wall-clock duration of an entity load.",
            labels, registry=self.registry,
            buckets=(1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600))

        self.api_requests = Counter(
            f"{NAMESPACE}_api_requests_total", "HTTP requests issued to Fineract.",
            labels, registry=self.registry)
        self.api_retries = Counter(
            f"{NAMESPACE}_api_retries_total", "HTTP requests retried.", labels,
            registry=self.registry)
        self.api_latency = Histogram(
            f"{NAMESPACE}_api_request_duration_seconds",
            "Mean Fineract response latency observed during the run.", labels,
            registry=self.registry,
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))

        self.last_success = Gauge(
            f"{NAMESPACE}_last_success_timestamp_seconds",
            "Unix timestamp of the last successful load - freshness alerts key off this.",
            labels, registry=self.registry)
        self.expectation_failures = Gauge(
            f"{NAMESPACE}_expectation_failures",
            "Failing data-quality expectations in the last run.",
            labels + ["severity"], registry=self.registry)
        self.table_rows = Gauge(
            f"{NAMESPACE}_table_rows",
            "Row count of the landing table after the load.", labels,
            registry=self.registry)

    # ------------------------------------------------------------------
    def _labels(self, entity: str) -> dict[str, str]:
        return {"entity": entity, "environment": self.environment}

    def record_load(self, entity: str, *, rows_read: int, rows_inserted: int,
                    rows_updated: int, rows_unchanged: int, rows_rejected: int,
                    duration_seconds: float, api_requests: int, api_retries: int,
                    mean_latency_seconds: float, status: str,
                    table_rows: Optional[int] = None) -> None:
        labels = self._labels(entity)
        self.rows_read.labels(**labels).inc(rows_read)
        self.rows_inserted.labels(**labels).inc(rows_inserted)
        self.rows_updated.labels(**labels).inc(rows_updated)
        self.rows_unchanged.labels(**labels).inc(rows_unchanged)
        self.rows_rejected.labels(**labels).inc(rows_rejected)
        self.runs_total.labels(**labels, status=status).inc()
        self.duration.labels(**labels).observe(duration_seconds)
        self.api_requests.labels(**labels).inc(api_requests)
        self.api_retries.labels(**labels).inc(api_retries)
        if mean_latency_seconds > 0:
            self.api_latency.labels(**labels).observe(mean_latency_seconds)
        if status == "success":
            self.last_success.labels(**labels).set(time.time())
        if table_rows is not None:
            self.table_rows.labels(**labels).set(table_rows)

    def record_expectations(self, entity: str, errors: int, warnings: int) -> None:
        labels = self._labels(entity)
        self.expectation_failures.labels(**labels, severity="error").set(errors)
        self.expectation_failures.labels(**labels, severity="warn").set(warnings)

    def push(self, job: str = "fineract_ingestion") -> None:
        if not self.enabled:
            log.debug("metrics_push_disabled")
            return
        try:
            push_to_gateway(self.pushgateway_url, job=job, registry=self.registry)
            log.info("metrics_pushed", extra={"gateway": self.pushgateway_url, "job": job})
        except Exception as exc:  # never fail a good load because telemetry is down
            log.warning("metrics_push_failed", extra={"error": str(exc)})
