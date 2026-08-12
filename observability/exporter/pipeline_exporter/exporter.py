"""The exporter process: wires collectors to Prometheus metrics and runs
the scrape loop.

Design choice: metrics are refreshed on a background timer, not on every
HTTP GET /metrics. A synchronous scrape-on-GET exporter ties Prometheus's
scrape_timeout to the slowest query against two live databases; a
background loop instead means /metrics always answers instantly with the
last known values, and a slow or hung ClickHouse query degrades
freshness rather than causing a scrape timeout that Prometheus treats as
"target down".

Gauges are label-cleared and rebuilt on every successful collector run so
that an entity/table that stops appearing (e.g. dropped from the CDC
capture set) does not leave a permanently stale time series behind.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import psycopg
import requests
from prometheus_client import Counter, Gauge, start_http_server

from .collectors import ALL_COLLECTORS, Collector, MetricSample
from .config import ExporterConfig

log = logging.getLogger("pipeline_exporter")

NAMESPACE = "fineract"


# ---------------------------------------------------------------------
# Executors: thin, reconnecting wrappers so a single dropped connection
# does not need the whole exporter process to restart.
# ---------------------------------------------------------------------
class PostgresExecutor:
    def __init__(self, cfg) -> None:
        self._cfg = cfg
        self._conn: psycopg.Connection | None = None

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.database,
            user=self._cfg.user,
            password=self._cfg.password,
            connect_timeout=self._cfg.connect_timeout,
            autocommit=True,
        )

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        except psycopg.Error:
            # One retry after a fresh connection - covers idle-connection
            # drops and Postgres restarts without failing the whole cycle.
            self._conn = self._connect()
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()


class ClickHouseExecutor:
    def __init__(self, cfg) -> None:
        self._cfg = cfg
        self._url = f"http://{cfg.host}:{cfg.http_port}/"

    def query(self, sql: str) -> list[dict[str, Any]]:
        response = requests.get(
            self._url,
            params={"query": f"{sql.strip().rstrip(';')} FORMAT JSON"},
            auth=(self._cfg.user, self._cfg.password),
            timeout=self._cfg.request_timeout,
        )
        response.raise_for_status()
        return response.json().get("data", [])


# ---------------------------------------------------------------------
# Metric name -> Prometheus object mapping.
#
# Keyed by (collector_name, sample_key) so each collector's ``collect()``
# output routes to exactly one Gauge, chosen from the metric list this
# exporter contracts to expose.
# ---------------------------------------------------------------------
def _build_gauges() -> dict[tuple[str, str], Gauge]:
    specs: dict[tuple[str, str], tuple[str, str, tuple[str, ...]]] = {
        ("ingestion_run", "run_status"): (
            "ingestion_run_status",
            "Outcome of the most recent ingestion run for this entity (1=success, 0=failed).",
            ("entity",),
        ),
        ("ingestion_run", "rows_last_run"): (
            "ingestion_rows_last_run",
            "Rows inserted or updated by the most recent ingestion run.",
            ("entity",),
        ),
        ("ingestion_run", "last_success_timestamp_seconds"): (
            "ingestion_last_success_timestamp_seconds",
            "Unix timestamp of the last successful ingestion run; freshness alerts key off this.",
            ("entity",),
        ),
        ("ingestion_reject", "rejected_rows_total"): (
            "ingestion_rejected_rows_total",
            "Records currently quarantined in meta.ingestion_reject for this entity.",
            ("entity",),
        ),
        ("replication_slot", "replication_slot_lag_bytes"): (
            "cdc_replication_slot_lag_bytes",
            "Bytes of WAL retained beyond the slot's confirmed flush LSN; "
            "an inactive or fast-growing slot fills the Postgres data disk.",
            ("slot",),
        ),
        ("replication_slot", "replication_slot_active"): (
            "cdc_replication_slot_active",
            "Whether the CDC replication slot has an active consumer (1=active, 0=inactive).",
            ("slot",),
        ),
        ("cdc_freshness", "freshness_seconds"): (
            "cdc_freshness_seconds",
            "Age of the newest row in ClickHouse measured against the source commit time.",
            ("table",),
        ),
        ("cdc_freshness", "lag_p50_seconds"): (
            "cdc_lag_p50_seconds",
            "Median Postgres-commit-to-ClickHouse-visible latency over the last 24h.",
            ("table",),
        ),
        ("cdc_freshness", "lag_p95_seconds"): (
            "cdc_lag_p95_seconds",
            "95th percentile Postgres-commit-to-ClickHouse-visible latency over the last 24h.",
            ("table",),
        ),
        ("cdc_freshness", "events_total"): (
            "cdc_events_total",
            "CDC change events observed in the last 24h, by table and operation type.",
            ("table", "op"),
        ),
        ("raw_table_health", "table_rows"): (
            "raw_table_rows",
            "Row count of the ClickHouse raw table.",
            ("table",),
        ),
        ("raw_table_health", "table_seconds_since_insert"): (
            "raw_table_seconds_since_insert",
            "Seconds since the last insert into the ClickHouse raw table.",
            ("table",),
        ),
        ("cdc_parse_errors", "parse_errors_total"): (
            "cdc_parse_errors_total",
            "Poison CDC messages quarantined in fineract_raw.cdc_errors by topic.",
            ("topic",),
        ),
        ("reconciliation", "row_delta"): (
            "reconciliation_row_delta",
            "Postgres row count minus ClickHouse distinct live keys; the only check that "
            "catches CDC events silently dropped rather than merely delayed.",
            ("entity",),
        ),
        ("merge_health", "active_parts"): (
            "clickhouse_active_parts",
            "Active parts per ClickHouse table; an unbounded count precedes slow and "
            "briefly-incorrect ReplacingMergeTree reads.",
            ("database", "table"),
        ),
        ("merge_health", "table_bytes"): (
            "clickhouse_table_bytes",
            "On-disk size of the ClickHouse table in bytes.",
            ("database", "table"),
        ),
        ("dbt_results", "test_failures"): (
            "dbt_test_failures",
            "Failing dbt tests in the most recent run, by warehouse layer.",
            ("layer",),
        ),
        ("dbt_results", "model_runtime_seconds"): (
            "dbt_model_runtime_seconds",
            "Execution time of each dbt model in the most recent run.",
            ("model",),
        ),
    }
    gauges: dict[tuple[str, str], Gauge] = {}
    for key, (suffix, help_text, labelnames) in specs.items():
        gauges[key] = Gauge(f"{NAMESPACE}_{suffix}", help_text, labelnames)
    return gauges


class PipelineExporter:
    """Owns the metric objects, the executors and the scrape loop."""

    def __init__(self, cfg: ExporterConfig, collectors: tuple[Collector, ...] = ALL_COLLECTORS):
        self._cfg = cfg
        self._collectors = collectors
        self._pg = PostgresExecutor(cfg.postgres)
        self._ch = ClickHouseExecutor(cfg.clickhouse)
        self._gauges = _build_gauges()

        self._collector_errors = Counter(
            "fineract_exporter_collector_errors_total",
            "Collector runs that raised an exception; a broken source must not blank the "
            "whole scrape, so each collector's failure is isolated and counted here.",
            ("collector",),
        )
        self._scrape_duration = Gauge(
            "fineract_exporter_scrape_duration_seconds",
            "Wall-clock time taken by the most recent run of each collector.",
            ("collector",),
        )

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    def run_once(self) -> None:
        """Run every collector once, isolating failures per collector."""
        for collector in self._collectors:
            start = time.monotonic()
            try:
                results = collector.collect(self._pg, self._ch)
                self._apply(collector.name, results)
            except Exception:  # a broken source must not blank the whole scrape
                self._collector_errors.labels(collector=collector.name).inc()
                log.exception("collector_failed", extra={"collector": collector.name})
            finally:
                self._scrape_duration.labels(collector=collector.name).set(
                    time.monotonic() - start)

    def _apply(self, collector_name: str, results: dict[str, list[MetricSample]]) -> None:
        for sample_key, samples in results.items():
            gauge = self._gauges.get((collector_name, sample_key))
            if gauge is None:
                log.warning("unmapped_metric", extra={"collector": collector_name, "key": sample_key})
                continue
            gauge.clear()  # drop stale label sets before repopulating
            for sample in samples:
                gauge.labels(**sample.labels).set(sample.value)

    # ------------------------------------------------------------------
    def _loop(self) -> None:
        while not self._stop.is_set():
            self.run_once()
            self._stop.wait(self._cfg.scrape_interval_seconds)

    def start(self) -> None:
        start_http_server(self._cfg.port)
        log.info("exporter_listening", extra={"port": self._cfg.port})
        # Populate metrics before the first scrape can ever hit an empty registry.
        self.run_once()
        self._thread = threading.Thread(target=self._loop, name="scrape-loop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
