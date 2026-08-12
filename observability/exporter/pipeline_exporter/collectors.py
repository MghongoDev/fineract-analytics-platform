"""Collectors: one class per source-of-truth query.

Each collector is independent and is run inside its own try/except by
``exporter.py``, so a broken ClickHouse view or a locked Postgres table
degrades one metric family rather than blanking the whole scrape - the
exporter itself is part of the thing being monitored, and it must not be
the single point of failure it exists to warn about.

Every "totals" gauge here (``*_total`` naming) is recomputed from the
source of truth on every scrape rather than accumulated in-process. That
is a deliberate departure from the usual Prometheus Counter pattern: an
in-process counter resets to zero on every exporter restart and produces
a false "everything dropped to zero" spike. Reading the absolute count
from Postgres/ClickHouse each cycle survives exporter restarts and is what
every other pull-based exporter (postgres_exporter, node_exporter) does
for the same reason.

Entity <-> table mapping mirrors the CDC capture set declared in
``platform/postgres/init/04_cdc_publication.sql`` and the reconciliation
view in ``platform/clickhouse/init/05_ops_views.sql``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol

# Reconciliation entities: (entity name, Postgres table, Postgres PK column).
# Must match fineract_ops.v_reconciliation_counts exactly, entity for entity.
RECONCILIATION_ENTITIES: tuple[tuple[str, str, str], ...] = (
    ("clients", "oltp.clients", "client_id"),
    ("loans", "oltp.loans", "loan_id"),
    ("loan_transactions", "oltp.loan_transactions", "transaction_id"),
    ("savings_accounts", "oltp.savings_accounts", "savings_id"),
    ("offices", "oltp.offices", "office_id"),
    ("staff", "oltp.staff", "staff_id"),
    ("loan_products", "oltp.loan_products", "product_id"),
    ("savings_products", "oltp.savings_products", "product_id"),
)

# CDC op codes emitted by Debezium: c=create, u=update, d=delete, r=snapshot read.
_OP_COLUMNS = (("c", "creates"), ("u", "updates"), ("d", "deletes"), ("r", "snapshot_reads"))


class PgExecutor(Protocol):
    def query(self, sql: str) -> list[tuple[Any, ...]]: ...


class ChExecutor(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class MetricSample:
    """One observation to write into a Gauge/Counter: labels + value."""

    labels: Mapping[str, str]
    value: float


class Collector(Protocol):
    """Every collector exposes a stable name (used in the error/duration
    metrics) and a ``collect`` method that returns metric-name -> samples.
    """

    name: str

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, Iterable[MetricSample]]: ...


# ---------------------------------------------------------------------
# Postgres-sourced collectors
# ---------------------------------------------------------------------
class IngestionRunCollector:
    """Did it run, and did it work? See meta.ingestion_run /
    meta.v_latest_ingestion_run - the single source of truth the
    ingestion job itself writes to at the end of every load.
    """

    name = "ingestion_run"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        latest = pg.query("""
            SELECT entity, status, rows_inserted + rows_updated AS rows_last_run
            FROM meta.v_latest_ingestion_run
        """)
        last_success = pg.query("""
            SELECT entity, EXTRACT(EPOCH FROM max(finished_at))
            FROM meta.ingestion_run
            WHERE status = 'success' AND finished_at IS NOT NULL
            GROUP BY entity
        """)

        run_status: list[MetricSample] = []
        rows_last_run: list[MetricSample] = []
        for entity, status, rows in latest:
            run_status.append(MetricSample({"entity": entity}, 1.0 if status == "success" else 0.0))
            rows_last_run.append(MetricSample({"entity": entity}, float(rows or 0)))

        last_success_ts = [
            MetricSample({"entity": entity}, float(epoch))
            for entity, epoch in last_success if epoch is not None
        ]

        return {
            "run_status": run_status,
            "rows_last_run": rows_last_run,
            "last_success_timestamp_seconds": last_success_ts,
        }


class IngestionRejectCollector:
    """Was the data good? Total quarantined rows per entity - the
    exporter's view of meta.ingestion_reject, the dead-letter table the
    ingestion job writes bad records to instead of failing the batch.
    """

    name = "ingestion_reject"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = pg.query("SELECT entity, count(*) FROM meta.ingestion_reject GROUP BY entity")
        return {
            "rejected_rows_total": [
                MetricSample({"entity": entity}, float(count)) for entity, count in rows
            ]
        }


class ReplicationSlotCollector:
    """Is the CDC pipeline's source-side foothold healthy?

    An inactive or fast-growing slot is the most destructive CDC failure
    mode there is: Postgres retains WAL for an inactive slot forever,
    which eventually fills the data disk and takes the OLTP database down
    - not just analytics. See cdc/README.md, "A heartbeat, with an action
    query".
    """

    name = "replication_slot"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = pg.query("""
            SELECT slot_name, active,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
            FROM pg_replication_slots
        """)
        lag = []
        active = []
        for slot_name, is_active, lag_bytes in rows:
            lag.append(MetricSample({"slot": slot_name}, float(lag_bytes or 0)))
            active.append(MetricSample({"slot": slot_name}, 1.0 if is_active else 0.0))
        return {"replication_slot_lag_bytes": lag, "replication_slot_active": active}


# ---------------------------------------------------------------------
# ClickHouse-sourced collectors
# ---------------------------------------------------------------------
class CdcFreshnessCollector:
    """How current is the data, and how much of that is pipeline latency
    versus a quiet source? See fineract_ops.v_cdc_freshness for why the
    two are kept separate.
    """

    name = "cdc_freshness"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = ch.query("""
            SELECT source_table, freshness_seconds, lag_p50_seconds, lag_p95_seconds,
                   creates, updates, deletes, snapshot_reads
            FROM fineract_ops.v_cdc_freshness
        """)
        freshness, p50, p95, events = [], [], [], []
        for r in rows:
            table = r["source_table"]
            freshness.append(MetricSample({"table": table}, float(r["freshness_seconds"])))
            p50.append(MetricSample({"table": table}, float(r["lag_p50_seconds"] or 0)))
            p95.append(MetricSample({"table": table}, float(r["lag_p95_seconds"] or 0)))
            for op, column in _OP_COLUMNS:
                events.append(MetricSample({"table": table, "op": op}, float(r[column] or 0)))
        return {
            "freshness_seconds": freshness,
            "lag_p50_seconds": p50,
            "lag_p95_seconds": p95,
            "events_total": events,
        }


class RawTableHealthCollector:
    """Row counts and staleness for every raw table, including ones with
    no CDC audit stream (dimension tables loaded less frequently).
    """

    name = "raw_table_health"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = ch.query("""
            SELECT table_name, row_count, seconds_since_insert
            FROM fineract_ops.v_raw_table_health
        """)
        return {
            "table_rows": [
                MetricSample({"table": r["table_name"]}, float(r["row_count"])) for r in rows
            ],
            "table_seconds_since_insert": [
                MetricSample({"table": r["table_name"]}, float(r["seconds_since_insert"] or 0))
                for r in rows
            ],
        }


class CdcParseErrorCollector:
    """Poison messages that could not be decoded by a materialized view.
    Any non-zero value here means an event was quarantined instead of
    silently dropped - see fineract_raw.cdc_errors.
    """

    name = "cdc_parse_errors"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = ch.query("SELECT topic, count() AS errors FROM fineract_raw.cdc_errors GROUP BY topic")
        return {
            "parse_errors_total": [
                MetricSample({"topic": r["topic"]}, float(r["errors"])) for r in rows
            ]
        }


class ReconciliationCollector:
    """Postgres row count minus ClickHouse distinct live keys, per entity.

    This is the only check in the whole metric set that catches a CDC
    event that was silently dropped rather than merely delayed: every
    other signal (freshness, lag, slot health) can look perfectly healthy
    while a handful of rows never made it across because of a poison
    message that predates the cdc_errors table, a mid-flight schema
    change, or a bug in a materialized view's WHERE clause. A non-zero,
    non-shrinking delta is the trigger to re-run an incremental snapshot
    (see cdc/README.md "ad-hoc re-snapshot").
    """

    name = "reconciliation"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        ch_rows = ch.query("SELECT entity, live_keys FROM fineract_ops.v_reconciliation_counts")
        ch_counts = {r["entity"]: int(r["live_keys"]) for r in ch_rows}

        deltas: list[MetricSample] = []
        for entity, pg_table, pg_key in RECONCILIATION_ENTITIES:
            pg_rows = pg.query(f"SELECT count(*) FROM {pg_table}")
            pg_count = int(pg_rows[0][0]) if pg_rows else 0
            ch_count = ch_counts.get(entity, 0)
            deltas.append(MetricSample({"entity": entity}, float(pg_count - ch_count)))
        return {"row_delta": deltas}


class MergeHealthCollector:
    """Part-count and size pressure per ClickHouse table. ReplacingMergeTree
    only collapses duplicate versions on merge, so an unbounded part count
    is a leading indicator of slow (and briefly wrong-looking) queries.
    """

    name = "merge_health"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        rows = ch.query("SELECT database, table, active_parts, size_mb FROM fineract_ops.v_merge_health")
        parts, bytes_ = [], []
        for r in rows:
            labels = {"database": r["database"], "table": r["table"]}
            parts.append(MetricSample(labels, float(r["active_parts"])))
            # size_mb is already rounded in the view; converting back to
            # bytes keeps the metric's declared unit consistent with the
            # rest of the exporter (Prometheus convention: base units).
            bytes_.append(MetricSample(labels, float(r["size_mb"]) * 1024 * 1024))
        return {"active_parts": parts, "table_bytes": bytes_}


class DbtResultsCollector:
    """Was the last transform run correct and how long did it take?
    Sourced from the dbt artefacts Airflow publishes after every run
    (fineract_ops.dbt_test_results / dbt_model_runs).
    """

    name = "dbt_results"

    def collect(self, pg: PgExecutor, ch: ChExecutor) -> Mapping[str, list[MetricSample]]:
        test_rows = ch.query("SELECT layer, failed FROM fineract_ops.v_dbt_latest_run")
        model_rows = ch.query("""
            SELECT model_name, execution_time
            FROM fineract_ops.dbt_model_runs
            WHERE invocation_id = (
                SELECT invocation_id FROM fineract_ops.dbt_model_runs
                ORDER BY executed_at DESC LIMIT 1
            )
        """)
        return {
            "test_failures": [
                MetricSample({"layer": r["layer"]}, float(r["failed"])) for r in test_rows
            ],
            "model_runtime_seconds": [
                MetricSample({"model": r["model_name"]}, float(r["execution_time"]))
                for r in model_rows
            ],
        }


ALL_COLLECTORS: tuple[Collector, ...] = (
    IngestionRunCollector(),
    IngestionRejectCollector(),
    ReplicationSlotCollector(),
    CdcFreshnessCollector(),
    RawTableHealthCollector(),
    CdcParseErrorCollector(),
    ReconciliationCollector(),
    MergeHealthCollector(),
    DbtResultsCollector(),
)
