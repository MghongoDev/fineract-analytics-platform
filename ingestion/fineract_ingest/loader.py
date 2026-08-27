"""Postgres loading layer.

Guarantees
----------
**Idempotent.** Every load is an ``INSERT ... ON CONFLICT (pk) DO UPDATE``
keyed on the Fineract natural key. Re-running a batch - after a crash,
after a manual backfill, twice by accident - converges to the same state.

**Exactly-once with respect to Postgres.** The data rows, the watermark
update and the run record are written in ONE transaction. There is no
window in which the watermark claims progress the data does not have.

**Churn-free.** ``DO UPDATE ... WHERE _payload_hash IS DISTINCT FROM
EXCLUDED._payload_hash`` means an unchanged row produces no row version,
no WAL record, and therefore no CDC event. On a steady-state book that
takes the change stream from "everything, every run" to "only what
actually moved" - which is what makes the ClickHouse side cheap.

**Observable.** ``RETURNING (xmax = 0)`` distinguishes inserts from
updates without a second query, so every run reports
inserted / updated / unchanged / rejected.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from .config import PostgresConfig
from .logging_setup import get_logger
from .parsers import chunked
from .validation import ExpectationResult, RejectedRecord

log = get_logger(__name__)

AUDIT_COLUMNS = ("_source_system", "_payload_hash")


class LoadResult:
    """Outcome of loading one entity."""

    def __init__(self, entity: str):
        self.entity = entity
        self.rows_read = 0
        self.rows_inserted = 0
        self.rows_updated = 0
        self.rows_unchanged = 0
        self.rows_rejected = 0

    def merge(self, other: LoadResult) -> None:
        self.rows_read += other.rows_read
        self.rows_inserted += other.rows_inserted
        self.rows_updated += other.rows_updated
        self.rows_unchanged += other.rows_unchanged
        self.rows_rejected += other.rows_rejected

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_unchanged": self.rows_unchanged,
            "rows_rejected": self.rows_rejected,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"<LoadResult {self.entity} {self.as_dict()}>"


class PostgresLoader:
    def __init__(self, config: PostgresConfig | None = None,
                 connection: psycopg.Connection | None = None):
        self.config = config or PostgresConfig()
        self._external_connection = connection
        self._connection: psycopg.Connection | None = connection

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------
    def connect(self) -> psycopg.Connection:
        if self._connection is None or self._connection.closed:
            log.info("postgres_connecting", extra={"dsn": self.config.masked_dsn()})
            self._connection = psycopg.connect(self.config.dsn, autocommit=False)
            with self._connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SET statement_timeout = {}").format(
                        sql.Literal(self.config.statement_timeout_ms)))
            self._connection.commit()
        return self._connection

    def close(self) -> None:
        if self._connection and not self._connection.closed and not self._external_connection:
            self._connection.close()

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        connection = self.connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def __enter__(self) -> PostgresLoader:
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------
    def upsert(self, connection: psycopg.Connection, table: str,
               primary_key: str, rows: Sequence[Mapping[str, Any]]) -> LoadResult:
        """Bulk upsert a batch of mapped records into ``table``."""
        result = LoadResult(table)
        if not rows:
            return result

        schema_name, table_name = table.split(".", 1)
        columns = list(rows[0].keys())
        update_columns = [c for c in columns if c != primary_key]

        statement = sql.SQL("""
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE SET {assignments}
            WHERE {schema}.{table}._payload_hash IS DISTINCT FROM EXCLUDED._payload_hash
            RETURNING (xmax = 0) AS was_inserted
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            pk=sql.Identifier(primary_key),
            assignments=sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in update_columns),
        )

        with connection.cursor() as cursor:
            for chunk in chunked(list(rows), self.config.batch_size):
                affected = 0
                for row in chunk:
                    cursor.execute(statement, [row.get(c) for c in columns])
                    returned = cursor.fetchone()
                    if returned is not None:
                        affected += 1
                        if returned[0]:
                            result.rows_inserted += 1
                        else:
                            result.rows_updated += 1
                result.rows_unchanged += len(chunk) - affected
                result.rows_read += len(chunk)
        return result

    # ------------------------------------------------------------------
    # Control plane
    # ------------------------------------------------------------------
    def start_run(self, connection: psycopg.Connection, entity: str,
                  batch_id: uuid.UUID, dag_run_id: str | None) -> int:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta.ingestion_run (batch_id, entity, dag_run_id, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING run_id
                """,
                (str(batch_id), entity, dag_run_id),
            )
            row = cursor.fetchone()
        connection.commit()          # visible immediately: a crashed run stays 'running'
        return int(row[0])

    def finish_run(self, connection: psycopg.Connection, run_id: int, status: str,
                   result: LoadResult, api_requests: int = 0, api_retries: int = 0,
                   error_message: str | None = None) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE meta.ingestion_run
                   SET finished_at      = now(),
                       duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
                       status           = %s,
                       rows_read        = %s,
                       rows_inserted    = %s,
                       rows_updated     = %s,
                       rows_unchanged   = %s,
                       rows_rejected    = %s,
                       api_requests     = %s,
                       api_retries      = %s,
                       error_message    = %s
                 WHERE run_id = %s
                """,
                (status, result.rows_read, result.rows_inserted, result.rows_updated,
                 result.rows_unchanged, result.rows_rejected, api_requests,
                 api_retries, error_message, run_id),
            )

    def update_watermark(self, connection: psycopg.Connection, entity: str,
                         cursor_value: str | None, row_count: int) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta.ingestion_watermark
                    (entity, last_success_at, last_cursor, last_row_count,
                     total_rows_loaded, updated_at)
                VALUES (%s, now(), %s, %s, %s, now())
                ON CONFLICT (entity) DO UPDATE
                   SET last_success_at   = EXCLUDED.last_success_at,
                       last_cursor       = COALESCE(EXCLUDED.last_cursor,
                                                    meta.ingestion_watermark.last_cursor),
                       last_row_count    = EXCLUDED.last_row_count,
                       total_rows_loaded = meta.ingestion_watermark.total_rows_loaded
                                           + EXCLUDED.last_row_count,
                       updated_at        = now()
                """,
                (entity, cursor_value, row_count, row_count),
            )

    def read_watermark(self, entity: str) -> dict[str, Any] | None:
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT entity, last_success_at, last_cursor, last_row_count,
                       total_rows_loaded
                  FROM meta.ingestion_watermark
                 WHERE entity = %s
                """, (entity,))
            row = cursor.fetchone()
        connection.commit()
        if not row:
            return None
        return {
            "entity": row[0], "last_success_at": row[1], "last_cursor": row[2],
            "last_row_count": row[3], "total_rows_loaded": row[4],
        }

    def record_rejects(self, connection: psycopg.Connection, batch_id: uuid.UUID,
                       rejects: Iterable[RejectedRecord]) -> int:
        rows = list(rejects)
        if not rows:
            return 0
        with connection.cursor() as cursor:
            for reject in rows:
                cursor.execute(
                    """
                    INSERT INTO meta.ingestion_reject
                        (batch_id, entity, source_key, rule, error_message, payload)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (str(batch_id), reject.entity, reject.source_key, reject.rule,
                     reject.error_message, Json(dict(reject.payload))),
                )
        return len(rows)

    def record_expectations(self, connection: psycopg.Connection, batch_id: uuid.UUID,
                            entity: str, results: Iterable[ExpectationResult],
                            layer: str = "ingestion") -> None:
        with connection.cursor() as cursor:
            for item in results:
                cursor.execute(
                    """
                    INSERT INTO meta.data_quality_result
                        (batch_id, layer, entity, check_name, severity, passed,
                         observed_value, threshold_value, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (str(batch_id), layer, entity, item.name, item.severity,
                     item.passed, item.observed_value, item.threshold_value,
                     item.details),
                )

    # ------------------------------------------------------------------
    # Helpers used by parent/child ingestion and by the Airflow sensors
    # ------------------------------------------------------------------
    def fetch_parent_ids(self, query: str, limit: int | None = None) -> list[int]:
        connection = self.connect()
        statement = query if limit is None else f"{query} LIMIT {int(limit)}"
        with connection.cursor() as cursor:
            cursor.execute(statement)
            ids = [int(row[0]) for row in cursor.fetchall() if row[0] is not None]
        connection.commit()
        return ids

    def table_count(self, table: str) -> int:
        connection = self.connect()
        schema_name, table_name = table.split(".", 1)
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)))
            count = int(cursor.fetchone()[0])
        connection.commit()
        return count

    def replication_slot_status(self) -> list[dict[str, Any]]:
        """Slot lag in bytes - the single most important CDC health metric."""
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT slot_name, active, restart_lsn,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
                  FROM pg_replication_slots
                """)
            rows = [
                {"slot_name": r[0], "active": r[1], "restart_lsn": str(r[2]),
                 "lag_bytes": int(r[3] or 0)}
                for r in cursor.fetchall()
            ]
        connection.commit()
        return rows

    def touch_heartbeat(self) -> None:
        """Advance the CDC heartbeat so an idle slot still confirms an LSN."""
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE cdc.debezium_heartbeat SET beat_at = now() WHERE id = 1")
        connection.commit()


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def new_batch_id() -> uuid.UUID:
    return uuid.uuid4()
