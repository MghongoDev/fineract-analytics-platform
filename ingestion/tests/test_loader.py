"""Tests for the Postgres loading layer without a Postgres.

The loader is a thin, deterministic layer over psycopg. Every statement
is a straight line, so a fake connection that records what it was asked
to run and returns scripted rows exercises the real logic - chunking,
the ON CONFLICT bookkeeping, watermark/run bookkeeping - in milliseconds
instead of against a server.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from fineract_ingest.config import PostgresConfig
from fineract_ingest.loader import LoadResult, PostgresLoader
from fineract_ingest.validation import ExpectationResult, RejectedRecord


class FakeConnection:
    """Records executed SQL; returns scripted fetch results."""

    def __init__(self, results: list | None = None):
        self.results = list(results or [])
        self.executed: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, connection: FakeConnection):
        self.connection = connection

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, statement: object, params: object = None) -> None:
        self.connection.executed.append(str(statement))

    def fetchone(self):
        if self.connection.results:
            return self.connection.results.pop(0)
        return None

    def fetchall(self):
        rows = list(self.connection.results)
        self.connection.results.clear()
        return rows


@pytest.fixture
def loader():
    connection = FakeConnection()
    instance = PostgresLoader(config=PostgresConfig(batch_size=2), connection=connection)
    return _LoaderWithFake(instance, connection)


class _LoaderWithFake:
    """Bundles a real PostgresLoader with the fake connection it owns."""

    def __init__(self, loader: PostgresLoader, connection: FakeConnection):
        self.loader = loader
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.loader, name)


class TestConnectAndTransaction:
    def test_connect_returns_the_injected_connection(self, loader):
        assert loader.connect() is loader.connection

    def test_transaction_commits_on_success(self, loader):
        with loader.transaction():
            pass
        assert loader.connection.commits == 1
        assert loader.connection.rollbacks == 0

    def test_transaction_rolls_back_on_failure(self, loader):
        with pytest.raises(RuntimeError), loader.transaction():
            raise RuntimeError("boom")
        assert loader.connection.rollbacks == 1
        assert loader.connection.commits == 0

    def test_context_manager_connects_and_exits(self, loader):
        with loader.loader as instance:
            assert instance is loader.loader
            assert loader.connect() is loader.connection

    def test_close_leaves_an_injected_connection_open(self, loader):
        # The loader owns its own connections, not ones passed in for
        # testing or by the runtime host containers.
        loader.close()
        assert loader.connection.closed is False


class TestUpsert:
    def test_empty_batch_is_a_noop(self, loader):
        result = loader.upsert(loader.connection, "oltp.clients", "client_id", [])
        assert result.rows_read == 0
        assert loader.connection.executed == []

    def test_batch_is_chunked_and_counted(self, loader):
        rows = [
            {"client_id": 1, "display_name": "A"},
            {"client_id": 2, "display_name": "B"},
            {"client_id": 3, "display_name": "C"},
        ]
        loader.connection.results = [(True,), (False,), None]
        result = loader.upsert(loader.connection, "oltp.clients", "client_id", rows)
        assert result.rows_read == 3
        assert result.rows_inserted == 1
        assert result.rows_updated == 1
        assert result.rows_unchanged == 1
        assert result.rows_rejected == 0

    def test_upsert_statement_uses_on_conflict_and_returning(self, loader):
        rows = [{"client_id": 1, "display_name": "A"}]
        loader.connection.results = [(True,)]
        loader.upsert(loader.connection, "oltp.clients", "client_id", rows)
        statement = loader.connection.executed[0]
        assert "ON CONFLICT" in statement
        assert "DO UPDATE" in statement
        assert "RETURNING" in statement
        assert "_payload_hash IS DISTINCT FROM" in statement


class TestRunAndWatermark:
    def test_start_run_returns_the_run_id(self, loader):
        loader.connection.results = [(42,)]
        run_id = loader.start_run(loader.connection, "clients", uuid.uuid4(), "dag-1")
        assert run_id == 42
        assert loader.connection.commits == 1

    def test_finish_run_writes_all_counters(self, loader):
        result = LoadResult("clients")
        result.rows_read, result.rows_inserted, result.rows_updated = 3, 2, 1
        result.rows_unchanged, result.rows_rejected = 0, 0
        loader.finish_run(
            loader.connection,
            42,
            "success",
            result,
            api_requests=5,
            api_retries=1,
        )
        statement = loader.connection.executed[0]
        assert "UPDATE meta.ingestion_run" in statement
        assert "error_message" in statement

    def test_update_watermark_upserts(self, loader):
        loader.update_watermark(loader.connection, "clients", "37", 12)
        statement = loader.connection.executed[0]
        assert "meta.ingestion_watermark" in statement
        assert "ON CONFLICT (entity)" in statement

    def test_read_watermark_returns_a_map_when_a_row_exists(self, loader):
        point = datetime(2026, 8, 11, tzinfo=timezone.utc)
        loader.connection.results = [("clients", point, "37", 12, 400)]
        watermark = loader.read_watermark("clients")
        assert watermark == {
            "entity": "clients",
            "last_success_at": point,
            "last_cursor": "37",
            "last_row_count": 12,
            "total_rows_loaded": 400,
        }

    def test_read_watermark_returns_none_when_absent(self, loader):
        loader.connection.results = []
        assert loader.read_watermark("clients") is None


class TestRejectsAndExpectations:
    def test_record_rejects_is_a_noop_when_empty(self, loader):
        assert loader.record_rejects(loader.connection, uuid.uuid4(), []) == 0
        assert loader.connection.executed == []

    def test_record_rejects_inserts_one_row_per_reject(self, loader):
        rejects = [
            RejectedRecord("clients", "1", "primary_key_not_null", "missing key", {"id": 1}),
            RejectedRecord("clients", None, "mapper_exception", "bad payload", {"id": 2}),
        ]
        count = loader.record_rejects(loader.connection, uuid.uuid4(), rejects)
        assert count == 2
        assert all("meta.ingestion_reject" in s for s in loader.connection.executed)

    def test_record_expectations_inserts_each_result(self, loader):
        results = [
            ExpectationResult("client_id_not_null", "not_null", "error", passed=True),
            ExpectationResult(
                "client_id_unique",
                "unique",
                "error",
                passed=False,
                observed_value=2.0,
                threshold_value=0.0,
                details="dup",
            ),
        ]
        loader.record_expectations(loader.connection, uuid.uuid4(), "clients", results)
        assert len(loader.connection.executed) == 2
        assert all("meta.data_quality_result" in s for s in loader.connection.executed)


class TestSensorHelpers:
    def test_fetch_parent_ids_filters_none(self, loader):
        loader.connection.results = [(1,), (2,), (None,), (3,)]
        assert loader.fetch_parent_ids("SELECT loan_id FROM oltp.loans") == [1, 2, 3]

    def test_fetch_parent_ids_appends_limit(self, loader):
        loader.connection.results = [(1,), (2,)]
        assert loader.fetch_parent_ids("SELECT loan_id FROM oltp.loans", limit=2) == [1, 2]
        assert loader.connection.executed[0].endswith("LIMIT 2")

    def test_table_count(self, loader):
        loader.connection.results = [(77,)]
        assert loader.table_count("oltp.clients") == 77

    def test_replication_slot_status_normalises_rows(self, loader):
        loader.connection.results = [
            ("slot_a", True, "0/16B2E50", 123456),
            ("slot_b", False, None, None),
        ]
        slots = loader.replication_slot_status()
        assert slots[0] == {
            "slot_name": "slot_a",
            "active": True,
            "restart_lsn": "0/16B2E50",
            "lag_bytes": 123456,
        }
        assert slots[1]["lag_bytes"] == 0
        assert slots[1]["restart_lsn"] == "None"

    def test_touch_heartbeat_advances_the_slot(self, loader):
        loader.touch_heartbeat()
        assert "cdc.debezium_heartbeat" in loader.connection.executed[0]
        assert loader.connection.commits == 1


class TestLoadResult:
    def test_merge_totals_across_batches(self):
        first = LoadResult("clients")
        first.rows_read, first.rows_inserted = 10, 8
        second = LoadResult("clients")
        second.rows_read, second.rows_updated, second.rows_rejected = 20, 1, 1
        first.merge(second)
        assert first.as_dict() == {
            "rows_read": 30,
            "rows_inserted": 8,
            "rows_updated": 1,
            "rows_unchanged": 0,
            "rows_rejected": 1,
        }

    def test_fresh_result_is_all_zeros(self):
        result = LoadResult("clients")
        assert result.as_dict() == {
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_unchanged": 0,
            "rows_rejected": 0,
        }
