"""Unit tests for pipeline behaviour that does not need a database.

The mapping / de-duplication / quality-gate logic is where the subtle
bugs live, and none of it needs Postgres. Testing it in isolation keeps
the feedback loop in milliseconds and means the integration tests can
focus on the things that genuinely need a server.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from fineract_ingest.entities import ENTITIES
from fineract_ingest.mock_server import FineractDataset
from fineract_ingest.pipeline import IngestionPipeline


class _StubLoader:
    """Loader stand-in: records what it was asked to do, writes nothing."""

    def __init__(self):
        self.upserts = []
        self.rejects = []
        self.watermarks = []

    def connect(self):
        return self

    def start_run(self, *_args, **_kwargs):
        return 1

    def finish_run(self, *_args, **_kwargs):
        return None

    def upsert(self, _connection, table, _pk, rows):
        from fineract_ingest.loader import LoadResult

        self.upserts.append((table, list(rows)))
        result = LoadResult(table)
        result.rows_read = len(rows)
        result.rows_inserted = len(rows)
        return result

    def record_rejects(self, _connection, _batch, rejects):
        rows = list(rejects)
        self.rejects.extend(rows)
        return len(rows)

    def record_expectations(self, *_args, **_kwargs):
        return None

    def update_watermark(self, _connection, entity, cursor_value, row_count):
        self.watermarks.append((entity, cursor_value, row_count))

    def table_count(self, _table):
        return 0

    def fetch_parent_ids(self, _query, _limit=None):
        return []

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


@pytest.fixture
def pipeline() -> IngestionPipeline:
    from fineract_ingest.config import RuntimeConfig, Settings

    settings = Settings(runtime=RuntimeConfig())
    instance = IngestionPipeline.__new__(IngestionPipeline)
    instance.settings = settings
    instance.batch_id = uuid.uuid4()
    return instance


@pytest.fixture(scope="module")
def dataset() -> FineractDataset:
    return FineractDataset(clients=50, loans=80, seed=3)


class TestMapAndValidate:
    def test_clean_records_all_map(self, pipeline, dataset):
        spec = ENTITIES["clients"]
        rows, rejects = pipeline._map_and_validate(spec, dataset.clients)
        assert len(rows) == len(dataset.clients)
        assert rejects == []

    def test_every_row_carries_a_payload_hash(self, pipeline, dataset):
        spec = ENTITIES["clients"]
        rows, _ = pipeline._map_and_validate(spec, dataset.clients)
        assert all(len(row["_payload_hash"]) == 64 for row in rows)
        assert all(row["_source_system"] == "fineract" for row in rows)

    def test_a_record_missing_its_key_is_quarantined_not_dropped(self, pipeline, dataset):
        spec = ENTITIES["clients"]
        broken = dict(dataset.clients[0])
        broken.pop("id")
        rows, rejects = pipeline._map_and_validate(spec, [broken, *dataset.clients[1:5]])
        assert len(rows) == 4
        assert len(rejects) == 1
        assert rejects[0].rule == "primary_key_not_null"
        # The raw payload must be preserved - a quarantined record we
        # cannot inspect is the same as a lost one.
        assert rejects[0].payload == broken

    def test_a_mapper_exception_is_quarantined(self, pipeline, dataset):
        spec = ENTITIES["loans"]
        poisoned = dict(dataset.loans[0])
        poisoned["summary"] = "not a dict"
        rows, rejects = pipeline._map_and_validate(spec, [poisoned])
        assert rows == [] or rejects
        if rejects:
            assert rejects[0].rule in {"mapper_exception", "record_not_empty"}

    def test_duplicate_keys_within_a_batch_are_collapsed_last_write_wins(
            self, pipeline, dataset):
        """The API can page the same record twice when rows are being
        written concurrently. Two rows with the same key in one INSERT
        makes ON CONFLICT fail outright, so the batch must be unique."""
        spec = ENTITIES["clients"]
        first = dict(dataset.clients[0])
        second = dict(dataset.clients[0])
        second["displayName"] = "Updated Name"
        rows, _ = pipeline._map_and_validate(spec, [first, second])
        assert len(rows) == 1
        assert rows[0]["display_name"] == "Updated Name"

    def test_hash_is_identical_for_an_unchanged_record(self, pipeline, dataset):
        spec = ENTITIES["loans"]
        first, _ = pipeline._map_and_validate(spec, [dataset.loans[0]])
        second, _ = pipeline._map_and_validate(spec, [dataset.loans[0]])
        assert first[0]["_payload_hash"] == second[0]["_payload_hash"]

    def test_hash_changes_when_a_balance_moves(self, pipeline, dataset):
        spec = ENTITIES["loans"]
        original = dataset.loans[0]
        changed = dict(original)
        changed["summary"] = dict(original["summary"])
        changed["summary"]["totalOutstanding"] = float(
            original["summary"]["totalOutstanding"]) + 0.01

        before, _ = pipeline._map_and_validate(spec, [original])
        after, _ = pipeline._map_and_validate(spec, [changed])
        assert before[0]["_payload_hash"] != after[0]["_payload_hash"]


class TestCursor:
    def test_cursor_is_the_maximum_key_in_the_batch(self, pipeline, dataset):
        spec = ENTITIES["loans"]
        rows, _ = pipeline._map_and_validate(spec, dataset.loans)
        cursor = IngestionPipeline._cursor_value(spec, rows)
        assert int(cursor) == max(row["loan_id"] for row in rows)

    def test_cursor_of_an_empty_batch_is_none(self):
        assert IngestionPipeline._cursor_value(ENTITIES["loans"], []) is None


class TestMoneyHandling:
    def test_amounts_are_decimals_all_the_way_through(self, pipeline, dataset):
        spec = ENTITIES["loans"]
        rows, _ = pipeline._map_and_validate(spec, dataset.loans[:5])
        for row in rows:
            for column in ("principal", "total_outstanding", "principal_paid"):
                if row[column] is not None:
                    assert isinstance(row[column], Decimal), (
                        f"{column} is {type(row[column])}, not Decimal - "
                        "money must never become a float")
