"""Tests for the orchestration layer: run_entity and run.

These exercise the full run lifecycle (fetch -> map -> validate -> load ->
finish) against stub client/loader/metrics so the control flow - success,
dry-run, reject-ratio abort, blocking-expectation abort, fail-fast - is
verified without any network or database.
"""

from __future__ import annotations

import uuid

import pytest

from fineract_ingest.client import FineractError
from fineract_ingest.config import RuntimeConfig, Settings
from fineract_ingest.entities import ENTITIES
from fineract_ingest.loader import LoadResult
from fineract_ingest.logging_setup import bind
from fineract_ingest.metrics import IngestionMetrics
from fineract_ingest.mock_server import FineractDataset
from fineract_ingest.pipeline import IngestionPipeline


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class StubLoader:
    """Stand-in for the Postgres loader; records the load lifecycle."""

    def __init__(self, table_rows=0):
        self.table_rows = table_rows
        self.start_calls = []
        self.finish_calls = []
        self.upsert_rows = []
        self.rejects = []
        self.watermarks = []
        self.expectations = []
        self.connection = FakeConnection()
        self.table_counts = []

    def connect(self):
        return self.connection

    def start_run(self, _connection, entity, batch_id, dag_run_id):
        self.start_calls.append((entity, batch_id, dag_run_id))
        return 7

    def finish_run(self, _connection, run_id, status, result, **kwargs):
        self.finish_calls.append((run_id, status))

    def upsert(self, _connection, table, _pk, rows):
        self.upsert_rows.extend(rows)
        result = LoadResult(table)
        result.rows_read = len(rows)
        result.rows_inserted = len(rows)
        return result

    def record_rejects(self, _connection, _batch_id, rejects):
        self.rejects.extend(rejects)
        return len(rejects)

    def record_expectations(self, _connection, _batch_id, _entity, results):
        self.expectations.extend(results)

    def update_watermark(self, _connection, entity, cursor_value, row_count):
        self.watermarks.append((entity, cursor_value, row_count))

    def table_count(self, _table):
        return self.table_rows

    def fetch_parent_ids(self, _query, _limit=None):
        return [1, 2]

    def close(self):
        self.closed = True


class StubClient:
    def __init__(self, records=None):
        self.records = records or []
        self.request_count = 0
        self.retry_count = 0
        self.total_latency_seconds = 0.0
        self.closed = False

    def authenticate(self):
        return None

    def iter_items(self, path, _params=None, paged=True):
        for record in self.records:
            yield record

    def close(self):
        self.closed = True


def make_pipeline(client_records=None, **loader_kwargs):
    settings = Settings(
        runtime=RuntimeConfig(
            log_level="WARNING",
            log_format="json",
            push_metrics=False,
            pushgateway_url="",
            max_reject_ratio=0.5,
        )
    )
    loader = StubLoader(**loader_kwargs)
    metrics = IngestionMetrics(pushgateway_url="", enabled=False)
    pipeline = IngestionPipeline(
        settings=settings,
        client=StubClient(client_records),
        loader=loader,
        metrics=metrics,
    )
    data = FineractDataset(clients=12, loans=15, seed=4)
    return pipeline, loader, data


class TestRunEntity:
    def test_success_path_writes_everything(self):
        dataset = FineractDataset(clients=12, loans=0, seed=2)
        pipeline, loader, _ = make_pipeline(dataset.clients)
        outcome = pipeline.run_entity("clients")
        assert outcome.ok is True
        assert outcome.result.rows_read == 12
        assert outcome.result.rows_inserted == 12
        assert len(loader.start_calls) == 1
        assert loader.finish_calls[-1][1] == "success"
        assert len(loader.watermarks) == 1
        assert loader.watermarks[0][0] == "clients"
        assert len(loader.expectations) == len(ENTITIES["clients"].expectations)

    def test_dry_run_skips_without_writing(self):
        dataset = FineractDataset(clients=10, loans=0, seed=1)
        pipeline, loader, _ = make_pipeline(dataset.clients)
        outcome = pipeline.run_entity("clients", dry_run=True)
        assert outcome.status == "skipped"
        assert outcome.ok is False
        assert loader.upsert_rows == []
        assert loader.finish_calls[-1][1] == "skipped"

    def test_reject_ratio_aborts_and_marks_failed(self):
        broken = [{"id": i, "displayName": None, "officeId": None} for i in (1, 2, 3, 4, 5, 6)]
        pipeline, loader, _ = make_pipeline(broken)
        settings = Settings(
            runtime=RuntimeConfig(
                log_level="WARNING",
                log_format="json",
                push_metrics=False,
                pushgateway_url="",
                max_reject_ratio=0.1,
            )
        )
        pipeline.settings = settings
        outcome = pipeline.run_entity("clients")
        assert outcome.status == "failed"
        assert "reject ratio" in (outcome.error or "")
        assert loader.finish_calls[-1][1] == "failed"

    def test_a_malformed_row_is_quarantined_not_fatal(self):
        dataset = FineractDataset(clients=8, loans=0, seed=5)
        records = list(dataset.clients)
        records[3] = {"id": None}
        pipeline, loader, _ = make_pipeline(records)
        outcome = pipeline.run_entity("clients")
        assert outcome.ok is True
        assert len(loader.rejects) == 1
        assert loader.rejects[0].rule == "primary_key_not_null"

    def test_parent_mode_fetches_child_collections(self):
        tx = {
            "id": 123,
            "amount": 42.0,
            "type": {"id": 2, "value": "REPAYMENT"},
            "submittedOnDate": [2026, 8, 11],
        }
        target_parent = 77

        class ParentLoader(StubLoader):
            def fetch_parent_ids(self, _query, _limit=None):
                return [target_parent]

        class ParentClient(StubClient):
            def iter_items(self, path, _params=None, paged=True):
                yield tx

        settings = Settings(
            runtime=RuntimeConfig(
                log_level="WARNING", log_format="json", push_metrics=False, pushgateway_url=""
            )
        )
        loader = ParentLoader()
        pipeline = IngestionPipeline(
            settings=settings, client=ParentClient(), loader=loader, metrics=IngestionMetrics("")
        )
        records = list(pipeline._fetch_records(ENTITIES["loan_transactions"]))
        assert len(records) == 1
        assert records[0]["_loan_id"] == target_parent
        assert records[0]["loanId"] == target_parent

    def test_child_collection_failure_is_not_fatal(self):
        class FailingLoader(StubLoader):
            def fetch_parent_ids(self, _query, _limit=None):
                return [1, 2]

        class FailingClient(StubClient):
            def iter_items(self, path, _params=None, paged=True):
                if "1" in path:
                    raise FineractError(f"GET {path} returned 404", 404)
                yield {"id": 99, "loanId": 2}

        settings = Settings(
            runtime=RuntimeConfig(
                log_level="WARNING", log_format="json", push_metrics=False, pushgateway_url=""
            )
        )
        loader = FailingLoader()
        pipeline = IngestionPipeline(
            settings=settings, client=FailingClient(), loader=loader, metrics=IngestionMetrics("")
        )
        records = list(pipeline._fetch_records(ENTITIES["loan_transactions"]))
        assert len(records) == 1
        assert records[0]["id"] == 99


class TestRun:
    def test_run_collects_outcomes_across_entities(self):
        dataset = FineractDataset(clients=6, loans=0, seed=6)
        pipeline, loader, _ = make_pipeline(dataset.clients)
        outcomes = pipeline.run(["clients"], dry_run=True)
        assert len(outcomes) == 1
        assert outcomes[0].entity == "clients"
        assert loader.finish_calls[-1][1] == "skipped"

    def test_fail_fast_stops_at_the_first_failure(self):
        records = [
            {"id": i, "displayName": None, "officeId": None} for i in (1, 2, 3, 4, 5, 6, 7, 8)
        ]
        pipeline, loader, _ = make_pipeline(records)
        settings = Settings(
            runtime=RuntimeConfig(
                log_level="WARNING",
                log_format="json",
                push_metrics=False,
                pushgateway_url="",
                max_reject_ratio=0.0,
            )
        )
        pipeline.settings = settings
        outcomes = pipeline.run(["clients", "loans"], fail_fast=True, dry_run=False)
        assert len(outcomes) == 1
        assert outcomes[0].status == "failed"

    def test_close_closes_client_and_loader(self):
        pipeline, loader, _ = make_pipeline([])
        pipeline.loader = loader
        pipeline.close()
        assert pipeline.client.closed is True
        assert loader.closed is True
