"""Tests for the command line entry point.

The DB-touching commands are exercised against the same fake connection
strategy as test_loader.py, so the arguments, output shapes and exit
codes are verified without a Postgres.
"""

from __future__ import annotations

import json

import pytest

from fineract_ingest.cli import (
    build_parser,
    cmd_health,
    cmd_heartbeat,
    cmd_list_entities,
    cmd_status,
)
from fineract_ingest.config import RuntimeConfig, Settings
from fineract_ingest.loader import LoadResult
from fineract_ingest.pipeline import EntityOutcome


def settings() -> Settings:
    return Settings(runtime=RuntimeConfig(log_level="WARNING", log_format="json"))


class FakeLoader:
    def __init__(self, watermark=None, table_rows=0, slots=None, fail=False):
        self.watermark = watermark or {}
        self.table_rows = table_rows
        self.slots = slots or []
        self.fail = fail
        self.closed = False
        self.heartbeat_touched = False

    def connect(self):
        if self.fail:
            raise RuntimeError("connection refused")
        return object()

    def commit(self):
        return None

    def read_watermark(self, _entity):
        if self.fail:
            raise RuntimeError("connection refused")
        return self.watermark

    def table_count(self, _table):
        if self.fail:
            raise RuntimeError("connection refused")
        return self.table_rows

    def replication_slot_status(self):
        if self.fail:
            raise RuntimeError("connection refused")
        return self.slots

    def touch_heartbeat(self):
        if self.fail:
            raise RuntimeError("connection refused")
        self.heartbeat_touched = True

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, healthy=True):
        self.healthy = healthy

    def health_check(self):
        return self.healthy

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class TestParser:
    def test_ingest_command_parses(self):
        args = build_parser().parse_args(
            ["ingest", "--entities", "clients,loans", "--parent-limit", "5", "--dry-run"]
        )
        assert args.command == "ingest"
        assert args.entities == "clients,loans"
        assert args.parent_limit == 5
        assert args.dry_run is True

    def test_ingest_all_is_mutually_exclusive_with_entities(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["ingest", "--all", "--entities", "clients"])

    def test_health_subcommand_exists(self):
        assert build_parser().parse_args(["health"]).command == "health"

    def test_status_subcommand_exists(self):
        assert build_parser().parse_args(["status"]).command == "status"

    def test_list_entities_subcommand_exists(self):
        assert build_parser().parse_args(["list-entities"]).command == "list-entities"

    def test_heartbeat_subcommand_exists(self):
        assert build_parser().parse_args(["heartbeat"]).command == "heartbeat"


class TestCommands:
    def test_list_entities_prints_the_registry(self, capsys):
        assert cmd_list_entities() == 0
        output = json.loads(capsys.readouterr().out)
        names = {row["entity"] for row in output}
        assert "clients" in names
        assert "loan_transactions" in names
        assert all("expectations" in row for row in output)

    def test_health_reports_all_green(self, capsys, monkeypatch):
        monkeypatch.setattr("fineract_ingest.cli.FineractClient", lambda _config: FakeClient(True))
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader",
            lambda _config: FakeLoader(slots=[{"slot_name": "deb", "active": True}]),
        )
        assert cmd_health(settings()) == 0
        output = capsys.readouterr().out
        assert '"healthy": true' in output or '"healthy": True' in output

    def test_health_reports_failed_api(self, capsys, monkeypatch):
        monkeypatch.setattr("fineract_ingest.cli.FineractClient", lambda _config: FakeClient(False))
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader",
            lambda _config: FakeLoader(slots=[{"slot_name": "deb", "active": True}]),
        )
        assert cmd_health(settings()) == 2

    def test_health_handles_a_connection_error(self, capsys, monkeypatch):
        monkeypatch.setattr("fineract_ingest.cli.FineractClient", lambda _config: FakeClient(True))
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader", lambda _config: FakeLoader(fail=True)
        )
        assert cmd_health(settings()) == 2
        assert '"component": "postgres"' in capsys.readouterr().out

    def test_status_prints_every_entity(self, capsys, monkeypatch):
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader",
            lambda _config: FakeLoader(watermark={"last_cursor": "9"}, table_rows=11),
        )
        assert cmd_status(settings()) == 0
        rows = json.loads(capsys.readouterr().out)
        assert len(rows) == 8
        assert rows[0]["entity"] == "offices"
        assert rows[0]["last_cursor"] == "9"

    def test_status_failure_returns_two(self, capsys, monkeypatch):
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader", lambda _config: FakeLoader(fail=True)
        )
        assert cmd_status(settings()) == 2
        assert "error" in capsys.readouterr().out

    def test_heartbeat_returns_zero_on_success(self, capsys, monkeypatch):
        fake = FakeLoader()
        monkeypatch.setattr("fineract_ingest.cli.PostgresLoader", lambda _config: fake)
        assert cmd_heartbeat(settings()) == 0
        assert fake.heartbeat_touched is True
        assert '"heartbeat": "ok"' in capsys.readouterr().out

    def test_heartbeat_failure_returns_two(self, capsys, monkeypatch):
        monkeypatch.setattr(
            "fineract_ingest.cli.PostgresLoader", lambda _config: FakeLoader(fail=True)
        )
        assert cmd_heartbeat(settings()) == 2
        assert "failed" in capsys.readouterr().out


class TestPyprojectCoverageHelpers:
    def test_load_result_repr_is_not_pure(self):
        result = LoadResult("clients")
        assert "LoadResult" in repr(result)


class TestMain:
    def test_main_runs_an_ingest_pass(self, monkeypatch, capsys):
        calls = {}

        class FakePipeline:
            def __init__(self, _settings):
                self.closed = False

            def run(self, **kwargs):
                calls.update(kwargs)
                result = LoadResult("clients")
                result.rows_read = 5
                result.rows_inserted = 5
                return [
                    EntityOutcome(
                        entity="clients",
                        status="success",
                        result=result,
                        duration_seconds=0.25,
                    )
                ]

            def close(self):
                self.closed = True

        monkeypatch.setattr("fineract_ingest.cli.IngestionPipeline", FakePipeline)
        monkeypatch.setattr("fineract_ingest.cli.Settings.load", lambda: settings())

        # Configure() would clobber root loggers; capture it as a no-op.
        monkeypatch.setattr("fineract_ingest.cli.configure", lambda *_a, **_k: None)

        from fineract_ingest.cli import main

        assert main(["ingest", "--entities", "clients", "--dry-run", "--fail-fast"]) == 0
        assert calls["entities"] == ["clients"]
        assert calls["dry_run"] is True
        assert calls["fail_fast"] is True
        output = json.loads(capsys.readouterr().out)
        assert output[0]["entity"] == "clients"
        assert output[0]["status"] == "success"

    def test_main_returns_one_when_an_entity_fails(self, monkeypatch, capsys):
        class FailingPipeline:
            def __init__(self, _settings):
                pass

            def run(self, **kwargs):
                result = LoadResult("clients")
                return [
                    EntityOutcome("clients", "failed", result, 0.1, error="reject ratio exceeded")
                ]

            def close(self):
                return None

        monkeypatch.setattr("fineract_ingest.cli.IngestionPipeline", FailingPipeline)
        monkeypatch.setattr("fineract_ingest.cli.Settings.load", lambda: settings())
        monkeypatch.setattr("fineract_ingest.cli.configure", lambda *_a, **_k: None)

        from fineract_ingest.cli import main

        assert main(["ingest", "--all"]) == 1
