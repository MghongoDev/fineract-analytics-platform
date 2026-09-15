"""Tests for the Prometheus instrumentation layer.

None of this needs a real Pushgateway: the registry is a local object and
``push_to_gateway`` is patched, so the assertions are about what metrics
were recorded, not about HTTP.
"""

from __future__ import annotations

import pytest

from fineract_ingest import metrics as metrics_module
from fineract_ingest.metrics import IngestionMetrics


@pytest.fixture
def m() -> IngestionMetrics:
    return IngestionMetrics(pushgateway_url="http://pushgateway:9091", environment="test")


class TestRecording:
    def test_rows_are_incremented_by_label(self, m):
        m.record_load(
            "clients",
            rows_read=10,
            rows_inserted=8,
            rows_updated=1,
            rows_unchanged=1,
            rows_rejected=0,
            duration_seconds=2.5,
            api_requests=4,
            api_retries=0,
            mean_latency_seconds=0.6,
            status="success",
            table_rows=2000,
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_rows_read_total", {"entity": "clients", "environment": "test"}
            )
            == 10.0
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_rows_inserted_total", {"entity": "clients", "environment": "test"}
            )
            == 8.0
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_rows_updated_total", {"entity": "clients", "environment": "test"}
            )
            == 1.0
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_rows_unchanged_total", {"entity": "clients", "environment": "test"}
            )
            == 1.0
        )

    def test_reject_counter_and_runs_total(self, m):
        m.record_load(
            "clients",
            rows_read=5,
            rows_inserted=4,
            rows_updated=0,
            rows_unchanged=0,
            rows_rejected=1,
            duration_seconds=1.0,
            api_requests=2,
            api_retries=0,
            mean_latency_seconds=0.0,
            status="success",
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_rows_rejected_total", {"entity": "clients", "environment": "test"}
            )
            == 1.0
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_runs_total",
                {"entity": "clients", "environment": "test", "status": "success"},
            )
            == 1.0
        )

    def test_success_sets_the_last_success_timestamp(self, m):
        m.record_load(
            "clients",
            rows_read=0,
            rows_inserted=0,
            rows_updated=0,
            rows_unchanged=0,
            rows_rejected=0,
            duration_seconds=0.0,
            api_requests=0,
            api_retries=0,
            mean_latency_seconds=0.0,
            status="success",
        )
        stamp = m.registry.get_sample_value(
            "fineract_ingest_last_success_timestamp_seconds",
            {"entity": "clients", "environment": "test"},
        )
        assert stamp is not None and stamp > 0

    def test_failed_run_records_no_last_success(self, m):
        m.record_load(
            "clients",
            rows_read=0,
            rows_inserted=0,
            rows_updated=0,
            rows_unchanged=0,
            rows_rejected=0,
            duration_seconds=0.0,
            api_requests=0,
            api_retries=0,
            mean_latency_seconds=0.0,
            status="failed",
        )
        assert (
            m.registry.get_sample_value("fineract_ingest_last_success_timestamp_seconds", {})
            is None
        )

    def test_skip_latency_when_zero(self, m):
        m.record_load(
            "clients",
            rows_read=0,
            rows_inserted=0,
            rows_updated=0,
            rows_unchanged=0,
            rows_rejected=0,
            duration_seconds=0.0,
            api_requests=0,
            api_retries=0,
            mean_latency_seconds=0.0,
            status="success",
        )
        assert m.registry.get_sample_value(
            "fineract_ingest_api_request_duration_seconds_count",
            {"entity": "clients", "environment": "test"},
        ) in (None, 0.0)

    def test_expectation_gauges_by_severity(self, m):
        m.record_expectations("clients", errors=1, warnings=2)
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_expectation_failures",
                {"entity": "clients", "environment": "test", "severity": "error"},
            )
            == 1.0
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_expectation_failures",
                {"entity": "clients", "environment": "test", "severity": "warn"},
            )
            == 2.0
        )

    def test_table_rows_of_a_success_with_count(self, m):
        m.record_load(
            "clients",
            rows_read=1,
            rows_inserted=1,
            rows_updated=0,
            rows_unchanged=0,
            rows_rejected=0,
            duration_seconds=0.0,
            api_requests=0,
            api_retries=0,
            mean_latency_seconds=0.0,
            status="success",
            table_rows=987,
        )
        assert (
            m.registry.get_sample_value(
                "fineract_ingest_table_rows", {"entity": "clients", "environment": "test"}
            )
            == 987.0
        )


class TestPush:
    def test_push_skips_when_disabled(self, m, monkeypatch):
        m.enabled = False
        sent = []

        def fake_push(url, job, registry):
            sent.append((url, job))

        monkeypatch.setattr(metrics_module, "push_to_gateway", fake_push)
        m.push()
        assert sent == []

    def test_push_sends_to_the_gateway(self, m, monkeypatch):
        sent = []

        def fake_push(url, job, registry):
            sent.append((url, job))

        monkeypatch.setattr(metrics_module, "push_to_gateway", fake_push)
        m.push()
        assert sent == [("http://pushgateway:9091", "fineract_ingestion")]

    def test_push_survives_a_gateway_failure(self, m, monkeypatch):
        def failing_push(url, job, registry):
            raise RuntimeError("gateway down")

        monkeypatch.setattr(metrics_module, "push_to_gateway", failing_push)
        m.push()
