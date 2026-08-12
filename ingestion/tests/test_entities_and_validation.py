"""Tests for the entity registry, mappers and quality gates."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from fineract_ingest.entities import DEFAULT_ORDER, ENTITIES, get_entity
from fineract_ingest.mock_server import FineractDataset
from fineract_ingest.validation import (
    evaluate_expectations,
    summarise,
    validate_record,
)


@pytest.fixture(scope="module")
def dataset() -> FineractDataset:
    return FineractDataset(clients=40, loans=60, seed=11)


class TestRegistry:
    def test_every_entity_in_the_load_order_exists(self):
        for name in DEFAULT_ORDER:
            assert name in ENTITIES

    def test_every_registered_entity_is_in_the_load_order(self):
        # Otherwise an entity is defined but never loaded, and nobody
        # notices until someone asks why a table is empty.
        assert set(ENTITIES) == set(DEFAULT_ORDER)

    def test_parents_are_loaded_before_children(self):
        for name, spec in ENTITIES.items():
            if spec.mode == "parent":
                assert spec.parent_entity is not None
                assert DEFAULT_ORDER.index(spec.parent_entity) < DEFAULT_ORDER.index(name)

    def test_every_entity_declares_expectations(self):
        for name, spec in ENTITIES.items():
            assert spec.expectations, f"{name} has no data-quality expectations"

    def test_every_entity_asserts_a_non_null_unique_key(self):
        for name, spec in ENTITIES.items():
            kinds = {(e.kind, e.columns) for e in spec.expectations}
            assert ("not_null", (spec.primary_key,)) in kinds, name
            assert ("unique", (spec.primary_key,)) in kinds, name

    def test_unknown_entity_raises_a_helpful_error(self):
        with pytest.raises(KeyError) as error:
            get_entity("nope")
        assert "Known:" in str(error.value)


class TestMappers:
    def test_client_mapper_produces_typed_values(self, dataset):
        mapped = ENTITIES["clients"].mapper(dataset.clients[0])
        assert isinstance(mapped["client_id"], int)
        assert isinstance(mapped["activation_date"], date)
        assert mapped["display_name"]
        assert mapped["status_value"] in {"Active", "Closed"}

    def test_loan_mapper_flattens_the_summary_block(self, dataset):
        mapped = ENTITIES["loans"].mapper(dataset.loans[0])
        assert isinstance(mapped["principal"], Decimal)
        assert isinstance(mapped["total_outstanding"], Decimal)
        assert isinstance(mapped["disbursed_on_date"], (date, type(None)))
        assert mapped["status_value"]

    def test_loan_transaction_mapper_carries_the_parent_id(self, dataset):
        loan_id = next(iter(dataset.transactions))
        raw = dict(dataset.transactions[loan_id][0])
        mapped = ENTITIES["loan_transactions"].mapper(raw)
        assert mapped["loan_id"] == loan_id
        assert mapped["transaction_category" if False else "type_value"]

    def test_every_mapper_survives_an_empty_payload(self):
        """A sparse payload must map to NULLs, not raise. Fineract omits
        optional fields entirely rather than sending null."""
        for name, spec in ENTITIES.items():
            mapped = spec.mapper({})
            assert spec.primary_key in mapped, name
            assert mapped[spec.primary_key] is None, name

    def test_mapper_output_keys_are_stable(self, dataset):
        """Two records of the same entity must produce identical column
        sets - otherwise the bulk upsert builds a different statement per
        row and the batch silently degrades."""
        spec = ENTITIES["clients"]
        first = set(spec.mapper(dataset.clients[0]))
        second = set(spec.mapper(dataset.clients[1]))
        assert first == second


class TestRecordValidation:
    def test_a_record_without_a_primary_key_is_rejected(self):
        spec = ENTITIES["clients"]
        rejection = validate_record(spec, {"client_id": None, "display_name": "x"}, {})
        assert rejection is not None
        assert rejection.rule == "primary_key_not_null"

    def test_a_record_with_only_a_key_is_rejected(self):
        spec = ENTITIES["clients"]
        mapped = {key: None for key in spec.mapper({})}
        mapped["client_id"] = 1
        rejection = validate_record(spec, mapped, {})
        assert rejection is not None
        assert rejection.rule == "record_not_empty"

    def test_a_valid_record_is_accepted(self, dataset):
        spec = ENTITIES["clients"]
        assert validate_record(spec, spec.mapper(dataset.clients[0]),
                               dataset.clients[0]) is None


class TestExpectations:
    def test_clean_batch_passes_every_expectation(self, dataset):
        spec = ENTITIES["loans"]
        rows = [spec.mapper(loan) for loan in dataset.loans]
        results = evaluate_expectations(spec, rows)
        failures = [r for r in results if not r.passed]
        assert not failures, f"unexpected failures: {[f.name for f in failures]}"

    def test_duplicate_keys_are_detected(self, dataset):
        spec = ENTITIES["loans"]
        rows = [spec.mapper(dataset.loans[0])] * 3
        results = {r.name: r for r in evaluate_expectations(spec, rows)}
        assert results["loan_id_unique"].passed is False
        assert results["loan_id_unique"].observed_value == 2

    def test_negative_money_fails_a_blocking_expectation(self, dataset):
        spec = ENTITIES["loans"]
        rows = [spec.mapper(loan) for loan in dataset.loans]
        rows[0]["principal"] = Decimal("-1")
        results = {r.name: r for r in evaluate_expectations(spec, rows)}
        assert results["principal_non_negative"].passed is False
        assert results["principal_non_negative"].is_blocking_failure is True

    def test_out_of_range_interest_rate_only_warns(self, dataset):
        # A 500% rate is suspicious, not impossible - some products are
        # quoted per period. It should surface, not stop the pipeline.
        spec = ENTITIES["loans"]
        rows = [spec.mapper(loan) for loan in dataset.loans]
        rows[0]["annual_interest_rate"] = Decimal("500")
        results = {r.name: r for r in evaluate_expectations(spec, rows)}
        assert results["interest_rate_sane"].passed is False
        assert results["interest_rate_sane"].is_blocking_failure is False

    def test_empty_batch_fails_the_row_count_expectation(self):
        spec = ENTITIES["offices"]
        results = {r.name: r for r in evaluate_expectations(spec, [])}
        assert results["has_head_office"].passed is False

    def test_summarise_counts_by_severity(self, dataset):
        spec = ENTITIES["loans"]
        rows = [spec.mapper(loan) for loan in dataset.loans]
        rows[0]["principal"] = Decimal("-1")
        rows[0]["annual_interest_rate"] = Decimal("500")
        summary = summarise(evaluate_expectations(spec, rows))
        assert summary["errors"] >= 1
        assert summary["warnings"] >= 1
        assert "principal_non_negative" in summary["failed_checks"]
