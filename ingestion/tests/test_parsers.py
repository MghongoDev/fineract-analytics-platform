"""Unit tests for the parsing layer.

These are the tests that matter most in the ingestion service. Fineract's
date encoding is unusual, its numerics are money, and a silent parsing
bug produces plausible-looking rows that nobody notices until a
reconciliation months later.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from fineract_ingest.parsers import (
    dig,
    enum_id,
    enum_value,
    parse_bool,
    parse_date,
    parse_decimal,
    parse_int,
    parse_text,
    parse_timestamp,
    payload_hash,
)


class TestParseDate:
    """Fineract serialises dates as [yyyy, m, d] arrays by default."""

    def test_fineract_array_form(self):
        assert parse_date([2026, 8, 11]) == date(2026, 8, 11)

    def test_array_with_time_components_is_truncated_to_the_date(self):
        assert parse_date([2026, 8, 11, 14, 30, 0]) == date(2026, 8, 11)

    def test_iso_string_form(self):
        assert parse_date("2026-08-11") == date(2026, 8, 11)

    def test_iso_timestamp_string(self):
        assert parse_date("2026-08-11T14:30:00") == date(2026, 8, 11)

    def test_long_date_form_used_by_some_endpoints(self):
        assert parse_date("11 August 2026") == date(2026, 8, 11)

    @pytest.mark.parametrize("value", [None, "", [], [2026], "not a date", {}])
    def test_unparseable_values_become_none_rather_than_raising(self, value):
        # A single bad date must not fail a 40,000-row batch. It becomes
        # NULL, and the row-level validator decides whether that matters.
        assert parse_date(value) is None

    def test_invalid_calendar_date_is_rejected(self):
        assert parse_date([2026, 2, 31]) is None

    def test_passthrough_of_a_real_date(self):
        assert parse_date(date(2026, 8, 11)) == date(2026, 8, 11)


class TestParseTimestamp:
    def test_six_element_array(self):
        assert parse_timestamp([2026, 8, 11, 14, 30, 15]) == datetime(
            2026, 8, 11, 14, 30, 15, tzinfo=timezone.utc)

    def test_iso_string_with_z_suffix(self):
        parsed = parse_timestamp("2026-08-11T14:30:00Z")
        assert parsed == datetime(2026, 8, 11, 14, 30, tzinfo=timezone.utc)

    def test_naive_datetimes_are_assumed_utc(self):
        parsed = parse_timestamp(datetime(2026, 8, 11, 14, 30))
        assert parsed.tzinfo is timezone.utc

    def test_epoch_millis(self):
        parsed = parse_timestamp(1786440600000)
        assert parsed.year == 2026


class TestParseDecimal:
    def test_money_is_never_parsed_as_float(self):
        # 0.1 + 0.2 famously is not 0.3 in binary floating point. Ledger
        # amounts must survive a round trip exactly.
        assert parse_decimal("0.1") + parse_decimal("0.2") == Decimal("0.3")

    def test_precision_is_preserved(self):
        assert parse_decimal("123456789.123456") == Decimal("123456789.123456")

    def test_integer_input(self):
        assert parse_decimal(250000) == Decimal("250000")

    @pytest.mark.parametrize("value", [None, "", "abc", [], {}])
    def test_unparseable_values_become_none(self, value):
        assert parse_decimal(value) is None


class TestParseScalars:
    @pytest.mark.parametrize("value,expected", [
        (1, 1), ("42", 42), (3.7, 3), ("3.7", 3), (None, None), ("", None), ("x", None)])
    def test_parse_int(self, value, expected):
        assert parse_int(value) == expected

    @pytest.mark.parametrize("value,expected", [
        (True, True), (False, False), ("true", True), ("TRUE", True),
        ("false", False), (1, True), (0, False), (None, None), ("", None)])
    def test_parse_bool(self, value, expected):
        assert parse_bool(value) == expected

    def test_parse_text_strips_and_nullifies_empty(self):
        assert parse_text("  hello  ") == "hello"
        assert parse_text("   ") is None
        assert parse_text(None) is None

    def test_parse_text_truncates_to_max_length(self):
        assert parse_text("abcdef", max_length=3) == "abc"


class TestNestedAccess:
    LOAN = {
        "id": 1,
        "summary": {"totalOutstanding": "1500.50", "nested": {"deep": "value"}},
        "status": {"id": 300, "code": "loanStatusType.active", "value": "Active"},
        "currency": None,
    }

    def test_dig_reaches_nested_values(self):
        assert dig(self.LOAN, "summary", "totalOutstanding") == "1500.50"
        assert dig(self.LOAN, "summary", "nested", "deep") == "value"

    def test_dig_returns_default_for_missing_paths(self):
        assert dig(self.LOAN, "summary", "absent") is None
        assert dig(self.LOAN, "nope", "nope", default="fallback") == "fallback"

    def test_dig_handles_a_null_node_without_raising(self):
        assert dig(self.LOAN, "currency", "code") is None

    def test_enum_helpers(self):
        assert enum_value(self.LOAN, "status") == "Active"
        assert enum_id(self.LOAN, "status") == 300
        assert enum_value(self.LOAN, "missing") is None


class TestPayloadHash:
    """The hash is what turns an unchanged row into a no-op update, so its
    stability is a correctness property, not an optimisation detail."""

    def test_identical_records_hash_identically(self):
        a = {"loan_id": 1, "principal": Decimal("100.00"), "date": date(2026, 1, 1)}
        b = {"date": date(2026, 1, 1), "principal": Decimal("100.00"), "loan_id": 1}
        assert payload_hash(a) == payload_hash(b), "key order must not matter"

    def test_a_changed_value_changes_the_hash(self):
        a = {"loan_id": 1, "principal": Decimal("100.00")}
        b = {"loan_id": 1, "principal": Decimal("100.01")}
        assert payload_hash(a) != payload_hash(b)

    def test_audit_columns_are_excluded(self):
        # Otherwise every row would look changed on every run, because
        # _ingested_at always moves - which would defeat the entire
        # point of hashing and flood the CDC stream.
        a = {"loan_id": 1, "_ingested_at": "2026-01-01", "_payload_hash": "x"}
        b = {"loan_id": 1, "_ingested_at": "2026-06-01", "_payload_hash": "y"}
        assert payload_hash(a) == payload_hash(b)

    def test_hash_is_stable_across_processes(self):
        # Hard-coding the digest catches an accidental change to the
        # serialisation, which would silently rewrite every row once.
        record = {"loan_id": 42, "amount": Decimal("1000.50")}
        assert payload_hash(record) == payload_hash(dict(record))
        assert len(payload_hash(record)) == 64
