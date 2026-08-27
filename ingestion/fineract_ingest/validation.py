"""Ingestion-side data quality.

Two mechanisms, deliberately separated:

``validate_record``
    Row-level gate. A record that fails is *quarantined*, not dropped:
    it goes to ``meta.ingestion_reject`` with the raw payload and the
    reason. One malformed loan must never fail a 40,000-row batch, and it
    must never vanish silently either.

``evaluate_expectations``
    Batch-level assertions declared next to each entity in
    :mod:`entities`. Results are written to ``meta.data_quality_result``
    and exported to Prometheus. ``severity='error'`` failures abort the
    load *before* it is committed, so a bad batch never reaches the CDC
    stream - failing closed is the right default for financial data.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .entities import EntitySpec, Expectation
from .logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class RejectedRecord:
    entity: str
    source_key: str | None
    rule: str
    error_message: str
    payload: Mapping[str, Any]


@dataclass
class ExpectationResult:
    name: str
    kind: str
    severity: str
    passed: bool
    observed_value: float | None = None
    threshold_value: float | None = None
    details: str = ""

    @property
    def is_blocking_failure(self) -> bool:
        return (not self.passed) and self.severity == "error"


# ---------------------------------------------------------------------
# Row level
# ---------------------------------------------------------------------
def validate_record(spec: EntitySpec, mapped: Mapping[str, Any],
                    raw: Mapping[str, Any]) -> RejectedRecord | None:
    """Return a rejection if the mapped record cannot be safely landed."""
    pk_value = mapped.get(spec.primary_key)
    if pk_value is None:
        return RejectedRecord(
            entity=spec.name,
            source_key=None,
            rule="primary_key_not_null",
            error_message=f"missing primary key '{spec.primary_key}'",
            payload=raw,
        )
    if isinstance(pk_value, int) and pk_value < 0:
        return RejectedRecord(
            entity=spec.name, source_key=str(pk_value),
            rule="primary_key_positive",
            error_message=f"primary key '{spec.primary_key}' is negative",
            payload=raw)

    # A record whose every business column is NULL is a parsing failure
    # wearing a valid-looking primary key.
    business_values = [v for k, v in mapped.items()
                       if k != spec.primary_key and not k.startswith("_")]
    if business_values and all(v is None for v in business_values):
        return RejectedRecord(
            entity=spec.name, source_key=str(pk_value),
            rule="record_not_empty",
            error_message="all non-key columns parsed to NULL",
            payload=raw)
    return None


# ---------------------------------------------------------------------
# Batch level
# ---------------------------------------------------------------------
def _numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return None


def _check_not_null(rows: Sequence[Mapping[str, Any]], columns: Iterable[str]) -> tuple[int, str]:
    failures = 0
    detail: list[str] = []
    for column in columns:
        nulls = sum(1 for row in rows if row.get(column) is None)
        if nulls:
            failures += nulls
            detail.append(f"{column}={nulls}")
    return failures, "null counts: " + ", ".join(detail) if detail else ""


def _check_unique(rows: Sequence[Mapping[str, Any]], columns: Iterable[str]) -> tuple[int, str]:
    duplicates = 0
    detail: list[str] = []
    for column in columns:
        values = [row.get(column) for row in rows if row.get(column) is not None]
        dupes = len(values) - len(set(values))
        if dupes:
            duplicates += dupes
            detail.append(f"{column}={dupes}")
    return duplicates, "duplicate counts: " + ", ".join(detail) if detail else ""


def _check_non_negative(
        rows: Sequence[Mapping[str, Any]], columns: Iterable[str]) -> tuple[int, str]:
    failures = 0
    detail: list[str] = []
    for column in columns:
        bad = sum(1 for row in rows
                  if (n := _numeric(row.get(column))) is not None and n < 0)
        if bad:
            failures += bad
            detail.append(f"{column}={bad}")
    return failures, "negative values: " + ", ".join(detail) if detail else ""


def _check_range(rows: Sequence[Mapping[str, Any]], columns: Iterable[str],
                 low: float | None, high: float | None) -> tuple[int, str]:
    failures = 0
    detail: list[str] = []
    for column in columns:
        bad = 0
        for row in rows:
            value = _numeric(row.get(column))
            if value is None:
                continue
            if (low is not None and value < low) or (high is not None and value > high):
                bad += 1
        if bad:
            failures += bad
            detail.append(f"{column}={bad}")
    return failures, f"outside [{low}, {high}]: " + ", ".join(detail) if detail else ""


def evaluate_expectations(spec: EntitySpec,
                          rows: Sequence[Mapping[str, Any]]) -> list[ExpectationResult]:
    """Run every declared expectation for an entity against a batch."""
    results: list[ExpectationResult] = []
    for expectation in spec.expectations:
        results.append(_evaluate_one(expectation, rows))
    return results


def _evaluate_one(expectation: Expectation,
                  rows: Sequence[Mapping[str, Any]]) -> ExpectationResult:
    kind = expectation.kind
    if kind == "row_count_min":
        threshold = expectation.min_value or 1
        observed = float(len(rows))
        return ExpectationResult(
            expectation.name, kind, expectation.severity,
            passed=observed >= threshold, observed_value=observed,
            threshold_value=threshold,
            details=f"{int(observed)} rows (min {int(threshold)})")

    if kind == "not_null":
        failures, detail = _check_not_null(rows, expectation.columns)
    elif kind == "unique":
        failures, detail = _check_unique(rows, expectation.columns)
    elif kind == "non_negative":
        failures, detail = _check_non_negative(rows, expectation.columns)
    elif kind == "range":
        failures, detail = _check_range(rows, expectation.columns,
                                        expectation.min_value, expectation.max_value)
    else:
        return ExpectationResult(expectation.name, kind, "warn", passed=True,
                                 details=f"unknown expectation kind '{kind}' - skipped")

    return ExpectationResult(
        expectation.name, kind, expectation.severity,
        passed=failures == 0, observed_value=float(failures), threshold_value=0.0,
        details=detail or "ok")


def summarise(results: Sequence[ExpectationResult]) -> dict[str, Any]:
    return {
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "warnings": sum(1 for r in results if not r.passed and r.severity == "warn"),
        "errors": sum(1 for r in results if r.is_blocking_failure),
        "failed_checks": [r.name for r in results if not r.passed],
    }
