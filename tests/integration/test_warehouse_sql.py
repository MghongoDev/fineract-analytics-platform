"""Warehouse SQL validation, driven through the standalone validators.

These wrap `scripts/validate_clickhouse_sql.py` and
`scripts/validate_dbt_sql.py` so the same checks that gate CI also run as
part of `pytest`. They need no containers: both scripts execute against
an embedded ClickHouse engine (chdb).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

chdb = pytest.importorskip("chdb", reason="chdb is required for warehouse SQL tests")


def _run(script: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True, text=True, timeout=600)


def test_clickhouse_ddl_executes_on_a_real_engine(repo_root: Path) -> None:
    """Every statement in platform/clickhouse/init must be valid
    ClickHouse, and the CDC conversions must round-trip."""
    result = _run(repo_root / "scripts" / "validate_clickhouse_sql.py")
    assert result.returncode == 0, (
        f"ClickHouse DDL validation failed:\n{result.stdout}\n{result.stderr}")
    assert "All ClickHouse conversion checks passed" in result.stdout


def test_dbt_dag_builds_and_tests_pass(repo_root: Path) -> None:
    """The whole dbt DAG is built on the embedded engine and every data
    test is executed against the result."""
    result = _run(repo_root / "scripts" / "validate_dbt_sql.py")
    assert result.returncode == 0, (
        f"dbt DAG validation failed:\n{result.stdout}\n{result.stderr}")
    assert "All models built and all data tests passed" in result.stdout


def test_every_model_is_reachable_from_a_source(repo_root: Path) -> None:
    """A model nobody refs and that refs nothing is dead code in a place
    where dead code silently costs compute on every run."""
    sys.path.insert(0, str(repo_root / "scripts"))
    from validate_dbt_sql import DbtJinja, discover_models, load_project_vars

    models = discover_models()
    jinja = DbtJinja(load_project_vars())
    jinja.models = models
    for model in models.values():
        jinja.render(model)

    # Generated dimensions have no upstream by definition: a calendar is
    # a function of its bounds, not of any source table.
    GENERATED = {"dim_date"}

    referenced = {ref for model in models.values() for ref in model.refs}
    for name, model in models.items():
        has_upstream = bool(model.refs or model.sources) or name in GENERATED
        has_downstream = name in referenced
        assert has_upstream, f"{name} reads from nothing"
        # Marts and ML tables are terminal by design; staging and
        # intermediate models must feed something.
        if model.layer in {"staging", "intermediate"}:
            assert has_downstream, f"{name} is not consumed by any model"


def test_no_model_selects_star_from_a_source(repo_root: Path) -> None:
    """`select *` straight out of the raw layer would leak CDC metadata
    columns into the warehouse and make schema drift invisible."""
    marts = (repo_root / "transform" / "fineract_analytics" / "models" / "marts")
    for path in marts.rglob("*.sql"):
        text = path.read_text().lower()
        assert "source(" not in text, (
            f"{path.name} reads a source directly; marts must go through staging")
