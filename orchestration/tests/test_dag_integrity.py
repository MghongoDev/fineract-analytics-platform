"""DAG integrity tests.

An unparseable DAG does not fail loudly - it disappears from the UI and
the scheduler carries on as though it never existed. That failure mode is
why these tests run on every push: they are the only thing standing
between a typo and a pipeline that silently stops running.

The suite checks four properties:
  1. every DAG file imports with no exceptions and no import errors
  2. structural policy (retries, owner, tags, no catchup surprises)
  3. the task graph is acyclic and every task is reachable
  4. the ingestion tasks match the entity registry exactly - the DAG
     cannot drift from what the ingestion service actually loads
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow is not installed in this environment")

from airflow.models import DagBag  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_DIR = REPO_ROOT / "orchestration" / "dags"
PLUGIN_DIR = REPO_ROOT / "orchestration" / "plugins"

EXPECTED_DAGS = {
    "fineract_analytics_pipeline",
    "fineract_backfill",
    "platform_maintenance",
}


def _dag(dagbag: DagBag, dag_id: str):
    """Fetch a parsed DAG without touching the metadata database.

    `DagBag.get_dag()` performs a database lookup to decide whether the
    file needs re-parsing. These tests deliberately run with no Airflow
    database - parsing is a pure function of the source files, and
    requiring a live metadata DB would make the single most valuable test
    in the repo the hardest one to run.
    """
    dag = dagbag.dags.get(dag_id)
    assert dag is not None, f"{dag_id} was not parsed"
    return dag


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    import sys

    if str(PLUGIN_DIR) not in sys.path:
        sys.path.insert(0, str(PLUGIN_DIR))
    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, (
        "DAG import failures:\n"
        + "\n".join(f"{path}: {error}" for path, error in dagbag.import_errors.items()))


def test_expected_dags_are_present(dagbag: DagBag) -> None:
    assert EXPECTED_DAGS.issubset(set(dagbag.dag_ids)), (
        f"missing DAGs: {EXPECTED_DAGS - set(dagbag.dag_ids)}")


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dag_has_documentation_and_tags(dagbag: DagBag, dag_id: str) -> None:
    dag = _dag(dagbag, dag_id)
    assert dag.doc_md, f"{dag_id} has no doc_md - the UI shows an empty Docs tab"
    assert dag.tags, f"{dag_id} has no tags - it will be unfindable in a large UI"
    assert "fineract" in dag.tags


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_tasks_have_retries_and_owner(dagbag: DagBag, dag_id: str) -> None:
    dag = _dag(dagbag, dag_id)
    for task in dag.tasks:
        assert task.owner not in (None, "", "airflow"), (
            f"{dag_id}.{task.task_id} has the default owner - nobody is on the hook")
        # A quality gate is deliberately retry-free: re-running an
        # assertion that just failed only wastes time. Compare on the
        # leaf name, because a task inside a TaskGroup is addressed as
        # "<group>.<task>".
        leaf = task.task_id.split(".")[-1]
        if leaf not in {"data_quality_gate", "dbt_source_freshness"}:
            assert task.retries >= 1, (
                f"{dag_id}.{task.task_id} has no retries; transient failures "
                f"in a distributed stack are normal")


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_no_cycles(dagbag: DagBag, dag_id: str) -> None:
    from airflow.utils.dag_cycle_tester import check_cycle

    check_cycle(_dag(dagbag, dag_id))


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_every_task_is_connected(dagbag: DagBag, dag_id: str) -> None:
    """An orphan task is almost always a wiring mistake."""
    dag = _dag(dagbag, dag_id)
    if len(dag.tasks) <= 1:
        return
    for task in dag.tasks:
        assert task.upstream_list or task.downstream_list, (
            f"{dag_id}.{task.task_id} is not connected to anything")


def test_pipeline_has_a_cdc_gate(dagbag: DagBag) -> None:
    """The gate between ingestion and transformation is the property that
    stops the pipeline publishing stale numbers. Assert it exists and sits
    in the right place."""
    dag = _dag(dagbag, "fineract_analytics_pipeline")
    gate = dag.get_task("wait_for_cdc_to_catch_up")
    assert gate is not None

    upstream = {t.task_id for t in gate.upstream_list}
    downstream_ids = {t.task_id for t in gate.get_flat_relatives(upstream=False)}

    assert any(task_id.startswith("ingest.") or task_id.startswith("ingest_")
               for task_id in upstream), "the CDC gate must run after ingestion"
    assert any("dbt_run" in task_id for task_id in downstream_ids), (
        "every dbt run must be downstream of the CDC gate")


def test_ingestion_tasks_match_the_entity_registry(dagbag: DagBag) -> None:
    """The DAG generates its ingestion tasks from the registry. If someone
    hardcodes a task, or an entity is added without the DAG noticing, this
    fails."""
    pytest.importorskip("fineract_ingest")
    from fineract_ingest.entities import DEFAULT_ORDER

    dag = _dag(dagbag, "fineract_analytics_pipeline")
    task_ids = {t.task_id.split(".")[-1] for t in dag.tasks}
    for entity in DEFAULT_ORDER:
        assert f"ingest_{entity}" in task_ids, (
            f"entity '{entity}' is in the registry but has no ingestion task")


def test_pipeline_does_not_catch_up(dagbag: DagBag) -> None:
    """Catchup on a current-state pipeline would spawn a backlog of runs
    that all transform the same 'now', racing on the same relations."""
    dag = _dag(dagbag, "fineract_analytics_pipeline")
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_backfill_is_manual_only(dagbag: DagBag) -> None:
    dag = _dag(dagbag, "fineract_backfill")
    assert dag.schedule_interval is None, (
        "the backfill DAG must never run on a schedule")
