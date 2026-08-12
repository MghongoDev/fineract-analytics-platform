"""
=========================================
Manual backfill / full rebuild
=========================================

A separate, manually triggered DAG rather than a flag on the main
pipeline, for two reasons:

1. A backfill is destructive-ish (it drops and rebuilds relations) and
   long-running. Sharing a DAG with the scheduled pipeline means a
   mistyped parameter on a scheduled run can rebuild the warehouse.
2. It needs different concurrency and timeout settings. Forcing one set
   of settings to cover both makes both worse.

Two independent things can be rebuilt, selected by parameter:

* `reingest_source`  - re-read the Fineract API into Postgres. Safe and
                       idempotent (upsert on natural key), but it puts
                       real load on a live core-banking system, so it is
                       off by default.
* `rebuild_models`   - `dbt build --full-refresh`, which recreates every
                       incremental model from the raw layer.

For a CDC re-snapshot (raw layer diverged from Postgres), use the
Debezium signal table instead - see cdc/README.md. Rebuilding dbt models
cannot fix missing raw rows, and reaching for it when the raw layer is
the problem is a common and expensive mistake.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/plugins")

from fineract import DbtOperator, PostgresHook, task_failure_callback  # noqa: E402

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="fineract_backfill",
    description="Manual re-ingestion and full dbt rebuild",
    default_args=default_args,
    schedule=None,                    # manual trigger only, by design
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["fineract", "backfill", "manual"],
    doc_md=__doc__,
    params={
        "reingest_source": Param(
            False, type="boolean",
            description="Re-read the Fineract API into Postgres. Puts load on "
                        "the source system."),
        "entities": Param(
            "all", type="string",
            description="Comma-separated entity names, or 'all'."),
        "rebuild_models": Param(
            True, type="boolean",
            description="dbt build --full-refresh over the selected models."),
        "dbt_select": Param(
            "", type="string",
            description="dbt selector, e.g. 'tag:marts' or "
                        "'stg_fineract__loans+'. Empty means everything."),
        "confirm": Param(
            "", type="string",
            description="Type REBUILD to confirm. A guard against an "
                        "accidental trigger from the UI."),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="validate_parameters")
    def validate_parameters(**context) -> dict:
        params = context["params"]
        if params.get("confirm") != "REBUILD":
            raise ValueError(
                "Refusing to run: set the 'confirm' parameter to REBUILD. "
                "This DAG rebuilds warehouse relations and can re-read the "
                "whole source book.")
        return {
            "reingest": bool(params.get("reingest_source")),
            "entities": params.get("entities", "all"),
            "rebuild": bool(params.get("rebuild_models")),
            "select": params.get("dbt_select") or None,
        }

    @task(task_id="snapshot_row_counts_before")
    def snapshot_before() -> dict:
        """Record row counts before the rebuild so the after-check can
        prove the backfill did not lose anything."""
        postgres = PostgresHook()
        tables = ["clients", "loans", "loan_transactions", "savings_accounts",
                  "offices", "staff", "loan_products", "savings_products"]
        counts = {t: int(postgres.scalar(f"SELECT count(*) FROM oltp.{t}") or 0)
                  for t in tables}
        print(f"row counts before: {counts}")
        return counts

    @task(task_id="reingest_from_source")
    def reingest(config: dict, **context) -> dict:
        if not config["reingest"]:
            print("reingest_source=false - skipping source re-read")
            return {"skipped": True}

        from fineract_ingest.cli import main

        argv = ["ingest"]
        if config["entities"] == "all":
            argv.append("--all")
        else:
            argv += ["--entities", config["entities"]]
        exit_code = main(argv)
        if exit_code != 0:
            raise RuntimeError(f"re-ingestion failed with exit code {exit_code}")
        return {"skipped": False, "entities": config["entities"]}

    @task(task_id="build_dbt_arguments")
    def build_dbt_arguments(config: dict) -> str:
        """dbt's --select cannot be empty, so resolve it here rather than
        templating a conditional flag into the operator."""
        return config["select"] or "fqn:*"

    rebuild = DbtOperator(
        task_id="dbt_full_refresh",
        command="build",
        select="{{ ti.xcom_pull(task_ids='build_dbt_arguments') }}",
        full_refresh=True,
        fail_fast=True,
        execution_timeout=timedelta(hours=6),
    )

    @task(task_id="verify_row_counts_after", trigger_rule=TriggerRule.ALL_DONE)
    def verify_after(before: dict) -> dict:
        """A backfill that silently loses rows is worse than no backfill."""
        postgres = PostgresHook()
        after = {t: int(postgres.scalar(f"SELECT count(*) FROM oltp.{t}") or 0)
                 for t in before}
        shrunk = {t: (before[t], after[t]) for t in before if after[t] < before[t]}
        print(f"row counts after: {after}")
        if shrunk:
            raise RuntimeError(f"row counts decreased during backfill: {shrunk}")
        return {"before": before, "after": after,
                "delta": {t: after[t] - before[t] for t in before}}

    finish = EmptyOperator(task_id="finish")

    config = validate_parameters()
    before = snapshot_before()
    ingested = reingest(config)
    arguments = build_dbt_arguments(config)

    start >> config >> before >> ingested >> arguments >> rebuild >> verify_after(before) >> finish
