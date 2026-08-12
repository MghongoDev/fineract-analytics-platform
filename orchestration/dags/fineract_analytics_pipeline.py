"""
================================================================
Fineract analytics pipeline - ingestion -> CDC gate -> dbt -> QA
================================================================

    preflight ─► ingest ─► cdc_gate ─► transform ─► test ─► publish ─► gate

Design decisions worth stating explicitly:

**One DAG, not four.** Ingestion, CDC verification, transformation and
quality are one dependency chain with one SLA. Splitting them across DAGs
linked by ExternalTaskSensor is the usual approach and it is usually a
mistake: it hides the real dependency, makes a partial failure hard to
read, and turns "is today's data good?" into a question about four
separate run histories.

**Ingestion tasks are generated from the entity registry.** The list of
entities lives in `fineract_ingest.entities` and nowhere else. Adding a
source entity adds an Airflow task automatically, so the DAG cannot drift
from what the ingestion service actually loads.

**The CDC gate is a first-class task.** Ingestion writes to Postgres, dbt
reads from ClickHouse, and the CDC stream between them is asynchronous
and outside Airflow's control. Without an explicit gate, dbt would
happily transform a warehouse that has not received the rows yet, succeed,
and publish stale numbers - a silent failure, which is the worst kind.

**dbt runs layer by layer, not as one `dbt build`.** A failure in the
marts should not make it look as though staging is broken, and layered
execution gives per-layer timing in the run history for free.

**Catchup is off and max_active_runs is 1.** This pipeline transforms
current state, not a historical window - two concurrent runs would race
on the same ClickHouse relations. Historical rebuilds are the backfill
DAG's job.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/plugins")

from fineract import (  # noqa: E402
    CDCCaughtUpSensor,
    DataQualityGateOperator,
    DbtOperator,
    KafkaConnectorHealthSensor,
    PublishDbtResultsOperator,
    ServiceHealthSensor,
    dag_failure_callback,
    dag_success_callback,
    sla_miss_callback,
    task_failure_callback,
)

# ---------------------------------------------------------------------
# Entity registry - the single source of truth for what gets ingested.
# Imported from the ingestion package so the DAG cannot drift from it.
# The fallback list keeps the DAG parseable in an Airflow image that does
# not have the ingestion package installed (for example a lint-only CI
# container), which matters because an unparseable DAG breaks the whole
# scheduler loop, not just this pipeline.
# ---------------------------------------------------------------------
try:
    from fineract_ingest.entities import DEFAULT_ORDER as INGEST_ENTITIES
except ImportError:  # pragma: no cover
    INGEST_ENTITIES = (
        "offices", "staff", "loan_products", "savings_products",
        "clients", "loans", "savings_accounts", "loan_transactions",
    )

#: Entities whose CDC delivery gates the transformation layer. The
#: dimensions are excluded on purpose: they change rarely, so waiting on
#: them would add minutes of latency to every run for no benefit.
CDC_GATED_ENTITIES = ["clients", "loans", "loan_transactions", "savings_accounts"]

INGESTION_IMAGE_CMD = ["python", "-m", "fineract_ingest"]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": task_failure_callback,
}


def _run_ingestion(entity: str, **context) -> dict:
    """Invoke the ingestion package in-process.

    In-process rather than DockerOperator/KubernetesPodOperator because
    the Airflow image already contains the package: it keeps the local
    stack to one container per role and makes the task log the ingestion
    log. In production on Kubernetes this becomes a
    KubernetesPodOperator with the same arguments - the CLI contract is
    the seam that makes that swap a config change, not a rewrite.
    """
    from fineract_ingest.cli import main

    os.environ["AIRFLOW_CTX_DAG_RUN_ID"] = context["run_id"]
    argv = ["ingest", "--entities", entity]
    if context["params"].get("dry_run"):
        argv.append("--dry-run")
    if context["params"].get("parent_limit"):
        argv += ["--parent-limit", str(context["params"]["parent_limit"])]

    exit_code = main(argv)
    if exit_code != 0:
        raise RuntimeError(f"ingestion of '{entity}' failed with exit code {exit_code}")
    return {"entity": entity, "status": "success"}


with DAG(
    dag_id="fineract_analytics_pipeline",
    description="Fineract REST -> Postgres -> CDC -> ClickHouse -> dbt marts and ML features",
    default_args=default_args,
    # Every 4 hours during the working day. Microfinance branches post
    # transactions in daylight hours; a 15-minute cadence would mostly
    # crawl an unchanged API and hold a replication slot open for nothing.
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    sla_miss_callback=sla_miss_callback,
    on_failure_callback=dag_failure_callback,
    on_success_callback=dag_success_callback,
    tags=["fineract", "elt", "cdc", "clickhouse", "dbt", "production"],
    doc_md=__doc__,
    params={
        "dry_run": False,
        "parent_limit": None,
        "full_refresh": False,
        "skip_cdc_gate": False,
    },
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------------------------------------------
    # 1. Preflight - fail cheaply before touching any data.
    # -----------------------------------------------------------------
    with TaskGroup("preflight", tooltip="Platform reachability checks") as preflight:
        check_fineract = ServiceHealthSensor(
            task_id="check_fineract_api", service="fineract",
            poke_interval=30, timeout=600, mode="reschedule")
        check_postgres = ServiceHealthSensor(
            task_id="check_postgres", service="postgres",
            poke_interval=15, timeout=300, mode="reschedule")
        check_clickhouse = ServiceHealthSensor(
            task_id="check_clickhouse", service="clickhouse",
            poke_interval=15, timeout=300, mode="reschedule")
        check_connector = KafkaConnectorHealthSensor(
            task_id="check_debezium_connector",
            connector_name="fineract-oltp-source",
            restart_failed=True,
            poke_interval=20, timeout=600, mode="reschedule")

    # -----------------------------------------------------------------
    # 2. Ingest - one task per entity, generated from the registry.
    #    Sequential within the group: the parent/child entities have a
    #    real dependency (loan_transactions crawls loan ids that the
    #    loans task landed), and hammering a live core-banking API with
    #    eight parallel crawlers is exactly the behaviour the client-side
    #    rate limiter exists to prevent.
    # -----------------------------------------------------------------
    with TaskGroup("ingest", tooltip="Fineract REST API -> Postgres OLTP") as ingest:
        previous = None
        for entity in INGEST_ENTITIES:
            ingest_task = PythonOperator(
                task_id=f"ingest_{entity}",
                python_callable=_run_ingestion,
                op_kwargs={"entity": entity},
                sla=timedelta(minutes=30),
                doc_md=f"Load `{entity}` from the Fineract API into `oltp.{entity}`.",
            )
            if previous:
                previous >> ingest_task
            previous = ingest_task

    # -----------------------------------------------------------------
    # 3. CDC gate - do not transform what has not arrived.
    # -----------------------------------------------------------------
    cdc_gate = CDCCaughtUpSensor(
        task_id="wait_for_cdc_to_catch_up",
        entities=CDC_GATED_ENTITIES,
        row_tolerance=0,
        max_lag_seconds=300,
        poke_interval=20,
        timeout=1800,
        mode="reschedule",
        doc_md=(
            "Blocks until ClickHouse row counts match Postgres AND CDC "
            "freshness is under five minutes. Both conditions are needed: "
            "an idle stream trivially satisfies row parity, and a stream "
            "that drops some events while delivering others still looks "
            "fresh."
        ),
    )

    # -----------------------------------------------------------------
    # 4. Transform - layer by layer.
    # -----------------------------------------------------------------
    with TaskGroup("transform", tooltip="dbt on ClickHouse") as transform:
        dbt_deps = DbtOperator(task_id="dbt_deps", command="deps", retries=2)
        dbt_seed = DbtOperator(task_id="dbt_seed", command="seed")

        dbt_staging = DbtOperator(
            task_id="dbt_run_staging", command="run",
            select="tag:staging",
            full_refresh="{{ params.full_refresh }}",
            execution_timeout=timedelta(minutes=60))
        dbt_intermediate = DbtOperator(
            task_id="dbt_run_intermediate", command="run", select="tag:intermediate")
        dbt_marts = DbtOperator(
            task_id="dbt_run_marts", command="run", select="tag:marts")
        dbt_ml = DbtOperator(
            task_id="dbt_run_ml", command="run", select="tag:ml")

        dbt_deps >> dbt_seed >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_ml

    # -----------------------------------------------------------------
    # 5. Test - source freshness plus the model tests.
    # -----------------------------------------------------------------
    with TaskGroup("test", tooltip="dbt tests and source freshness") as test:
        # Freshness is a warning, not a gate: a quiet Sunday is not a
        # pipeline failure, and paging on it trains people to ignore
        # pages. The CDC gate above is the hard freshness guarantee.
        dbt_freshness = DbtOperator(
            task_id="dbt_source_freshness", command="source freshness",
            retries=0, trigger_rule=TriggerRule.ALL_DONE)
        dbt_test = DbtOperator(
            task_id="dbt_test", command="test",
            execution_timeout=timedelta(minutes=30))

        dbt_freshness >> dbt_test

    # -----------------------------------------------------------------
    # 6. Publish - artefacts and metrics, even when tests failed.
    # -----------------------------------------------------------------
    publish_results = PublishDbtResultsOperator(
        task_id="publish_dbt_results",
        trigger_rule=TriggerRule.ALL_DONE,   # a failed test is exactly when
        retries=1,                            # you most want the history
    )

    @task(task_id="publish_pipeline_metrics", trigger_rule=TriggerRule.ALL_DONE)
    def publish_pipeline_metrics(**context) -> dict:
        """Push run-level metrics so Grafana shows DAG health next to data health."""
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        dag_run = context["dag_run"]
        duration = ((dag_run.end_date or datetime.utcnow()) - dag_run.start_date
                    ).total_seconds() if dag_run.start_date else 0
        failed = [ti.task_id for ti in dag_run.get_task_instances()
                  if ti.state == "failed"]

        registry = CollectorRegistry()
        Gauge("fineract_pipeline_duration_seconds", "Pipeline wall clock",
              ["dag_id"], registry=registry).labels(dag_id=dag_run.dag_id).set(duration)
        Gauge("fineract_pipeline_failed_tasks", "Failed tasks in the last run",
              ["dag_id"], registry=registry).labels(dag_id=dag_run.dag_id).set(len(failed))
        Gauge("fineract_pipeline_last_run_timestamp_seconds", "Last run finish time",
              ["dag_id"], registry=registry).labels(
                  dag_id=dag_run.dag_id).set(datetime.utcnow().timestamp())
        try:
            push_to_gateway(
                os.environ.get("PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091"),
                job="fineract_pipeline", registry=registry)
        except Exception as exc:  # noqa: BLE001 - telemetry must not fail the run
            print(f"metric push failed: {exc}")
        return {"duration_seconds": duration, "failed_tasks": failed}

    # -----------------------------------------------------------------
    # 7. Quality gate - is this run fit to be consumed?
    # -----------------------------------------------------------------
    quality_gate = DataQualityGateOperator(
        task_id="data_quality_gate",
        retries=0,
        doc_md=(
            "Warehouse-level assertions run after the marts exist, so a "
            "failure leaves the output available for inspection instead of "
            "vanishing with a rolled-back transaction."
        ),
    )

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    (
        start
        >> preflight
        >> ingest
        >> cdc_gate
        >> transform
        >> test
        >> [publish_results, publish_pipeline_metrics()]
        >> quality_gate
        >> finish
    )
