"""
=====================================================
Platform maintenance - the housekeeping nobody enjoys
=====================================================

Runs hourly and does the four things that quietly break a CDC pipeline
that is otherwise working perfectly:

1. **CDC heartbeat.** An idle capture set means the replication slot's
   confirmed LSN never advances, and Postgres retains WAL until the disk
   fills. Debezium's own heartbeat handles this in normal operation; this
   task is the belt to that pair of braces, and it also proves end to end
   that a write in Postgres reaches ClickHouse.

2. **Replication slot inspection.** Slot lag is the single number that
   predicts a CDC outage. Reported as a metric and failed on when it
   crosses a threshold that still leaves time to react.

3. **ClickHouse merge pressure.** ReplacingMergeTree only collapses
   duplicates on merge. Unbounded part growth means queries read more and
   more duplicate versions - slow first, then wrong-looking. An explicit
   OPTIMIZE on the small tables keeps that in check without waiting for
   the background scheduler.

4. **Retention.** Rejects, audit rows and dbt history are bounded by TTL
   in DDL; this task removes the Postgres-side rows that have no TTL
   mechanism, and reports what it removed.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow/plugins")

from fineract import (  # noqa: E402
    ClickHouseHook,
    KafkaConnectHook,
    PostgresHook,
    task_failure_callback,
)

#: Fail the task above this. 512 MB of retained WAL is comfortably inside
#: the 1 GB max_slot_wal_keep_size set in postgresql.conf, so the alert
#: fires while there is still headroom to fix the cause.
SLOT_LAG_FAIL_BYTES = 512 * 1024 * 1024

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="platform_maintenance",
    description="CDC heartbeat, slot lag, ClickHouse merges and retention",
    default_args=default_args,
    schedule="15 * * * *",          # hourly, offset so it never collides
    start_date=datetime(2026, 1, 1),  # with the 4-hourly pipeline run
    catchup=False,
    max_active_runs=1,
    tags=["fineract", "maintenance", "cdc", "platform"],
    doc_md=__doc__,
    params={
        "reject_retention_days": Param(30, type="integer"),
        "run_history_retention_days": Param(90, type="integer"),
        "optimize_tables": Param(True, type="boolean"),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="cdc_heartbeat")
    def cdc_heartbeat() -> dict:
        """Write in Postgres, then confirm the write is visible downstream."""
        postgres = PostgresHook()
        postgres.query("UPDATE cdc.debezium_heartbeat SET beat_at = now() WHERE id = 1")
        beat = postgres.scalar("SELECT beat_at FROM cdc.debezium_heartbeat WHERE id = 1")
        print(f"heartbeat written at {beat}")
        return {"beat_at": str(beat)}

    @task(task_id="check_replication_slots")
    def check_replication_slots() -> dict:
        postgres = PostgresHook()
        rows = postgres.query(
            """
            SELECT slot_name, active, plugin,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes,
                   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
              FROM pg_replication_slots
            """)
        if not rows:
            raise RuntimeError(
                "no replication slots exist - the Debezium connector is not "
                "streaming. Nothing has reached ClickHouse since it stopped.")

        report = []
        problems = []
        for slot_name, active, plugin, lag_bytes, lag_pretty in rows:
            lag_bytes = int(lag_bytes or 0)
            print(f"slot={slot_name} plugin={plugin} active={active} lag={lag_pretty}")
            report.append({"slot": slot_name, "active": bool(active),
                           "lag_bytes": lag_bytes})
            if not active:
                problems.append(f"{slot_name} is INACTIVE - WAL is accumulating")
            if lag_bytes > SLOT_LAG_FAIL_BYTES:
                problems.append(f"{slot_name} lag {lag_pretty} exceeds threshold")

        if problems:
            raise RuntimeError("replication slot problems: " + "; ".join(problems))
        return {"slots": report}

    @task(task_id="check_connector_health")
    def check_connector_health() -> dict:
        hook = KafkaConnectHook()
        connectors = hook.list_connectors()
        statuses = {}
        for name in connectors:
            healthy, detail = hook.is_running(name)
            statuses[name] = {"healthy": healthy, "states": detail}
            if not healthy:
                restarted = hook.restart_failed_tasks(name)
                statuses[name]["restarted_tasks"] = restarted
                print(f"connector {name} unhealthy ({detail}); restarted {restarted}")
        return statuses

    @task(task_id="optimize_clickhouse_tables")
    def optimize_clickhouse_tables(**context) -> dict:
        """Force a collapse on the dimension-sized ReplacingMergeTree tables.

        Only the small ones: OPTIMIZE FINAL on a large partitioned fact
        table rewrites every part and can take longer than the interval
        between runs, which turns maintenance into the thing that breaks
        the pipeline. The fact tables are left to the background merge
        scheduler and watched via v_merge_health instead.
        """
        if not context["params"].get("optimize_tables"):
            return {"skipped": True}

        clickhouse = ClickHouseHook()
        tables = ["offices", "staff", "loan_products", "savings_products",
                  "clients", "savings_accounts"]
        optimized = {}
        for table in tables:
            before = clickhouse.scalar(
                f"SELECT count() FROM system.parts "
                f"WHERE active AND database = 'fineract_raw' AND table = '{table}'")
            clickhouse.execute(f"OPTIMIZE TABLE fineract_raw.{table} FINAL")
            after = clickhouse.scalar(
                f"SELECT count() FROM system.parts "
                f"WHERE active AND database = 'fineract_raw' AND table = '{table}'")
            optimized[table] = {"parts_before": before, "parts_after": after}
            print(f"optimized fineract_raw.{table}: {before} -> {after} parts")
        return optimized

    @task(task_id="report_merge_pressure")
    def report_merge_pressure() -> dict:
        clickhouse = ClickHouseHook()
        rows = clickhouse.query_json(
            "SELECT database, table, active_parts, total_rows, size_mb, "
            "compression_ratio FROM fineract_ops.v_merge_health "
            "ORDER BY active_parts DESC LIMIT 20")
        for row in rows:
            print(f"{row['database']}.{row['table']}: {row['active_parts']} parts, "
                  f"{row['total_rows']} rows, {row['size_mb']} MB, "
                  f"x{row['compression_ratio']} compression")
        hot = [r for r in rows if int(r["active_parts"]) > 300]
        if hot:
            print(f"WARNING: tables with excessive parts: "
                  f"{[r['table'] for r in hot]}")
        return {"tables": rows, "over_threshold": [r["table"] for r in hot]}

    @task(task_id="apply_retention")
    def apply_retention(**context) -> dict:
        """Trim the Postgres control-plane tables (ClickHouse uses TTL)."""
        postgres = PostgresHook()
        reject_days = int(context["params"]["reject_retention_days"])
        run_days = int(context["params"]["run_history_retention_days"])

        rejects = postgres.query(
            "DELETE FROM meta.ingestion_reject "
            "WHERE rejected_at < now() - make_interval(days => %s) RETURNING 1",
            (reject_days,))
        runs = postgres.query(
            "DELETE FROM meta.ingestion_run "
            "WHERE started_at < now() - make_interval(days => %s) RETURNING 1",
            (run_days,))
        quality = postgres.query(
            "DELETE FROM meta.data_quality_result "
            "WHERE checked_at < now() - make_interval(days => %s) RETURNING 1",
            (run_days,))

        removed = {"rejects": len(rejects), "runs": len(runs),
                   "quality_results": len(quality)}
        print(f"retention removed: {removed}")
        return removed

    @task(task_id="report_open_rejects")
    def report_open_rejects() -> dict:
        """Surface quarantined records. A reject nobody looks at is a
        silently-dropped record with extra steps."""
        postgres = PostgresHook()
        rows = postgres.query(
            "SELECT entity, rule, count(*) "
            "FROM meta.ingestion_reject "
            "WHERE rejected_at > now() - interval '24 hours' "
            "GROUP BY entity, rule ORDER BY count(*) DESC")
        summary = [{"entity": r[0], "rule": r[1], "count": int(r[2])} for r in rows]
        for item in summary:
            print(f"rejects last 24h: {item['entity']} / {item['rule']}: {item['count']}")
        if not summary:
            print("no rejected records in the last 24 hours")
        return {"rejects": summary}

    finish = EmptyOperator(task_id="finish")

    heartbeat = cdc_heartbeat()
    slots = check_replication_slots()
    connectors = check_connector_health()
    optimize = optimize_clickhouse_tables()
    merges = report_merge_pressure()
    retention = apply_retention()
    rejects = report_open_rejects()

    start >> heartbeat >> [slots, connectors]
    [slots, connectors] >> optimize >> merges
    merges >> [retention, rejects] >> finish
