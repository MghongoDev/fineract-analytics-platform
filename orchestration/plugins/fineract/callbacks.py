"""Failure/SLA callbacks that turn an Airflow event into a metric.

Airflow's own StatsD stream tells you a task failed. It does not tell you
*which data* is now stale, which is the thing an on-call engineer needs
first. These callbacks push a labelled Prometheus metric alongside the
log line, so an alert can say "loans ingestion has been failing for 40
minutes" rather than "a task failed".
"""

from __future__ import annotations

import os
from typing import Any

from airflow.utils.context import Context

PUSHGATEWAY_URL = os.environ.get("PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091")


def _push(metric: str, value: float, labels: dict[str, str]) -> None:
    """Best-effort metric push. Telemetry must never fail a callback."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        gauge = Gauge(metric, f"Airflow event: {metric}",
                      list(labels.keys()), registry=registry)
        gauge.labels(**labels).set(value)
        push_to_gateway(PUSHGATEWAY_URL, job="airflow_events", registry=registry)
    except Exception:  # noqa: BLE001 - deliberately swallowed
        pass


def task_failure_callback(context: Context) -> None:
    task_instance = context.get("task_instance")
    dag_id = context["dag"].dag_id
    task_id = getattr(task_instance, "task_id", "unknown")
    exception = context.get("exception")

    print(f"[ALERT] task failed dag={dag_id} task={task_id} "
          f"run={context.get('run_id')} try={getattr(task_instance, 'try_number', '?')} "
          f"error={exception}")

    _push("airflow_task_failed", 1, {"dag_id": dag_id, "task_id": task_id})


def dag_failure_callback(context: Context) -> None:
    dag_id = context["dag"].dag_id
    print(f"[ALERT] dag run failed dag={dag_id} run={context.get('run_id')}")
    _push("airflow_dag_failed", 1, {"dag_id": dag_id})


def dag_success_callback(context: Context) -> None:
    dag_id = context["dag"].dag_id
    _push("airflow_dag_last_success_timestamp_seconds",
          context["dag_run"].end_date.timestamp() if context["dag_run"].end_date else 0,
          {"dag_id": dag_id})


def sla_miss_callback(dag: Any, task_list: str, blocking_task_list: str,
                      slas: Any, blocking_tis: Any) -> None:
    """An SLA miss is a freshness problem, so it is reported as one."""
    print(f"[ALERT] SLA missed dag={dag.dag_id} tasks={task_list}")
    _push("airflow_sla_missed", 1, {"dag_id": dag.dag_id})
