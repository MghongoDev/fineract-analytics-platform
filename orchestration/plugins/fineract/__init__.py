"""Shared Airflow building blocks for the Fineract analytics pipeline.

Kept in `plugins/` (on the Airflow PYTHONPATH) rather than inline in the
DAG files so that the logic is importable by the DAG-integrity tests and
reusable across the pipeline, backfill and maintenance DAGs.
"""

from .callbacks import (
    dag_failure_callback,
    dag_success_callback,
    sla_miss_callback,
    task_failure_callback,
)
from .hooks import ClickHouseHook, FineractHook, KafkaConnectHook, PostgresHook
from .operators import DataQualityGateOperator, DbtOperator, PublishDbtResultsOperator
from .sensors import (
    CDCCaughtUpSensor,
    DataFreshnessSensor,
    KafkaConnectorHealthSensor,
    ServiceHealthSensor,
)

__all__ = [
    "ClickHouseHook",
    "FineractHook",
    "KafkaConnectHook",
    "PostgresHook",
    "DbtOperator",
    "PublishDbtResultsOperator",
    "DataQualityGateOperator",
    "ServiceHealthSensor",
    "KafkaConnectorHealthSensor",
    "CDCCaughtUpSensor",
    "DataFreshnessSensor",
    "task_failure_callback",
    "dag_failure_callback",
    "dag_success_callback",
    "sla_miss_callback",
]
