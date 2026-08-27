"""Sensors that gate the transformation layer on the CDC layer.

The central problem this file solves: ingestion writes to Postgres and
dbt reads from ClickHouse, but nothing in Airflow connects the two - the
CDC stream is asynchronous and outside the DAG. Running dbt immediately
after ingestion would transform a ClickHouse that has not yet received
the rows, produce a technically-successful run, and publish stale
numbers. That is the worst failure mode a pipeline can have, because it
is invisible.

`CDCCaughtUpSensor` closes the loop by waiting until ClickHouse has
actually observed the change, before any transformation starts.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from .hooks import ClickHouseHook, FineractHook, KafkaConnectHook, PostgresHook


class ServiceHealthSensor(BaseSensorOperator):
    """Wait for a platform component to answer.

    Used as a preflight gate: failing here costs nothing, whereas failing
    halfway through ingestion leaves a partially-loaded batch to reason
    about.
    """

    ui_color = "#c5e5ff"

    def __init__(self, service: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.service = service

    def poke(self, context: Context) -> bool:
        hooks = {
            "fineract": FineractHook().ping,
            "postgres": PostgresHook().ping,
            "clickhouse": ClickHouseHook().ping,
        }
        if self.service not in hooks:
            raise AirflowFailException(f"unknown service '{self.service}'")
        healthy = hooks[self.service]()
        self.log.info("health check %s -> %s", self.service, healthy)
        return healthy


class KafkaConnectorHealthSensor(BaseSensorOperator):
    """Assert the Debezium connector AND all its tasks are RUNNING."""

    ui_color = "#c5e5ff"

    def __init__(self, connector_name: str = "fineract-oltp-source",
                 restart_failed: bool = True, **kwargs: Any):
        super().__init__(**kwargs)
        self.connector_name = connector_name
        self.restart_failed = restart_failed

    def poke(self, context: Context) -> bool:
        hook = KafkaConnectHook()
        healthy, detail = hook.is_running(self.connector_name)
        self.log.info("connector %s states: %s", self.connector_name, detail)
        if not healthy and self.restart_failed and "FAILED" in detail:
            # A Debezium task that failed on a transient broker blip
            # recovers on restart. Self-healing here saves a page for
            # something the platform can fix itself; if it keeps failing,
            # the sensor still times out and the alert fires.
            restarted = hook.restart_failed_tasks(self.connector_name)
            if restarted:
                self.log.warning("restarted failed tasks: %s", restarted)
        return healthy


class CDCCaughtUpSensor(BaseSensorOperator):
    """Wait until ClickHouse has caught up with Postgres.

    Two independent conditions, both of which must hold:

    1. **Row parity.** For each watched entity, the count of live keys in
       ClickHouse is within `row_tolerance` of the Postgres count. This
       catches events that were dropped entirely.
    2. **Freshness.** The newest source commit time visible in ClickHouse
       is no older than `max_lag_seconds`. This catches a stream that is
       merely slow.

    Row parity alone is not enough (an idle stream trivially matches) and
    freshness alone is not enough (a stream that is delivering some events
    while silently dropping others looks perfectly fresh). Requiring both
    is what makes this a real gate.
    """

    ui_color = "#ffe0b2"
    template_fields: Sequence[str] = ("entities",)

    ENTITY_TABLES = {
        "clients": ("oltp.clients", "clients"),
        "loans": ("oltp.loans", "loans"),
        "loan_transactions": ("oltp.loan_transactions", "loan_transactions"),
        "savings_accounts": ("oltp.savings_accounts", "savings_accounts"),
        "offices": ("oltp.offices", "offices"),
        "staff": ("oltp.staff", "staff"),
        "loan_products": ("oltp.loan_products", "loan_products"),
        "savings_products": ("oltp.savings_products", "savings_products"),
    }

    def __init__(self, entities: Sequence[str] | None = None,
                 row_tolerance: int = 0,
                 max_lag_seconds: int = 300,
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.entities = list(entities or ["clients", "loans", "loan_transactions"])
        self.row_tolerance = row_tolerance
        self.max_lag_seconds = max_lag_seconds

    def poke(self, context: Context) -> bool:
        postgres = PostgresHook()
        clickhouse = ClickHouseHook()

        clickhouse_counts = {
            row["entity"]: int(row["live_keys"])
            for row in clickhouse.query_json(
                "SELECT entity, live_keys FROM fineract_ops.v_reconciliation_counts")
        }

        caught_up = True
        for entity in self.entities:
            if entity not in self.ENTITY_TABLES:
                raise AirflowFailException(f"unknown entity '{entity}'")
            pg_table, ch_entity = self.ENTITY_TABLES[entity]
            source_rows = int(postgres.scalar(f"SELECT count(*) FROM {pg_table}") or 0)
            target_rows = clickhouse_counts.get(ch_entity, 0)
            delta = source_rows - target_rows

            self.log.info("reconciliation %-18s postgres=%-8d clickhouse=%-8d delta=%d",
                          entity, source_rows, target_rows, delta)
            if abs(delta) > self.row_tolerance:
                caught_up = False

        freshness = clickhouse.query_json(
            "SELECT source_table, freshness_seconds "
            "FROM fineract_ops.v_cdc_freshness")
        for row in freshness:
            seconds = float(row["freshness_seconds"])
            self.log.info("freshness %-18s %.0fs", row["source_table"], seconds)
            if seconds > self.max_lag_seconds:
                caught_up = False

        # An empty freshness view means no events in the audit window.
        # That is normal on a first run or a quiet period, so it does not
        # by itself fail the gate - row parity still has to hold.
        return caught_up


class DataFreshnessSensor(BaseSensorOperator):
    """Assert an entity was successfully ingested recently enough."""

    ui_color = "#ffe0b2"

    def __init__(self, entity: str, max_age_seconds: int = 7200, **kwargs: Any):
        super().__init__(**kwargs)
        self.entity = entity
        self.max_age_seconds = max_age_seconds

    def poke(self, context: Context) -> bool:
        age = PostgresHook().scalar(
            "SELECT EXTRACT(EPOCH FROM (now() - last_success_at)) "
            "FROM meta.ingestion_watermark WHERE entity = %s", (self.entity,))
        if age is None:
            self.log.info("no watermark yet for %s", self.entity)
            return False
        self.log.info("%s last succeeded %.0fs ago", self.entity, float(age))
        return float(age) <= self.max_age_seconds
