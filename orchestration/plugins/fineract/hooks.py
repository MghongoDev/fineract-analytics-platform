"""Thin hooks over the platform components the DAGs touch.

Deliberately not Airflow Connections: every credential in this stack is
already an environment variable (12-factor, same values used by the
ingestion container and by dbt). Adding a second source of truth in the
Airflow metadata DB would mean rotating secrets in two places, and the
usual outcome of that is a pipeline that works until someone rotates only
one of them.
"""

from __future__ import annotations

import json
import os
from typing import Any

import requests


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


class ClickHouseHook:
    """Minimal ClickHouse HTTP client.

    HTTP rather than the native protocol: it needs no extra driver in the
    Airflow image, it is trivially debuggable with curl, and the queries
    the orchestrator runs are small control-plane queries where protocol
    overhead is irrelevant.
    """

    def __init__(self, host: str | None = None, port: int | None = None,
                 user: str | None = None, password: str | None = None,
                 database: str = "default", timeout: int = 120):
        self.host = host or _env("CLICKHOUSE_HOST", "clickhouse")
        self.port = port or int(_env("CLICKHOUSE_HTTP_PORT", "8123"))
        self.user = user or _env("CLICKHOUSE_USER", "analytics")
        self.password = password or _env("CLICKHOUSE_PASSWORD", "analytics")
        self.database = database
        self.timeout = timeout

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def execute(self, query: str, params: dict | None = None) -> str:
        response = requests.post(
            self.url,
            params={"database": self.database, **(params or {})},
            data=query.encode("utf-8"),
            auth=(self.user, self.password),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"ClickHouse query failed ({response.status_code}): "
                f"{response.text[:1000]}")
        return response.text

    def query_json(self, query: str) -> list[dict[str, Any]]:
        raw = self.execute(query.rstrip().rstrip(";") + " FORMAT JSON")
        return json.loads(raw).get("data", [])

    def scalar(self, query: str) -> Any:
        rows = self.query_json(query)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def ping(self) -> bool:
        try:
            return self.execute("SELECT 1").strip() == "1"
        except Exception:
            return False


class PostgresHook:
    """Direct psycopg connection to the OLTP landing database."""

    def __init__(self) -> None:
        self.dsn = (
            f"host={_env('POSTGRES_HOST', 'postgres')} "
            f"port={_env('POSTGRES_PORT', '5432')} "
            f"dbname={_env('POSTGRES_DB', 'fineract_oltp')} "
            f"user={_env('POSTGRES_USER', 'app_ingest')} "
            f"password={_env('POSTGRES_PASSWORD', 'app_ingest')} "
            f"application_name=airflow"
        )

    def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        import psycopg

        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchall() if cursor.description else []

    def scalar(self, sql: str, args: tuple = ()) -> Any:
        rows = self.query(sql, args)
        return rows[0][0] if rows else None

    def ping(self) -> bool:
        try:
            return self.scalar("SELECT 1") == 1
        except Exception:
            return False


class KafkaConnectHook:
    """Kafka Connect REST client - used to assert the CDC path is alive."""

    def __init__(self, url: str | None = None, timeout: int = 30):
        self.url = (url or _env("KAFKA_CONNECT_URL", "http://kafka-connect:8083")).rstrip("/")
        self.timeout = timeout

    def connector_status(self, name: str) -> dict:
        response = requests.get(f"{self.url}/connectors/{name}/status",
                                timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def list_connectors(self) -> list[str]:
        response = requests.get(f"{self.url}/connectors", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def is_running(self, name: str) -> tuple[bool, str]:
        """(healthy, detail). A connector can be RUNNING while its task is
        FAILED - checking only the connector state is the classic way to
        miss a dead CDC pipeline."""
        try:
            status = self.connector_status(name)
        except Exception as exc:
            return False, f"status call failed: {exc}"
        states = [status.get("connector", {}).get("state", "UNKNOWN")]
        states += [task.get("state", "UNKNOWN") for task in status.get("tasks", [])]
        healthy = bool(states) and all(state == "RUNNING" for state in states)
        return healthy, ",".join(states)

    def restart_failed_tasks(self, name: str) -> list[int]:
        restarted = []
        status = self.connector_status(name)
        for task in status.get("tasks", []):
            if task.get("state") == "FAILED":
                task_id = task["id"]
                requests.post(f"{self.url}/connectors/{name}/tasks/{task_id}/restart",
                              timeout=self.timeout)
                restarted.append(task_id)
        return restarted


class FineractHook:
    """Reachability probe for the source API."""

    def __init__(self) -> None:
        self.base_url = _env(
            "FINERACT_BASE_URL", "https://fineract:8443/fineract-provider/api/v1")
        self.tenant = _env("FINERACT_TENANT_ID", "default")
        self.username = _env("FINERACT_USERNAME", "mifos")
        self.password = _env("FINERACT_PASSWORD", "password")
        self.verify = _env("FINERACT_VERIFY_SSL", "false").lower() == "true"

    def ping(self) -> bool:
        try:
            response = requests.get(
                f"{self.base_url.rstrip('/')}/offices",
                params={"limit": 1},
                headers={"Fineract-Platform-TenantId": self.tenant},
                auth=(self.username, self.password),
                timeout=30,
                verify=self.verify,
            )
            return response.status_code == 200
        except Exception:
            return False
