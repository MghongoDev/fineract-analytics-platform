"""Configuration for the pipeline exporter.

Environment driven, matching the convention used across the rest of the
platform (see ``ingestion/fineract_ingest/config.py``): the same image runs
unchanged in every environment because nothing is baked in, and defaults
match the demo-stack service names so the exporter works out of the box in
docker compose.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the control-plane database (``meta.*``)."""

    host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "postgres"))
    port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _env("POSTGRES_DB", "fineract_oltp"))
    user: str = field(default_factory=lambda: _env("POSTGRES_USER", "analyst_ro"))
    password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "analyst_ro"))
    connect_timeout: int = field(default_factory=lambda: _env_int("POSTGRES_CONNECT_TIMEOUT", 10))


@dataclass(frozen=True)
class ClickHouseConfig:
    """Connection settings for the ClickHouse HTTP interface.

    HTTP (rather than the native protocol) is deliberate: it needs no
    compiled client library, speaks plain JSON, and is the same interface
    dbt and ad-hoc curl debugging already use - one fewer thing to reason
    about when a scrape fails at 3am.
    """

    host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "clickhouse"))
    http_port: int = field(default_factory=lambda: _env_int("CLICKHOUSE_HTTP_PORT", 8123))
    user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "analytics"))
    password: str = field(default_factory=lambda: _env("CLICKHOUSE_PASSWORD", "analytics"))
    request_timeout: float = field(
        default_factory=lambda: float(_env("CLICKHOUSE_HTTP_TIMEOUT", "10")))


@dataclass(frozen=True)
class ExporterConfig:
    """Top-level exporter settings."""

    port: int = field(default_factory=lambda: _env_int("EXPORTER_PORT", 9105))
    scrape_interval_seconds: int = field(
        default_factory=lambda: _env_int("EXPORTER_SCRAPE_INTERVAL_SECONDS", 30))
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
