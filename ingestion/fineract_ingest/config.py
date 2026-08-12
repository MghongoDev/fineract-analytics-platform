"""Configuration for the Fineract -> Postgres ingestion service.

Everything is environment driven (12-factor). The same image therefore
runs unchanged against:

* the public Apache Fineract demo  (``https://demo.mifos.io``)
* a self-hosted Fineract container (``https://fineract:8443``)
* a recorded-fixture mock server   (``http://fineract-mock:8090``) used by CI

No secrets are ever baked into the image or the compose file - see
``.env.example`` for the full contract.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class FineractConfig:
    """Connection settings for the Apache Fineract REST API."""

    base_url: str = field(default_factory=lambda: _env(
        "FINERACT_BASE_URL", "https://fineract:8443/fineract-provider/api/v1"))
    tenant_id: str = field(default_factory=lambda: _env("FINERACT_TENANT_ID", "default"))
    username: str = field(default_factory=lambda: _env("FINERACT_USERNAME", "mifos"))
    password: str = field(default_factory=lambda: _env("FINERACT_PASSWORD", "password"))

    #: Fineract demo/self-hosted images ship a self-signed certificate.
    #: Defaults to False so the stack works out of the box; set
    #: FINERACT_VERIFY_SSL=true (and mount a CA bundle) for production.
    verify_ssl: bool = field(default_factory=lambda: _env_bool("FINERACT_VERIFY_SSL", False))

    #: Page size for ``paged=true`` collection endpoints.
    page_size: int = field(default_factory=lambda: _env_int("FINERACT_PAGE_SIZE", 200))

    #: Hard ceiling on pages per entity per run - a guard rail so a
    #: misconfigured cursor can never turn into an unbounded crawl.
    max_pages: int = field(default_factory=lambda: _env_int("FINERACT_MAX_PAGES", 500))

    connect_timeout: float = field(default_factory=lambda: _env_float("FINERACT_CONNECT_TIMEOUT", 10.0))
    read_timeout: float = field(default_factory=lambda: _env_float("FINERACT_READ_TIMEOUT", 60.0))

    max_retries: int = field(default_factory=lambda: _env_int("FINERACT_MAX_RETRIES", 5))
    backoff_base_seconds: float = field(default_factory=lambda: _env_float("FINERACT_BACKOFF_BASE", 0.5))
    backoff_max_seconds: float = field(default_factory=lambda: _env_float("FINERACT_BACKOFF_MAX", 30.0))

    #: Client-side rate limit (requests/second). Fineract is a
    #: transactional core-banking system: an analytics crawler must never
    #: be the reason a teller's request queues.
    requests_per_second: float = field(default_factory=lambda: _env_float("FINERACT_RPS", 8.0))

    #: 'basic' uses HTTP Basic on every call; 'oauth-key' calls
    #: POST /authentication once and reuses base64EncodedAuthenticationKey.
    auth_mode: str = field(default_factory=lambda: _env("FINERACT_AUTH_MODE", "basic"))

    def masked(self) -> dict:
        """Loggable representation - never emits the password."""
        return {
            "base_url": self.base_url,
            "tenant_id": self.tenant_id,
            "username": self.username,
            "password": "***" if self.password else "",
            "verify_ssl": self.verify_ssl,
            "page_size": self.page_size,
            "auth_mode": self.auth_mode,
            "requests_per_second": self.requests_per_second,
        }


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the OLTP landing database."""

    host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "postgres"))
    port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _env("POSTGRES_DB", "fineract_oltp"))
    user: str = field(default_factory=lambda: _env("POSTGRES_USER", "app_ingest"))
    password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "app_ingest"))
    connect_timeout: int = field(default_factory=lambda: _env_int("POSTGRES_CONNECT_TIMEOUT", 15))
    statement_timeout_ms: int = field(default_factory=lambda: _env_int("POSTGRES_STATEMENT_TIMEOUT_MS", 300_000))
    batch_size: int = field(default_factory=lambda: _env_int("INGEST_BATCH_SIZE", 1000))

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password} "
            f"connect_timeout={self.connect_timeout} application_name=fineract_ingest"
        )

    def masked_dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password=***"
        )


@dataclass(frozen=True)
class RuntimeConfig:
    """Cross-cutting runtime knobs."""

    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO").upper())
    log_format: str = field(default_factory=lambda: _env("LOG_FORMAT", "json"))

    #: Pushgateway is used because ingestion is a *batch* job: it exits
    #: before Prometheus could scrape it. Set empty to disable.
    pushgateway_url: str = field(default_factory=lambda: _env(
        "PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091"))
    push_metrics: bool = field(default_factory=lambda: _env_bool("INGEST_PUSH_METRICS", True))

    #: Fail the run if the reject ratio for an entity exceeds this.
    max_reject_ratio: float = field(default_factory=lambda: _env_float("INGEST_MAX_REJECT_RATIO", 0.05))

    dag_run_id: Optional[str] = field(default_factory=lambda: _env("AIRFLOW_CTX_DAG_RUN_ID") or None)
    environment: str = field(default_factory=lambda: _env("ENVIRONMENT", "local"))


@dataclass(frozen=True)
class Settings:
    fineract: FineractConfig = field(default_factory=FineractConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)

    @staticmethod
    def load() -> "Settings":
        return Settings()
