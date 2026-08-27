"""HTTP client for the Apache Fineract v1 REST API.

Responsibilities kept deliberately narrow: authentication, transport
resilience, pagination and instrumentation. It returns raw ``dict``
payloads - parsing and validation live in :mod:`parsers` /
:mod:`validation` so that each concern can be unit-tested alone.

Fineract specifics handled here
-------------------------------
``Fineract-Platform-TenantId``
    Mandatory on every request; Fineract is multi-tenant and a missing
    header returns a 400 that reads like an auth error.

Two auth modes
    ``basic``      - HTTP Basic on each request (works everywhere).
    ``oauth-key``  - ``POST /authentication`` once, then reuse the
                     returned ``base64EncodedAuthenticationKey``. Cheaper
                     for large crawls because Fineract skips the password
                     hash round on every call.

Pagination
    Collection endpoints accept ``paged=true&offset=&limit=`` and reply
    with ``{"totalFilteredRecords": N, "pageItems": [...]}``. A handful of
    endpoints (``/offices``, ``/staff``, ``/loanproducts``) ignore
    ``paged`` and return a bare list - :meth:`iter_pages` normalises both.

Retries
    Idempotent GETs are retried with exponential backoff **and jitter** on
    429/5xx and on connection errors. Jitter matters: without it, a fleet
    of workers retrying a recovering core-banking node re-synchronises
    into a thundering herd.
"""

from __future__ import annotations

import base64
import random
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

from .config import FineractConfig
from .logging_setup import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class FineractError(RuntimeError):
    """Raised when the API cannot be read after exhausting retries."""

    def __init__(self, message: str, status_code: int | None = None, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body[:2000]


class RateLimiter:
    """Simple token-bucket style pacer (one process, one thread)."""

    def __init__(self, rps: float):
        self.min_interval = 1.0 / rps if rps and rps > 0 else 0.0
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


class FineractClient:
    """Thin, resilient wrapper around the Fineract v1 API."""

    def __init__(self, config: FineractConfig | None = None,
                 session: requests.Session | None = None):
        self.config = config or FineractConfig()
        self.session = session or requests.Session()
        self.session.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=8))
        self.session.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=8))
        self._limiter = RateLimiter(self.config.requests_per_second)
        self._auth_key: str | None = None

        # Counters surfaced as Prometheus metrics by the pipeline.
        self.request_count = 0
        self.retry_count = 0
        self.error_count = 0
        self.bytes_received = 0
        self.total_latency_seconds = 0.0

        if not self.config.verify_ssl:
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        headers = {
            "Fineract-Platform-TenantId": self.config.tenant_id,
            "Accept": "application/json",
            "User-Agent": "fineract-analytics-ingest/1.0",
        }
        if self.config.auth_mode == "oauth-key" and self._auth_key:
            headers["Authorization"] = f"Basic {self._auth_key}"
        else:
            raw = f"{self.config.username}:{self.config.password}".encode()
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode()
        return headers

    def authenticate(self) -> None:
        """Exchange credentials for a reusable authentication key.

        Only used in ``oauth-key`` mode. Failure here is non-fatal: we log
        and fall back to Basic, because a working (if slightly chattier)
        pipeline beats a hard failure on an optional optimisation.
        """
        if self.config.auth_mode != "oauth-key":
            return
        url = self._url("authentication")
        try:
            self._limiter.wait()
            response = self.session.post(
                url,
                json={"username": self.config.username, "password": self.config.password},
                headers={"Fineract-Platform-TenantId": self.config.tenant_id,
                         "Content-Type": "application/json"},
                timeout=(self.config.connect_timeout, self.config.read_timeout),
                verify=self.config.verify_ssl,
            )
            self.request_count += 1
            response.raise_for_status()
            self._auth_key = response.json().get("base64EncodedAuthenticationKey")
            log.info("fineract_authenticated", extra={"auth_mode": "oauth-key"})
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("fineract_authentication_failed_falling_back_to_basic",
                        extra={"error": str(exc)})
            self._auth_key = None

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------
    def _url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/") + "/"
        return urljoin(base, path.lstrip("/"))

    def _sleep_for_attempt(self, attempt: int, retry_after: str | None) -> None:
        if retry_after:
            try:
                time.sleep(min(float(retry_after), self.config.backoff_max_seconds))
                return
            except ValueError:
                pass
        delay = min(self.config.backoff_base_seconds * (2 ** attempt),
                    self.config.backoff_max_seconds)
        time.sleep(delay * (0.5 + random.random() / 2))  # full-ish jitter

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET with retry/backoff. Returns the decoded JSON body."""
        url = self._url(path)
        last_error: Exception | None = None

        for attempt in range(self.config.max_retries + 1):
            self._limiter.wait()
            started = time.monotonic()
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=(self.config.connect_timeout, self.config.read_timeout),
                    verify=self.config.verify_ssl,
                )
                self.request_count += 1
                self.total_latency_seconds += time.monotonic() - started
                self.bytes_received += len(response.content or b"")

                if response.status_code in RETRYABLE_STATUS:
                    self.retry_count += 1
                    log.warning("fineract_retryable_status", extra={
                        "path": path, "status": response.status_code, "attempt": attempt})
                    if attempt < self.config.max_retries:
                        self._sleep_for_attempt(attempt, response.headers.get("Retry-After"))
                        continue
                    self.error_count += 1
                    raise FineractError(
                        f"GET {path} failed after {attempt + 1} attempts",
                        response.status_code, response.text)

                if response.status_code >= 400:
                    # Non-retryable (403 permission, 404 missing endpoint...).
                    self.error_count += 1
                    raise FineractError(
                        f"GET {path} returned {response.status_code}",
                        response.status_code, response.text)

                return response.json()

            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                self.retry_count += 1
                log.warning("fineract_transport_error", extra={
                    "path": path, "attempt": attempt, "error": str(exc)})
                if attempt < self.config.max_retries:
                    self._sleep_for_attempt(attempt, None)
                    continue
                self.error_count += 1
                raise FineractError(f"GET {path} transport failure: {exc}") from exc
            except ValueError as exc:  # invalid JSON
                self.error_count += 1
                raise FineractError(f"GET {path} returned non-JSON body: {exc}") from exc

        raise FineractError(f"GET {path} exhausted retries: {last_error}")

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------
    @staticmethod
    def _items_of(payload: Any) -> tuple[list[dict], int | None]:
        """Normalise Fineract's two collection shapes into (items, total)."""
        if isinstance(payload, list):
            return payload, len(payload)
        if isinstance(payload, dict):
            if "pageItems" in payload:
                return payload.get("pageItems") or [], payload.get("totalFilteredRecords")
            # single-resource GET
            return [payload], 1
        return [], 0

    def iter_pages(self, path: str,
                   params: dict[str, Any] | None = None,
                   paged: bool = True) -> Iterator[list[dict]]:
        """Yield successive pages of a collection endpoint.

        Terminates on: short page, offset >= totalFilteredRecords, empty
        page, or ``max_pages`` - whichever comes first. The belt-and-braces
        conditions matter because a few Fineract endpoints under-report
        ``totalFilteredRecords`` when row-level filters are applied.
        """
        base_params = dict(params or {})
        if not paged:
            payload = self.get(path, base_params)
            items, _ = self._items_of(payload)
            if items:
                yield items
            return

        offset = 0
        limit = self.config.page_size
        for page_no in range(self.config.max_pages):
            page_params = dict(base_params)
            page_params.update({"paged": "true", "offset": offset, "limit": limit})
            payload = self.get(path, page_params)
            items, total = self._items_of(payload)

            if not items:
                return
            yield items

            offset += len(items)
            if len(items) < limit:
                return
            if total is not None and offset >= total:
                return
            if page_no == self.config.max_pages - 1:
                log.warning("fineract_max_pages_reached", extra={
                    "path": path, "max_pages": self.config.max_pages, "fetched": offset})

    def iter_items(self, path: str,
                   params: dict[str, Any] | None = None,
                   paged: bool = True) -> Iterator[dict]:
        for page in self.iter_pages(path, params=params, paged=paged):
            yield from page

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    def health_check(self) -> bool:
        """Cheap reachability probe used by the Airflow sensor."""
        try:
            self.get("offices", {"limit": 1})
            return True
        except FineractError as exc:
            log.error("fineract_health_check_failed", extra={"error": str(exc)})
            return False

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> FineractClient:
        self.authenticate()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
