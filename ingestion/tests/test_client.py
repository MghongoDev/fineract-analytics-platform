"""Tests for the Fineract HTTP client.

Run against the real mock server rather than a mocked `requests` object:
mocking the transport tests that we call requests correctly, which is not
the interesting question. The interesting questions are whether
pagination terminates, whether retries actually retry, and whether the
tenant header is always sent - and only a real socket answers those.
"""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from fineract_ingest.client import FineractClient, FineractError
from fineract_ingest.config import FineractConfig
from fineract_ingest.mock_server import FineractDataset, Handler


class IsolatedHandler(Handler):
    """A private handler subclass for this module.

    `Handler.dataset` and `Handler.failure_rate` are class attributes, so
    two test modules sharing the base class would stamp on each other's
    fault injection when the suite runs as a whole. Subclassing gives
    this module its own attribute namespace - a one-line fix for a
    genuinely confusing class of cross-test interference.
    """

    dataset = None
    failure_rate = 0.0


@pytest.fixture(scope="module")
def mock_server():
    IsolatedHandler.dataset = FineractDataset(clients=250, loans=120, seed=7)
    IsolatedHandler.failure_rate = 0.0
    server = ThreadingHTTPServer(("127.0.0.1", 0), IsolatedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/fineract-provider/api/v1"
    server.shutdown()


@pytest.fixture
def client(mock_server):
    config = FineractConfig(
        base_url=mock_server, tenant_id="default", username="mifos",
        password="password", verify_ssl=False, page_size=50, max_pages=100,
        connect_timeout=5, read_timeout=15, max_retries=3,
        backoff_base_seconds=0.01, backoff_max_seconds=0.05,
        requests_per_second=0, auth_mode="basic")
    instance = FineractClient(config)
    yield instance
    instance.close()


class TestAuthentication:
    def test_tenant_header_is_always_sent(self, client):
        headers = client._headers()
        assert headers["Fineract-Platform-TenantId"] == "default"
        assert headers["Authorization"].startswith("Basic ")

    def test_missing_tenant_header_is_rejected_by_the_api(self, mock_server):
        import requests
        response = requests.get(f"{mock_server}/offices",
                                headers={"Authorization": "Basic x"}, timeout=10)
        assert response.status_code == 400

    def test_missing_authorization_is_rejected(self, mock_server):
        import requests
        response = requests.get(
            f"{mock_server}/offices",
            headers={"Fineract-Platform-TenantId": "default"}, timeout=10)
        assert response.status_code == 401

    def test_oauth_key_mode_exchanges_credentials(self, mock_server):
        config = FineractConfig(base_url=mock_server, auth_mode="oauth-key",
                                requests_per_second=0)
        instance = FineractClient(config)
        instance.authenticate()
        assert instance._auth_key is not None
        instance.close()


class TestPagination:
    def test_unpaged_endpoint_returns_a_bare_list(self, client):
        offices = list(client.iter_items("offices", paged=False))
        assert len(offices) == 6
        assert all("id" in office for office in offices)

    def test_paged_endpoint_walks_every_page(self, client):
        clients = list(client.iter_items("clients", paged=True))
        assert len(clients) == 250, "pagination must reach the last record"

    def test_pagination_yields_unique_records(self, client):
        ids = [c["id"] for c in client.iter_items("clients", paged=True)]
        assert len(ids) == len(set(ids)), "a page boundary duplicated records"

    def test_page_size_is_respected(self, client):
        pages = list(client.iter_pages("clients", paged=True))
        assert all(len(page) <= 50 for page in pages)
        assert len(pages) == 5

    def test_max_pages_bounds_the_crawl(self, mock_server):
        config = FineractConfig(base_url=mock_server, page_size=10, max_pages=2,
                                requests_per_second=0)
        instance = FineractClient(config)
        records = list(instance.iter_items("clients", paged=True))
        assert len(records) == 20, "max_pages must cap an unbounded crawl"
        instance.close()

    def test_child_collection(self, client):
        loans = list(client.iter_items("loans", paged=True))
        loan_id = next(loan["id"] for loan in loans if loan["status"]["id"] >= 300)
        transactions = list(
            client.iter_items(f"loans/{loan_id}/transactions", paged=False))
        assert transactions
        assert all(tx["loanId"] == loan_id for tx in transactions)


class TestResilience:
    def test_transient_failures_are_retried(self, mock_server):
        """A 503 must be retried, not surfaced.

        Deterministic injection (fail exactly the next 3 requests) rather
        than a probability: a probabilistic 503 on a single-request
        endpoint makes this test a coin flip, and a flaky test that
        guards a resilience feature is worse than no test.
        """
        IsolatedHandler.fail_next_n = 3
        try:
            config = FineractConfig(
                base_url=mock_server, max_retries=12, backoff_base_seconds=0.001,
                backoff_max_seconds=0.01, requests_per_second=0)
            instance = FineractClient(config)
            offices = list(instance.iter_items("offices", paged=False))
            assert len(offices) == 6, "the request never succeeded after retrying"
            assert instance.retry_count == 3, (
                f"expected exactly 3 retries, saw {instance.retry_count}")
            instance.close()
        finally:
            IsolatedHandler.fail_next_n = 0

    def test_exhausted_retries_raise_fineract_error(self, mock_server):
        IsolatedHandler.failure_rate = 1.0
        try:
            config = FineractConfig(
                base_url=mock_server, max_retries=2, backoff_base_seconds=0.001,
                backoff_max_seconds=0.01, requests_per_second=0)
            instance = FineractClient(config)
            with pytest.raises(FineractError) as error:
                instance.get("offices")
            assert error.value.status_code == 503
            instance.close()
        finally:
            IsolatedHandler.failure_rate = 0.0

    def test_non_retryable_status_fails_immediately(self, client):
        with pytest.raises(FineractError) as error:
            client.get("this/path/does/not/exist")
        assert error.value.status_code == 404

    def test_rate_limiter_paces_requests(self, mock_server):
        import time

        config = FineractConfig(base_url=mock_server, requests_per_second=20)
        instance = FineractClient(config)
        started = time.monotonic()
        for _ in range(5):
            instance.get("offices")
        elapsed = time.monotonic() - started
        # 5 requests at 20 rps cannot complete faster than ~0.2s.
        assert elapsed >= 0.15, "the client-side rate limit was not applied"
        instance.close()

    def test_health_check(self, client):
        assert client.health_check() is True

    def test_request_counters_are_maintained(self, client):
        before = client.request_count
        list(client.iter_items("offices", paged=False))
        assert client.request_count > before
