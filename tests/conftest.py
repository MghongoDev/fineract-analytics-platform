"""Shared pytest fixtures.

Integration tests are opt-in: they are skipped unless the environment
points at a running Postgres. That keeps `pytest` fast and green on a
laptop with nothing running, while CI (which does start the services)
runs the full suite. A test suite that cannot be run without the whole
stack up is a test suite that stops being run.
"""

from __future__ import annotations

import os
import socket
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: needs a live Postgres")
    config.addinivalue_line("markers", "slow: takes more than a few seconds")


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def postgres_available() -> bool:
    host = os.environ.get("POSTGRES_HOST", "")
    if not host:
        return False
    if host.startswith("/"):        # unix socket directory
        return True
    return _port_open(host, int(os.environ.get("POSTGRES_PORT", "5432")))


@pytest.fixture(scope="session")
def mock_fineract_url() -> str:
    """A real HTTP Fineract stand-in for the whole test session."""
    from fineract_ingest.mock_server import FineractDataset, Handler

    Handler.dataset = FineractDataset(clients=120, loans=200, seed=99)
    Handler.failure_rate = 0.0
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/fineract-provider/api/v1"
    os.environ["FINERACT_BASE_URL"] = url
    yield url
    server.shutdown()


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT
