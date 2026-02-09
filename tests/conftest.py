"""Pytest configuration and fixtures for sqlalchemy-neon tests."""

from __future__ import annotations

import os

import pytest
import logfire
from pytest_asyncio import is_async_test



logfire.configure(send_to_logfire=True, service_name="neon-serverless", scrubbing=False)
logfire.instrument_psycopg()
logfire.instrument_aiohttp_client(capture_all=True)

def pytest_collection_modifyitems(items):
    # https://pytest-asyncio.readthedocs.io/en/v0.24.0/how-to-guides/run_session_tests_in_same_loop.html
    print("pytest_collection_modifyitems called".center(80, "/"))
    pytest_asyncio_tests = (item for item in items if is_async_test(item))
    session_scope_marker = pytest.mark.asyncio(loop_scope="session")
    for async_test in pytest_asyncio_tests:
        print(async_test)
        async_test.add_marker(session_scope_marker, append=False)

@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Backend for anyio pytest plugin.

    :return: backend name.
    """
    return "asyncio"


@pytest.fixture(scope="session")
def mock_connection_string() -> str:
    """Provide a mock connection string for testing."""
    return "postgresql://testuser:testpass@test-host.neon.tech:5432/testdb"


@pytest.fixture(scope="session")
def neon_connection_string() -> str | None:
    """Provide real Neon connection string from environment.

    Returns None if NEON_DATABASE_URL is not set.
    """
    return os.environ.get("NEON_DATABASE_URL")


@pytest.fixture(scope="session")
def require_neon(neon_connection_string: str | None):
    """Skip test if Neon connection is not available."""
    if neon_connection_string is None:
        pytest.skip("NEON_DATABASE_URL environment variable not set")
    return neon_connection_string
