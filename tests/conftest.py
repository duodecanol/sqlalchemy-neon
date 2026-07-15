"""Pytest configuration and fixtures for sqlalchemy-neon tests."""

from __future__ import annotations

import os

from testsupport.neon_test_safety import validate_destructive_test_database

import pytest
from pytest_asyncio import is_async_test


@pytest.fixture(scope="session", autouse=True)
def configure_test_telemetry() -> None:
    """Enable safe remote test telemetry only when explicitly requested."""
    if os.environ.get("ENABLE_TEST_TELEMETRY") != "1":
        return
    if not os.environ.get("LOGFIRE_TOKEN"):
        return

    import logfire

    logfire.configure(
        send_to_logfire="if-token-present",
        service_name="neon-serverless",
    )
    logfire.instrument_psycopg()
    logfire.instrument_aiohttp_client()

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
    """Provide the explicit test-only Neon connection string."""
    return os.environ.get("NEON_TEST_DATABASE_URL")


@pytest.fixture(scope="session")
def require_neon(neon_connection_string: str | None) -> str:
    """Skip live tests when no explicit test-only database is configured."""
    if not neon_connection_string:
        pytest.skip("NEON_TEST_DATABASE_URL environment variable not set")
    return neon_connection_string


@pytest.fixture(scope="session")
def require_destructive_neon(require_neon: str) -> str:
    """Refuse schema DDL unless the test target is explicitly approved."""
    validate_destructive_test_database(require_neon, os.environ)
    return require_neon
