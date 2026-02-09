"""Pytest configuration and fixtures for sqlalchemy-neon tests."""

from __future__ import annotations

import os
from typing import Generator

import pytest
import logfire


logfire.configure(send_to_logfire=True, service_name="neon-serverless")
logfire.instrument_httpx(capture_all=True)
logfire.instrument_psycopg()
logfire.instrument_aiohttp_client(capture_all=True)


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
