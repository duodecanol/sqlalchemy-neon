"""Tests for Neon HTTP client."""

from __future__ import annotations

import aiohttp
import pytest
import pytest_asyncio

from sqlalchemy_neon.neon_http_client import (
    AsyncNeonHTTPClient,
    NeonHTTPClient,
    IsolationLevel,
    TransactionOptions,
)
from sqlalchemy_neon.errors import NeonConfigurationError


class TestAsyncNeonHTTPClient:
    """Tests for AsyncNeonHTTPClient class."""

    def test_parse_endpoint_postgresql(self):
        """Test parsing postgresql:// URL."""
        client = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        assert client._url == "https://host.neon.tech/sql"

    def test_parse_endpoint_postgres(self):
        """Test parsing postgres:// URL."""
        client = AsyncNeonHTTPClient("postgres://user:pass@host.neon.tech/db")
        assert client._url == "https://host.neon.tech/sql"

    def test_parse_endpoint_invalid_scheme(self):
        """Test invalid scheme raises error."""
        with pytest.raises(NeonConfigurationError, match="Invalid scheme"):
            AsyncNeonHTTPClient("mysql://user:pass@host/db")

    def test_parse_endpoint_missing_host(self):
        """Test missing hostname raises error."""
        with pytest.raises(NeonConfigurationError, match="hostname"):
            AsyncNeonHTTPClient("postgresql:///db")

    def test_parse_endpoint_missing_user(self):
        """Test missing username raises error."""
        with pytest.raises(NeonConfigurationError, match="username"):
            AsyncNeonHTTPClient("postgresql://host.neon.tech/db")

    def test_parse_endpoint_missing_database(self):
        """Test missing database raises error."""
        with pytest.raises(NeonConfigurationError, match="database"):
            AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/")

    def test_build_headers_basic(self):
        """Test building basic headers."""
        client = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        headers = client._build_headers()

        assert headers["Content-Type"] == "application/json"
        assert "Neon-Connection-String" in headers
        assert headers["Neon-Raw-Text-Output"] == "true"
        assert headers["Neon-Array-Mode"] == "false"

    def test_build_headers_array_mode(self):
        """Test array mode header."""
        client = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        headers = client._build_headers(array_mode=True)

        assert headers["Neon-Array-Mode"] == "true"

    def test_build_headers_auth_token(self):
        """Test auth token header."""
        client = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db",
            auth_token="my-jwt-token",
        )
        headers = client._build_headers()

        assert headers["Authorization"] == "Bearer my-jwt-token"

    def test_build_headers_transaction(self):
        """Test transaction headers."""
        client = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        options = TransactionOptions(
            isolation_level=IsolationLevel.SERIALIZABLE,
            read_only=True,
            deferrable=True,
        )
        headers = client._build_headers(transaction_options=options)

        assert headers["Neon-Batch-Isolation-Level"] == "Serializable"
        assert headers["Neon-Batch-Read-Only"] == "true"
        assert headers["Neon-Batch-Deferrable"] == "true"

    def test_external_client_flag(self):
        """Test that external client is tracked correctly."""
        # Without external client
        client1 = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        assert client1._external_client is False

        # With external client (mock)
        class MockClient:
            async def close(self):
                return None

        mock_client = MockClient()
        client2 = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db",
            http_client=mock_client,
        )
        assert client2._external_client is True

    def test_neon_http_client_alias(self):
        """Test NeonHTTPClient is an alias for AsyncNeonHTTPClient."""
        assert NeonHTTPClient is AsyncNeonHTTPClient


class TestTransactionOptions:
    """Tests for TransactionOptions validation."""

    def test_default_options(self):
        """Test default transaction options."""
        options = TransactionOptions()
        assert options.isolation_level == IsolationLevel.READ_COMMITTED
        assert options.read_only is False
        assert options.deferrable is False

    def test_deferrable_requires_serializable(self):
        """Test deferrable requires SERIALIZABLE isolation."""
        with pytest.raises(NeonConfigurationError, match="SERIALIZABLE"):
            TransactionOptions(
                isolation_level=IsolationLevel.READ_COMMITTED,
                read_only=True,
                deferrable=True,
            )

    def test_deferrable_requires_read_only(self):
        """Test deferrable requires read_only."""
        with pytest.raises(NeonConfigurationError, match="read_only"):
            TransactionOptions(
                isolation_level=IsolationLevel.SERIALIZABLE,
                read_only=False,
                deferrable=True,
            )

    def test_valid_deferrable(self):
        """Test valid deferrable configuration."""
        options = TransactionOptions(
            isolation_level=IsolationLevel.SERIALIZABLE,
            read_only=True,
            deferrable=True,
        )
        assert options.deferrable is True


class TestIsolationLevel:
    """Tests for IsolationLevel enum."""

    def test_isolation_level_values(self):
        """Test isolation level string values."""
        assert IsolationLevel.READ_UNCOMMITTED.value == "ReadUncommitted"
        assert IsolationLevel.READ_COMMITTED.value == "ReadCommitted"
        assert IsolationLevel.REPEATABLE_READ.value == "RepeatableRead"
        assert IsolationLevel.SERIALIZABLE.value == "Serializable"


class TestAsyncContextManager:
    """Tests for async context manager."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager opens and closes properly."""
        async with AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db"
        ) as client:
            assert client._http_client is None  # Not created until first request

    @pytest.mark.asyncio
    async def test_close_external_client_not_closed(self):
        """Test that external client is not closed by client."""
        mock_client = aiohttp.ClientSession()

        client = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db",
            http_client=mock_client,
        )
        await client.close()
        # External client should still be usable (not closed)
        assert mock_client.closed is False
        await mock_client.close()
