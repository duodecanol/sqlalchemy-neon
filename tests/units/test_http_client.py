"""Tests for Neon HTTP client."""

from __future__ import annotations

import asyncio
import aiohttp
import pytest

from sqlalchemy_neon.neon_http_client import (
    AsyncNeonHTTPClient,
    AsyncNeonWebSocketClient,
    AsyncNeonWebSocketPool,
    NeonWebSocketClient,
    NeonHTTPClient,
    IsolationLevel,
    QueryResult,
    TransactionOptions,
)
from sqlalchemy_neon.errors import NeonConfigurationError


class TestAsyncNeonHTTPClient:
    """Tests for AsyncNeonHTTPClient class."""

    def test_parse_endpoint_postgresql(self):
        """Test parsing postgresql:// URL."""
        client = AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db")
        assert client._url == "https://api.neon.tech/sql"

    def test_parse_endpoint_postgres(self):
        """Test parsing postgres:// URL."""
        client = AsyncNeonHTTPClient("postgres://user:pass@host.neon.tech/db")
        assert client._url == "https://api.neon.tech/sql"

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

    def test_custom_fetch_endpoint_static(self):
        client = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db",
            fetch_endpoint="https://proxy.local/sql",
        )
        assert client._url == "https://proxy.local/sql"

    def test_custom_fetch_endpoint_callable(self):
        client = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech:5444/db",
            fetch_endpoint=lambda host, port, jwt_auth: (
                f"https://edge.local/sql?host={host}&port={port}&auth={int(jwt_auth)}"
            ),
            auth_token="x",
        )
        assert (
            client._url
            == "https://edge.local/sql?host=host.neon.tech&port=5444&auth=1"
        )

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


class TestAsyncNeonWebSocketClient:
    """Tests for AsyncNeonWebSocketClient class."""

    def test_parse_websocket_endpoint_default(self):
        client = AsyncNeonWebSocketClient(
            "postgresql://user:pass@host.neon.tech:5432/db"
        )
        assert client._ws_url == "wss://host.neon.tech/v2"

    def test_parse_websocket_endpoint_proxy_string(self):
        client = AsyncNeonWebSocketClient(
            "postgresql://user:pass@host.neon.tech:5432/db",
            ws_proxy="proxy.internal/ws",
            use_secure_websocket=False,
        )
        assert client._ws_url == "ws://proxy.internal/ws?address=host.neon.tech:5432"

    def test_parse_websocket_endpoint_proxy_callable(self):
        client = AsyncNeonWebSocketClient(
            "postgresql://user:pass@host.neon.tech:5432/db",
            ws_proxy=lambda host, port: f"proxy.internal/ws?address={host}:{port}",
        )
        assert client._ws_url == "wss://proxy.internal/ws?address=host.neon.tech:5432"

    def test_neon_websocket_client_alias(self):
        assert NeonWebSocketClient is AsyncNeonWebSocketClient

    def test_ws_url_default_port(self):
        """Test default port is 5432 when not specified."""
        client = AsyncNeonWebSocketClient(
            "postgresql://user:pass@host.neon.tech/db"
        )
        assert client._ws_url == "wss://host.neon.tech/v2"


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

    @pytest.mark.asyncio
    async def test_callable_client_factory_is_closed_by_owner(self):
        created: list[aiohttp.ClientSession] = []

        async def factory():
            sess = aiohttp.ClientSession()
            created.append(sess)
            return sess

        client = AsyncNeonHTTPClient(
            "postgresql://user:pass@host.neon.tech/db",
            http_client=factory,
        )
        await client._ensure_client()
        await client.close()
        assert created and created[0].closed is True


@pytest.mark.asyncio
async def test_fetch_function_injection_is_used():
    calls = []

    async def fake_fetch(url: str, body: str, headers: dict[str, str]):
        calls.append((url, body, headers))
        return 200, '{"rows":[{"v":"1"}],"fields":[{"name":"v","dataTypeID":23}],"rowCount":1,"command":"SELECT"}'

    client = AsyncNeonHTTPClient(
        "postgresql://user:pass@host.neon.tech/db",
        fetch_function=fake_fetch,
    )
    result = await client.query("select 1 as v")
    assert result.row_count == 1
    assert result.command == "SELECT"
    assert result.rows[0]["v"] == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_websocket_pool_reuses_and_limits_clients(monkeypatch):
    import sqlalchemy_neon.neon_http_client as neon_http_client_module

    class FakeWSClient:
        created = 0
        active = 0
        max_active = 0

        def __init__(self, *args, **kwargs):
            FakeWSClient.created += 1

        async def query(self, sql, params=None, options=None):
            FakeWSClient.active += 1
            FakeWSClient.max_active = max(FakeWSClient.max_active, FakeWSClient.active)
            await asyncio.sleep(0.02)
            FakeWSClient.active -= 1
            return QueryResult(
                rows=[{"v": 1}],
                fields=[{"name": "v"}],
                row_count=1,
                command="SELECT",
            )

        async def transaction(self, queries, options=None):
            return []

        async def close(self):
            return None

        async def force_close(self):
            return None

    monkeypatch.setattr(
        neon_http_client_module,
        "AsyncNeonWebSocketClient",
        FakeWSClient,
    )

    pool = AsyncNeonWebSocketPool(
        "postgresql://user:pass@host.neon.tech/db",
        max_size=2,
    )
    await asyncio.gather(
        pool.query("select 1"),
        pool.query("select 1"),
        pool.query("select 1"),
    )
    await pool.close()

    assert FakeWSClient.created <= 2
    assert FakeWSClient.max_active <= 2


def test_websocket_pool_validates_size():
    with pytest.raises(NeonConfigurationError, match="max_size"):
        AsyncNeonWebSocketPool(
            "postgresql://user:pass@host.neon.tech/db",
            max_size=0,
        )
