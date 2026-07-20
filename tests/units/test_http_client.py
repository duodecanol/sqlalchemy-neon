"""Tests for Neon HTTP client."""

from __future__ import annotations

import asyncio
import sys
import aiohttp
import pytest

from sqlalchemy_neon.neon_http_client import (
    AsyncNeonHTTPClient,
    AsyncNeonWebSocketClient,
    AsyncNeonWebSocketPool,
    NeonWebSocketClient,
    NeonHTTPClient,
    IsolationLevel,
    QueryOptions,
    QueryResult,
    TransactionOptions,
)
from sqlalchemy_neon.errors import (
    NeonAuthenticationError,
    NeonConfigurationError,
    NeonQueryError,
    NeonTypeError,
)
from sqlalchemy_neon.pg_protocol import (
    PGQueryResult,
    _PipelineAuthenticationRequired,
)


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

    @pytest.mark.parametrize(
        "parameter",
        [
            "auth_token",
            "AUTH_TOKEN",
            "timeout",
            "transport",
            "websocket_pool_size",
            "fetch_endpoint",
            "fetch_function",
        ],
    )
    def test_ignores_driver_url_parameters_without_exposing_values(
        self, parameter: str
    ):
        with pytest.warns(UserWarning, match=parameter) as warnings:
            client = AsyncNeonHTTPClient(
                "postgresql://user:pass@host.neon.tech/db"
                f"?{parameter}=sensitive-value"
            )

        assert len(warnings) == 1
        assert "sensitive-value" not in str(warnings[0].message)
        assert parameter not in client._build_headers()["Neon-Connection-String"]

    def test_ignores_blank_driver_url_parameter(self):
        with pytest.warns(UserWarning, match="auth_token"):
            client = AsyncNeonHTTPClient(
                "postgresql://user:pass@host.neon.tech/db?auth_token="
            )

        assert "auth_token" not in client._build_headers()["Neon-Connection-String"]

    def test_ignores_repeated_driver_url_parameter_without_exposing_values(self):
        with pytest.warns(UserWarning, match="auth_token") as warnings:
            client = AsyncNeonHTTPClient(
                "postgresql://user:pass@host.neon.tech/db"
                "?sslmode=require&auth_token=&auth_token=sensitive-value"
            )

        assert len(warnings) == 1
        assert "sensitive-value" not in str(warnings[0].message)
        assert client._build_headers()["Neon-Connection-String"].endswith(
            "?sslmode=require"
        )

    def test_preserves_postgresql_url_parameters(self):
        connection_string = (
            "postgresql://user:pass@host.neon.tech/db"
            "?sslmode=require&application_name=sqlalchemy-neon"
        )
        client = AsyncNeonHTTPClient(connection_string)

        assert client._build_headers()["Neon-Connection-String"] == connection_string

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

    def test_rejects_auth_token(self):
        with pytest.raises(NeonConfigurationError, match="auth_token"):
            AsyncNeonWebSocketClient(
                "postgresql://user:pass@host.neon.tech/db",
                auth_token="jwt-token",
            )

    def test_ignores_auth_token_url_parameter(self):
        with pytest.warns(UserWarning, match="auth_token"):
            client = AsyncNeonWebSocketClient(
                "postgresql://user:pass@host.neon.tech/db?auth_token=jwt-token"
            )

        assert client._connection_string.endswith("/db")


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
async def test_websocket_connection_does_not_require_logfire(monkeypatch):
    class FakeWebSocket:
        def __init__(self):
            self.sent: list[bytes] = []

        async def send_bytes(self, data: bytes) -> None:
            self.sent.append(data)

    class FakeClient:
        async def ws_connect(self, *args, **kwargs):
            return websocket

    async def close_connection() -> None:
        return None

    async def ensure_client() -> FakeClient:
        return fake_client

    websocket = FakeWebSocket()
    fake_client = FakeClient()
    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )
    monkeypatch.setitem(sys.modules, "logfire", None)
    monkeypatch.setattr(client, "_close_connection", close_connection)
    monkeypatch.setattr(client, "_ensure_client", ensure_client)

    protocol = await client._establish_websocket_connection()

    await protocol._send(b"outbound-frame")
    assert websocket.sent == [b"outbound-frame"]


@pytest.mark.asyncio
async def test_websocket_query_falls_back_to_standard_authentication(monkeypatch):
    class PipelineProtocol:
        async def startup_with_query(self, user, password, database, sql, params):
            raise _PipelineAuthenticationRequired(
                "Pipeline requires authentication negotiation for auth type 10."
            )

    class NegotiatingProtocol:
        def __init__(self):
            self.startup_calls: list[tuple[str, str, str]] = []
            self.extended_query_calls: list[tuple[str, list[bytes | None]]] = []

        async def startup(self, user, password, database):
            self.startup_calls.append((user, password, database))

        async def extended_query(self, sql, params):
            self.extended_query_calls.append((sql, params))
            return PGQueryResult(fields=[], rows=[], command_tag="SELECT 0")

    class FakeWebSocket:
        closed = False

        async def close(self):
            self.closed = True

    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )
    pipeline_protocol = PipelineProtocol()
    negotiating_protocol = NegotiatingProtocol()
    protocols = [pipeline_protocol, negotiating_protocol]
    connection_count = 0

    async def establish():
        nonlocal connection_count
        protocol = protocols.pop(0)
        connection_count += 1
        client._protocol = protocol
        client._ws = FakeWebSocket()
        return protocol

    monkeypatch.setattr(client, "_establish_websocket_connection", establish)

    result = await client.query("SELECT 1")

    assert result.command == "SELECT"
    assert connection_count == 2
    assert negotiating_protocol.startup_calls == [("user", "pass", "db")]
    assert negotiating_protocol.extended_query_calls == [("SELECT 1", [])]


@pytest.mark.asyncio
async def test_websocket_query_does_not_retry_sql_errors(monkeypatch):
    class PipelineProtocol:
        async def startup_with_query(self, user, password, database, sql, params):
            raise NeonQueryError("syntax error")

    class FakeWebSocket:
        closed = False

        async def close(self):
            self.closed = True

    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )
    connection_count = 0

    async def establish():
        nonlocal connection_count
        connection_count += 1
        protocol = PipelineProtocol()
        client._protocol = protocol
        client._ws = FakeWebSocket()
        return protocol

    monkeypatch.setattr(client, "_establish_websocket_connection", establish)

    with pytest.raises(NeonQueryError, match="syntax error"):
        await client.query("INVALID SQL")

    assert connection_count == 1



@pytest.mark.asyncio
async def test_insecure_websocket_refuses_cleartext_authentication(monkeypatch):
    import struct

    class FakeMessage:
        type = aiohttp.WSMsgType.BINARY
        data = b"R" + struct.pack("!I", 8) + struct.pack("!I", 3)

    class FakeWebSocket:
        def __init__(self):
            self.closed = False
            self.sent: list[bytes] = []

        async def send_bytes(self, data):
            self.sent.append(data)

        async def receive(self):
            return FakeMessage()

        async def close(self):
            self.closed = True

    class FakeClient:
        async def ws_connect(self, *args, **kwargs):
            return websocket

    websocket = FakeWebSocket()
    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db",
        use_secure_websocket=False,
    )

    async def ensure_client():
        return FakeClient()

    monkeypatch.setattr(client, "_ensure_client", ensure_client)

    with pytest.raises(NeonAuthenticationError, match="secure transport"):
        await client.query("SELECT 1")

    assert websocket.sent
    assert b"pass\x00" not in b"".join(websocket.sent)


@pytest.mark.asyncio
async def test_websocket_query_rejects_auth_token_option():
    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )

    with pytest.raises(NeonConfigurationError, match="auth_token"):
        await client.query("SELECT 1", options=QueryOptions(auth_token="jwt-token"))


@pytest.mark.asyncio
async def test_websocket_transaction_rejects_auth_token_option():
    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )

    with pytest.raises(NeonConfigurationError, match="auth_token"):
        await client.transaction(
            [("SELECT 1", [])],
            options=TransactionOptions(auth_token="jwt-token"),
        )

@pytest.mark.asyncio
async def test_websocket_pool_reuses_and_limits_clients(monkeypatch):
    import sqlalchemy_neon.neon_http_client as neon_http_client_module

    class FakeWSClient:
        created = 0
        active = 0
        max_active = 0

        def __init__(self, *args, **kwargs):
            FakeWSClient.created += 1
            self.is_reusable = True

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


@pytest.mark.asyncio
async def test_websocket_query_cancellation_quarantines_client(monkeypatch):
    class FakeWebSocket:
        closed = False

    class FakeProtocol:
        is_reusable = True

        async def extended_query(self, sql, params):
            raise asyncio.CancelledError

    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )
    client._ws = FakeWebSocket()
    client._protocol = FakeProtocol()
    force_close_calls = 0

    async def force_close():
        nonlocal force_close_calls
        force_close_calls += 1

    monkeypatch.setattr(client, "force_close", force_close)

    with pytest.raises(asyncio.CancelledError):
        await client.query("SELECT 1")

    assert force_close_calls == 1


@pytest.mark.asyncio
async def test_websocket_transaction_conversion_failure_quarantines_client(
    monkeypatch,
):
    class FakeWebSocket:
        closed = False

    class FakeProtocol:
        is_reusable = True

        def __init__(self):
            self.queries: list[str] = []

        async def simple_query(self, sql):
            self.queries.append(sql)
            return []

    client = AsyncNeonWebSocketClient(
        "postgresql://user:pass@host.neon.tech/db"
    )
    protocol = FakeProtocol()
    client._ws = FakeWebSocket()
    client._protocol = protocol
    force_close_calls = 0

    def raise_type_error(params):
        raise NeonTypeError("conversion failed")

    async def force_close():
        nonlocal force_close_calls
        force_close_calls += 1

    monkeypatch.setattr(client._type_converter, "convert_params", raise_type_error)
    monkeypatch.setattr(client, "force_close", force_close)

    with pytest.raises(NeonTypeError, match="conversion failed"):
        await client.transaction([("SELECT $1", [object()])])

    assert protocol.queries[0].startswith("BEGIN ")
    assert protocol.queries[1] == "ROLLBACK"
    assert force_close_calls == 1


@pytest.mark.asyncio
async def test_websocket_pool_discards_unhealthy_client(monkeypatch):
    import sqlalchemy_neon.neon_http_client as neon_http_client_module

    class FakeWSClient:
        created = 0

        def __init__(self, *args, **kwargs):
            FakeWSClient.created += 1
            self.is_reusable = True
            self.force_close_calls = 0

        async def close(self):
            return None

        async def force_close(self):
            self.force_close_calls += 1
            self.is_reusable = False

    monkeypatch.setattr(
        neon_http_client_module,
        "AsyncNeonWebSocketClient",
        FakeWSClient,
    )
    pool = AsyncNeonWebSocketPool(
        "postgresql://user:pass@host.neon.tech/db",
        max_size=1,
    )

    client = await pool.acquire()
    client.is_reusable = False
    await pool.release(client)

    assert client.force_close_calls == 1
    assert client not in pool._clients
    assert pool._available.empty()

    replacement = await pool.acquire()
    assert replacement is not client
    assert FakeWSClient.created == 2
    await pool.close()


@pytest.mark.asyncio
async def test_websocket_pool_discards_cancelled_borrower(monkeypatch):
    import sqlalchemy_neon.neon_http_client as neon_http_client_module

    class FakeWSClient:
        def __init__(self, *args, **kwargs):
            self.is_reusable = True
            self.force_close_calls = 0

        async def close(self):
            return None

        async def force_close(self):
            self.force_close_calls += 1
            self.is_reusable = False

    monkeypatch.setattr(
        neon_http_client_module,
        "AsyncNeonWebSocketClient",
        FakeWSClient,
    )
    pool = AsyncNeonWebSocketPool(
        "postgresql://user:pass@host.neon.tech/db",
        max_size=1,
    )

    with pytest.raises(asyncio.CancelledError):
        async with pool.connection() as client:
            raise asyncio.CancelledError

    assert client.force_close_calls == 1
    assert client not in pool._clients
    assert pool._available.empty()

    replacement = await pool.acquire()
    assert replacement is not client
    await pool.close()


def test_websocket_pool_validates_size():
    with pytest.raises(NeonConfigurationError, match="max_size"):
        AsyncNeonWebSocketPool(
            "postgresql://user:pass@host.neon.tech/db",
            max_size=0,
        )


def test_websocket_pool_rejects_auth_token():
    with pytest.raises(NeonConfigurationError, match="auth_token"):
        AsyncNeonWebSocketPool(
            "postgresql://user:pass@host.neon.tech/db",
            auth_token="jwt-token",
        )
