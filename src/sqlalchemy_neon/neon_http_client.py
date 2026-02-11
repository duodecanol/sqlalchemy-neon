"""
Async HTTP and WebSocket clients for Neon serverless PostgreSQL API.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Sequence

import aiohttp

from .errors import (
    NeonAuthenticationError,
    NeonConfigurationError,
    NeonConnectionError,
    NeonHTTPError,
    NeonQueryError,
    NeonTransactionError,
)
from .pg_protocol import PGProtocol, PGQueryResult
from .types import TypeConverter

if TYPE_CHECKING:
    from urllib.parse import ParseResult


_FIRST_WORD_REGEX = re.compile(r"^[^.]+\.")


class IsolationLevel(Enum):
    """PostgreSQL transaction isolation levels."""

    READ_UNCOMMITTED = "ReadUncommitted"
    READ_COMMITTED = "ReadCommitted"
    REPEATABLE_READ = "RepeatableRead"
    SERIALIZABLE = "Serializable"


@dataclass
class QueryResult:
    """Result of a single query execution."""

    rows: Sequence[dict[str, Any] | Sequence[Any]]
    fields: Sequence[dict[str, Any]]
    row_count: int
    command: str
    row_as_array: bool = False

    @property
    def columns(self) -> Sequence[str]:
        return [f.get("name", "") for f in self.fields]


@dataclass
class QueryOptions:
    """Options for query execution."""

    array_mode: bool = False
    full_results: bool = True
    auth_token: str | None = None


@dataclass
class TransactionOptions:
    """Options for transaction execution."""

    isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    read_only: bool = False
    deferrable: bool = False
    array_mode: bool = False
    full_results: bool = True
    auth_token: str | None = None

    def __post_init__(self) -> None:
        if self.deferrable:
            if self.isolation_level != IsolationLevel.SERIALIZABLE:
                raise NeonConfigurationError(
                    "deferrable=True requires isolation_level=SERIALIZABLE"
                )
            if not self.read_only:
                raise NeonConfigurationError("deferrable=True requires read_only=True")


class AsyncNeonHTTPClient:
    """Async HTTP client for Neon serverless PostgreSQL API."""

    def __init__(
        self,
        connection_string: str,
        *,
        http_client: aiohttp.ClientSession
        | Callable[[], Awaitable[aiohttp.ClientSession]]
        | None = None,
        auth_token: str | None = None,
        timeout: float | aiohttp.ClientTimeout | None = None,
        fetch_endpoint: str | Callable[[str, int, bool], str] | None = None,
        fetch_function: (
            Callable[[str, str, dict[str, str]], Awaitable[tuple[int, str]]] | None
        ) = None,
    ) -> None:
        self._connection_string = connection_string
        self._auth_token = auth_token
        self._timeout = timeout
        self._parsed_connection = self._parse_connection_string(connection_string)
        self._fetch_endpoint = fetch_endpoint
        self._fetch_function = fetch_function
        self._url = self._resolve_fetch_url(jwt_auth=bool(auth_token))
        self._external_client: bool = (
            http_client is not None
            and not callable(http_client)
            and not inspect.isawaitable(http_client)
        )
        self._http_client: (
            aiohttp.ClientSession
            | Callable[[], Awaitable[aiohttp.ClientSession]]
            | None
        ) = http_client
        self._type_converter = TypeConverter()

    def _parse_connection_string(self, connection_string: str) -> "ParseResult":
        from urllib.parse import urlparse

        parsed = urlparse(connection_string)
        if parsed.scheme not in ("postgresql", "postgres"):
            raise NeonConfigurationError(
                f"Invalid scheme '{parsed.scheme}'. Expected 'postgresql' or 'postgres'."
            )
        if not parsed.hostname:
            raise NeonConfigurationError("Connection string must include a hostname.")
        if not parsed.username:
            raise NeonConfigurationError("Connection string must include a username.")
        if not parsed.path or parsed.path == "/":
            raise NeonConfigurationError(
                "Connection string must include a database name."
            )
        return parsed

    def _default_fetch_endpoint(self, host: str, *, jwt_auth: bool = False) -> str:
        new_prefix = "apiauth." if jwt_auth else "api."
        rewritten_host = _FIRST_WORD_REGEX.sub(new_prefix, host)
        return f"https://{rewritten_host}/sql"

    def _resolve_fetch_url(self, *, jwt_auth: bool) -> str:
        host = self._parsed_connection.hostname
        if host is None:
            raise NeonConfigurationError("Connection string must include a hostname.")
        port = self._parsed_connection.port or 5432

        endpoint = self._fetch_endpoint
        if callable(endpoint):
            return endpoint(host, port, jwt_auth)
        if isinstance(endpoint, str):
            return endpoint
        return self._default_fetch_endpoint(host, jwt_auth=jwt_auth)

    def _build_headers(
        self,
        array_mode: bool = False,
        auth_token: str | None = None,
        transaction_options: TransactionOptions | None = None,
    ) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Neon-Connection-String": self._connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": "true" if array_mode else "false",
        }

        token = auth_token or self._auth_token
        if token:
            headers["Authorization"] = f"Bearer {token}"

        if transaction_options:
            headers["Neon-Batch-Isolation-Level"] = (
                transaction_options.isolation_level.value
            )
            headers["Neon-Batch-Read-Only"] = str(transaction_options.read_only).lower()
            headers["Neon-Batch-Deferrable"] = str(
                transaction_options.deferrable
            ).lower()

        return headers

    async def _ensure_client(self) -> aiohttp.ClientSession:
        if inspect.isawaitable(self._http_client):
            self._http_client = await self._http_client

        if callable(self._http_client):
            maybe_client = self._http_client()
            if inspect.isawaitable(maybe_client):
                maybe_client = await maybe_client
            self._http_client = maybe_client

        if self._http_client is None:
            if isinstance(self._timeout, aiohttp.ClientTimeout):
                timeout = self._timeout
            elif isinstance(self._timeout, (int, float)):
                timeout = aiohttp.ClientTimeout(total=self._timeout)
            else:
                timeout = None
            self._http_client = aiohttp.ClientSession(timeout=timeout)

        if not isinstance(self._http_client, aiohttp.ClientSession):
            raise NeonConfigurationError(
                "http_client must be an aiohttp.ClientSession or a factory returning one."
            )

        if self._http_client.closed:
            self._http_client = aiohttp.ClientSession()

        return self._http_client

    async def _request(
        self, body: dict[str, Any], headers: dict[str, str]
    ) -> dict[str, Any]:
        json_body = json.dumps(body)
        url = self._resolve_fetch_url(jwt_auth=("Authorization" in headers))

        try:
            if self._fetch_function is not None:
                status_code, response_text = await self._fetch_function(
                    url,
                    json_body,
                    headers,
                )
            else:
                client = await self._ensure_client()
                async with client.post(
                    url, data=json_body, headers=headers
                ) as response:
                    status_code = response.status
                    response_text = await response.text()

            if status_code == 401:
                raise NeonAuthenticationError(
                    "Authentication failed. Check your connection string credentials."
                )
            if status_code == 403:
                raise NeonAuthenticationError(
                    "Authorization failed. Check your auth token or permissions."
                )
            if status_code != 200:
                raise NeonHTTPError(
                    f"Request failed: {response_text}",
                    status_code=status_code,
                    response_body=response_text,
                )

            try:
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                raise NeonHTTPError(
                    f"Invalid JSON response: {e}",
                    status_code=status_code,
                    response_body=response_text,
                ) from e

        except aiohttp.ConnectionTimeoutError as e:
            raise NeonConnectionError(f"Request timeout: {e}") from e
        except aiohttp.ClientConnectionError as e:
            raise NeonConnectionError(f"Connection error: {e}") from e
        except aiohttp.ClientError as e:
            raise NeonConnectionError(f"HTTP error: {e}") from e

    def _parse_query_response(
        self,
        response: dict[str, Any],
        array_mode: bool = False,
    ) -> QueryResult:
        if "error" in response or "message" in response:
            error_msg = response.get("message") or response.get(
                "error", "Unknown error"
            )
            raise NeonQueryError(
                message=error_msg,
                code=response.get("code"),
                detail=response.get("detail"),
                hint=response.get("hint"),
            )

        rows = response.get("rows", [])
        fields = response.get("fields", [])
        is_array = response.get("rowAsArray", array_mode)

        if rows and fields:
            rows = [
                self._type_converter.convert_row(row, fields, is_array) for row in rows
            ]

        return QueryResult(
            rows=rows,
            fields=fields,
            row_count=response.get("rowCount", 0),
            command=response.get("command", ""),
            row_as_array=is_array,
        )

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | tuple[Any, ...] | None = None,
        options: QueryOptions | None = None,
    ) -> QueryResult:
        options = options or QueryOptions()
        pg_params = self._type_converter.convert_params(params)
        body = {"query": sql, "params": pg_params}
        headers = self._build_headers(
            array_mode=options.array_mode,
            auth_token=options.auth_token,
        )

        response = await self._request(body, headers)
        return self._parse_query_response(response, array_mode=options.array_mode)

    async def transaction(
        self,
        queries: Sequence[tuple[str, Sequence[Any] | tuple[Any, ...] | None]],
        options: TransactionOptions | None = None,
    ) -> Sequence[QueryResult]:
        options = options or TransactionOptions()

        processed_queries = [
            {
                "query": sql,
                "params": self._type_converter.convert_params(params),
            }
            for sql, params in queries
        ]
        body = {"queries": processed_queries}

        headers = self._build_headers(
            array_mode=options.array_mode,
            auth_token=options.auth_token,
            transaction_options=options,
        )

        response = await self._request(body, headers)

        results_data: list[dict[str, Any]]
        if "results" in response:
            results_data = response["results"]
        elif isinstance(response, list):
            results_data = response
        else:
            raise NeonTransactionError(
                f"Unexpected transaction response format: {response}"
            )

        results: list[QueryResult] = [
            self._parse_query_response(result_data, array_mode=options.array_mode)
            for result_data in results_data
        ]

        return results

    async def close(self) -> None:
        if self._http_client is not None and not self._external_client:
            await self.force_close()

    async def force_close(self) -> None:
        if self._http_client is None:
            return
        if callable(self._http_client):
            return
        await self._http_client.close()
        self._http_client = None

    async def __aenter__(self) -> "AsyncNeonHTTPClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


NeonHTTPClient = AsyncNeonHTTPClient


class AsyncNeonWebSocketClient(AsyncNeonHTTPClient):
    """Neon client that runs PostgreSQL wire protocol directly over WebSocket."""

    def __init__(
        self,
        connection_string: str,
        *,
        http_client: aiohttp.ClientSession
        | Callable[[], Awaitable[aiohttp.ClientSession]]
        | None = None,
        auth_token: str | None = None,
        timeout: float | aiohttp.ClientTimeout | None = None,
        ws_proxy: str | Callable[[str, int], str] | None = None,
        use_secure_websocket: bool = True,
        heartbeat: float | None = 30.0,
    ) -> None:
        super().__init__(
            connection_string,
            http_client=http_client,
            auth_token=auth_token,
            timeout=timeout,
        )

        host = self._parsed_connection.hostname
        if host is None:
            raise NeonConfigurationError("Connection string must include a hostname.")
        port = self._parsed_connection.port or 5432

        self._host = host
        self._port = port
        self._ws_proxy = ws_proxy
        self._use_secure_websocket = use_secure_websocket
        self._heartbeat = heartbeat
        self._ws_url = self._build_ws_url()

        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._protocol: PGProtocol | None = None
        self._request_lock = asyncio.Lock()

    def _build_ws_url(self) -> str:
        proxy = self._ws_proxy
        if callable(proxy):
            addr = proxy(self._host, self._port)
        elif isinstance(proxy, str):
            addr = f"{proxy}?address={self._host}:{self._port}"
        else:
            addr = f"{self._host}/v2"
        if "://" in addr:
            return addr
        protocol = "wss" if self._use_secure_websocket else "ws"
        return f"{protocol}://{addr}"

    async def _ensure_connection(self) -> PGProtocol:
        if self._protocol is not None and self._ws is not None and not self._ws.closed:
            return self._protocol

        await self._close_connection()

        try:
            client = await self._ensure_client()
            self._ws = await client.ws_connect(
                self._ws_url,
                heartbeat=self._heartbeat,
                max_msg_size=0,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise NeonConnectionError(f"WebSocket connect error: {e}") from e

        ws = self._ws

        async def send_fn(data: bytes) -> None:
            await ws.send_bytes(data)

        async def recv_fn() -> bytes:
            msg = await ws.receive()
            if msg.type is aiohttp.WSMsgType.BINARY:
                return msg.data
            if msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
            ):
                raise NeonConnectionError("WebSocket closed during receive")
            if msg.type is aiohttp.WSMsgType.ERROR:
                raise NeonConnectionError("WebSocket error during receive")
            raise NeonConnectionError(f"Unexpected WebSocket message type: {msg.type}")

        self._protocol = PGProtocol(send_fn, recv_fn)

        user = self._parsed_connection.username or ""
        password = self._parsed_connection.password or ""
        database = (self._parsed_connection.path or "/neondb").lstrip("/")

        try:
            await self._protocol.startup(user, password, database)
        except (NeonAuthenticationError, NeonConnectionError):
            await self._close_connection()
            raise

        return self._protocol

    async def _close_connection(self) -> None:
        if self._protocol is not None:
            try:
                await self._protocol.terminate()
            except Exception:
                pass
            self._protocol = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _pg_result_to_query_result(
        self,
        pg_result: PGQueryResult,
        *,
        array_mode: bool,
    ) -> QueryResult:
        """Convert PGProtocol result to the shared QueryResult format."""
        fields = [
            {
                "name": f.name,
                "dataTypeID": f.type_oid,
                "dataTypeSize": f.type_size,
            }
            for f in pg_result.fields
        ]

        command = (
            pg_result.command_tag.split(" ", 1)[0] if pg_result.command_tag else ""
        )
        tag_parts = pg_result.command_tag.split(" ")
        row_count = len(pg_result.rows)
        if len(tag_parts) >= 2:
            try:
                row_count = int(tag_parts[-1])
            except ValueError:
                pass

        if array_mode:
            rows: list[Any] = [
                [cell.decode() if cell is not None else None for cell in raw_row]
                for raw_row in pg_result.rows
            ]
        else:
            keys = [f["name"] for f in fields]
            rows = [
                {
                    keys[i]: cell.decode() if cell is not None else None
                    for i, cell in enumerate(raw_row)
                }
                for raw_row in pg_result.rows
            ]

        result = QueryResult(
            rows=rows,
            fields=fields,
            row_count=row_count,
            command=command,
            row_as_array=array_mode,
        )

        if result.rows and result.fields:
            result.rows = [
                self._type_converter.convert_row(row, result.fields, array_mode)
                for row in result.rows
            ]
        return result

    def _begin_clause(self, options: TransactionOptions) -> str:
        iso_map = {
            IsolationLevel.READ_UNCOMMITTED: "READ UNCOMMITTED",
            IsolationLevel.READ_COMMITTED: "READ COMMITTED",
            IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
            IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
        }
        clause = [f"ISOLATION LEVEL {iso_map[options.isolation_level]}"]
        clause.append("READ ONLY" if options.read_only else "READ WRITE")
        if options.deferrable:
            clause.append("DEFERRABLE")
        return "BEGIN " + " ".join(clause)

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | tuple[Any, ...] | None = None,
        options: QueryOptions | None = None,
    ) -> QueryResult:
        options = options or QueryOptions()
        async with self._request_lock:
            protocol = await self._ensure_connection()
            text_params = self._type_converter.convert_params(params)
            byte_params: Sequence[bytes | None] = [
                p.encode() if p is not None else None for p in text_params
            ]
            try:
                pg_result = await protocol.extended_query(sql, byte_params)
                return self._pg_result_to_query_result(
                    pg_result, array_mode=options.array_mode
                )
            except NeonConnectionError:
                await self._close_connection()
                raise

    async def transaction(
        self,
        queries: Sequence[tuple[str, Sequence[Any] | tuple[Any, ...] | None]],
        options: TransactionOptions | None = None,
    ) -> Sequence[QueryResult]:
        options = options or TransactionOptions()

        async with self._request_lock:
            protocol = await self._ensure_connection()
            results: Sequence[QueryResult] = []
            try:
                await protocol.simple_query(self._begin_clause(options))
                for sql, params in queries:
                    text_params = self._type_converter.convert_params(params)
                    byte_params: Sequence[bytes | None] = [
                        p.encode() if p is not None else None for p in text_params
                    ]
                    pg_result = await protocol.extended_query(sql, byte_params)
                    results.append(
                        self._pg_result_to_query_result(
                            pg_result, array_mode=options.array_mode
                        )
                    )
                await protocol.simple_query("COMMIT")
                return results
            except NeonConnectionError:
                try:
                    await protocol.simple_query("ROLLBACK")
                except Exception:
                    pass
                await self._close_connection()
                raise
            except NeonQueryError as e:
                try:
                    await protocol.simple_query("ROLLBACK")
                except Exception:
                    pass
                raise NeonTransactionError(str(e)) from e

    async def close(self) -> None:
        await self._close_connection()
        await super().close()

    async def force_close(self) -> None:
        await self._close_connection()
        await super().force_close()


class AsyncNeonWebSocketPool:
    """Bounded pool of AsyncNeonWebSocketClient instances."""

    def __init__(
        self,
        connection_string: str,
        *,
        max_size: int = 10,
        http_client: aiohttp.ClientSession
        | Callable[[], Awaitable[aiohttp.ClientSession]]
        | None = None,
        auth_token: str | None = None,
        timeout: float | aiohttp.ClientTimeout | None = None,
        ws_proxy: str | Callable[[str, int], str] | None = None,
        use_secure_websocket: bool = True,
        heartbeat: float | None = 30.0,
    ) -> None:
        if max_size < 1:
            raise NeonConfigurationError("WebSocket pool max_size must be >= 1.")

        self._connection_string = connection_string
        self._max_size = max_size
        self._http_client = http_client
        self._auth_token = auth_token
        self._timeout = timeout
        self._ws_proxy = ws_proxy
        self._use_secure_websocket = use_secure_websocket
        self._heartbeat = heartbeat

        self._create_lock = asyncio.Lock()
        self._available: asyncio.LifoQueue[AsyncNeonWebSocketClient] = (
            asyncio.LifoQueue()
        )
        self._clients: set[AsyncNeonWebSocketClient] = set()
        self._leased: set[AsyncNeonWebSocketClient] = set()
        self._closed = False

    async def _new_client(self) -> AsyncNeonWebSocketClient:
        client = AsyncNeonWebSocketClient(
            self._connection_string,
            http_client=self._http_client,
            auth_token=self._auth_token,
            timeout=self._timeout,
            ws_proxy=self._ws_proxy,
            use_secure_websocket=self._use_secure_websocket,
            heartbeat=self._heartbeat,
        )
        self._clients.add(client)
        return client

    async def acquire(self) -> AsyncNeonWebSocketClient:
        if self._closed:
            raise NeonConnectionError("WebSocket pool is closed.")

        while True:
            try:
                client = self._available.get_nowait()
                if client in self._clients:
                    self._leased.add(client)
                    return client
            except asyncio.QueueEmpty:
                pass

            async with self._create_lock:
                if len(self._clients) < self._max_size:
                    client = await self._new_client()
                    self._leased.add(client)
                    return client

            client = await self._available.get()
            if client in self._clients:
                self._leased.add(client)
                return client

    async def release(self, client: AsyncNeonWebSocketClient) -> None:
        if client not in self._leased:
            return

        self._leased.remove(client)
        if self._closed:
            self._clients.discard(client)
            await client.force_close()
            return

        await self._available.put(client)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncNeonWebSocketClient]:
        client = await self.acquire()
        try:
            yield client
        finally:
            await self.release(client)

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | tuple[Any, ...] | None = None,
        options: QueryOptions | None = None,
    ) -> QueryResult:
        async with self.connection() as client:
            return await client.query(sql, params=params, options=options)

    async def transaction(
        self,
        queries: Sequence[tuple[str, Sequence[Any] | tuple[Any, ...] | None]],
        options: TransactionOptions | None = None,
    ) -> Sequence[QueryResult]:
        async with self.connection() as client:
            return await client.transaction(queries=queries, options=options)

    async def _close_clients(self, *, force: bool) -> None:
        clients = list(self._clients)
        self._clients.clear()
        self._leased.clear()
        while not self._available.empty():
            try:
                self._available.get_nowait()
            except asyncio.QueueEmpty:
                break

        closer = "force_close" if force else "close"
        await asyncio.gather(
            *(getattr(client, closer)() for client in clients),
            return_exceptions=True,
        )

    async def close(self) -> None:
        self._closed = True
        await self._close_clients(force=False)

    async def force_close(self) -> None:
        self._closed = True
        await self._close_clients(force=True)

    async def __aenter__(self) -> "AsyncNeonWebSocketPool":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


NeonWebSocketClient = AsyncNeonWebSocketClient
