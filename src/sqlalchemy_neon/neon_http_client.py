"""
Async HTTP client for Neon serverless PostgreSQL API.

Uses aiohttp for fully async HTTP.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Awaitable

import aiohttp

from .errors import (
    NeonAuthenticationError,
    NeonConfigurationError,
    NeonConnectionError,
    NeonHTTPError,
    NeonQueryError,
    NeonTransactionError,
)
from .types import TypeConverter

if TYPE_CHECKING:
    from urllib.parse import urlparse


class IsolationLevel(Enum):
    """PostgreSQL transaction isolation levels."""

    READ_UNCOMMITTED = "ReadUncommitted"
    READ_COMMITTED = "ReadCommitted"
    REPEATABLE_READ = "RepeatableRead"
    SERIALIZABLE = "Serializable"


@dataclass
class QueryResult:
    """Result of a single query execution."""

    rows: list[dict[str, Any] | list[Any]]
    fields: list[dict[str, Any]]
    row_count: int
    command: str
    row_as_array: bool = False

    @property
    def columns(self) -> list[str]:
        """Get column names from fields."""
        return [f.get("name", "") for f in self.fields]


@dataclass
class QueryOptions:
    """Options for query execution."""

    array_mode: bool = False
    full_results: bool = True
    auth_token: str | None = None
    timeout: float | None = None


@dataclass
class TransactionOptions:
    """Options for transaction execution."""

    isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    read_only: bool = False
    deferrable: bool = False
    array_mode: bool = False
    full_results: bool = True
    auth_token: str | None = None
    timeout: float | None = None

    def __post_init__(self) -> None:
        """Validate transaction options."""
        if self.deferrable:
            if self.isolation_level != IsolationLevel.SERIALIZABLE:
                raise NeonConfigurationError(
                    "deferrable=True requires isolation_level=SERIALIZABLE"
                )
            if not self.read_only:
                raise NeonConfigurationError("deferrable=True requires read_only=True")


class AsyncNeonHTTPClient:
    """Async HTTP client for Neon serverless PostgreSQL API.

    This client communicates with Neon's HTTP SQL endpoint to execute
    queries without maintaining a persistent TCP connection.

    Uses aiohttp for fully async HTTP.

    Example:
        async with AsyncNeonHTTPClient("postgresql://user:pass@host.neon.tech/db") as client:
            result = await client.query("SELECT * FROM users WHERE id = $1", [1])
            print(result.rows)
    """

    def __init__(
        self,
        connection_string: str,
        *,
        http_client: aiohttp.ClientSession
        | Callable[[], Awaitable[aiohttp.ClientSession]]
        | None = None,
        auth_token: str | None = None,
        timeout: float | None = 30.0,
    ) -> None:
        """Initialize the async Neon HTTP client.

        Args:
            connection_string: PostgreSQL connection URI.
            http_client: Optional external aiohttp.ClientSession (or async factory).
                         If not provided, a new ClientSession is created.
            auth_token: Optional JWT token for Row-Level Security.
            timeout: Default request timeout in seconds.
        """
        self._connection_string = connection_string
        self._auth_token = auth_token
        self._timeout = timeout
        self._url = self._parse_endpoint(connection_string)
        self._external_client: bool = http_client is not None
        self._http_client: aiohttp.ClientSession | Callable[
            [], Awaitable[aiohttp.ClientSession]
        ] | None = http_client
        self._type_converter = TypeConverter()


    def _parse_endpoint(self, connection_string: str) -> str:
        """Parse the HTTP endpoint URL from connection string.

        Args:
            connection_string: PostgreSQL connection URI.

        Returns:
            HTTPS endpoint URL for Neon SQL API.

        Raises:
            NeonConfigurationError: If connection string is invalid.
        """
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

        return f"https://{parsed.hostname}/sql"

    def _build_headers(
        self,
        array_mode: bool = False,
        auth_token: str | None = None,
        transaction_options: TransactionOptions | None = None,
    ) -> dict[str, str]:
        """Build HTTP headers for a request.

        Args:
            array_mode: Whether to request array format responses.
            auth_token: Optional JWT token override.
            transaction_options: Options for transaction batching.

        Returns:
            Dictionary of HTTP headers.
        """
        headers = {
            "Content-Type": "application/json",
            "Neon-Connection-String": self._connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": "true" if array_mode else "false",
        }

        # Use provided token or fall back to instance token
        token = auth_token or self._auth_token
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # Add transaction headers if applicable
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
        """Ensure an async HTTP client exists.

        Returns:
            Active aiohttp.ClientSession instance.
        """
        if self._external_client is True and callable(self._http_client):
            self._http_client = await self._http_client()

        if self._http_client is None:
            self._http_client = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout)
                if self._timeout
                else None,
            )
        assert isinstance(self._http_client, aiohttp.ClientSession)
        return self._http_client

    async def _request(
        self,
        body: dict[str, Any],
        headers: dict[str, str],
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Make an async HTTP request to the Neon API.

        Args:
            body: JSON request body.
            headers: HTTP headers.
            timeout: Optional request timeout override.

        Returns:
            Parsed JSON response.

        Raises:
            NeonHTTPError: For HTTP-level errors.
            NeonAuthenticationError: For authentication failures.
            NeonConnectionError: For connection failures.
        """
        client = await self._ensure_client()
        json_body = json.dumps(body)

        request_timeout = timeout or self._timeout
        try:
            # response = await client.post(
            #     self._url,
            #     content=json_body,
            #     headers=headers,
            #     timeout=request_timeout,
            # )
            # response_text = response.text
            async with client.post(
                self._url,
                data=json_body,
                headers=headers,
                timeout=request_timeout,
            ) as response:
                response_text = await response.text()

            if response.status == 401:
                raise NeonAuthenticationError(
                    "Authentication failed. Check your connection string credentials."
                )

            if response.status == 403:
                raise NeonAuthenticationError(
                    "Authorization failed. Check your auth token or permissions."
                )

            if response.status != 200:
                raise NeonHTTPError(
                    f"Request failed: {response_text}",
                    status_code=response.status,
                    response_body=response_text,
                )

            try:
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                raise NeonHTTPError(
                    f"Invalid JSON response: {e}",
                    status_code=response.status,
                    response_body=response_text,
                ) from e
        except aiohttp.ClientConnectionError as e:
            raise NeonConnectionError(f"Connection error: {e}") from e
        except aiohttp.ConnectionTimeoutError as e:
            raise NeonConnectionError(f"Request timeout: {e}") from e
        except aiohttp.ClientError as e:
            raise NeonConnectionError(f"HTTP error: {e}") from e
        # except httpx.ConnectError as e:
        #     raise NeonConnectionError(f"Failed to connect to Neon: {e}") from e
        # except httpx.TimeoutException as e:
        #     raise NeonConnectionError(f"Request timeout: {e}") from e
        # except httpx.HTTPError as e:
        #     raise NeonConnectionError(f"HTTP error: {e}") from e

    def _parse_query_response(
        self,
        response: dict[str, Any],
        array_mode: bool = False,
    ) -> QueryResult:
        """Parse a single query response.

        Args:
            response: JSON response from Neon API.
            array_mode: Whether response is in array mode.

        Returns:
            Parsed QueryResult.

        Raises:
            NeonQueryError: If response indicates a query error.
        """
        # Check for error response
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

        return QueryResult(
            rows=response.get("rows", []),
            fields=response.get("fields", []),
            row_count=response.get("rowCount", 0),
            command=response.get("command", ""),
            row_as_array=response.get("rowAsArray", array_mode),
        )

    async def query(
        self,
        sql: str,
        params: list[Any] | tuple[Any, ...] | None = None,
        options: QueryOptions | None = None,
    ) -> QueryResult:
        """Execute a single SQL query.

        Args:
            sql: SQL query string with $1, $2, ... placeholders.
            params: Query parameters.
            options: Query execution options.

        Returns:
            QueryResult with rows and metadata.

        Example:
            result = await client.query(
                "SELECT * FROM users WHERE id = $1",
                [42]
            )
        """
        options = options or QueryOptions()

        # Convert Python params to PostgreSQL text format
        pg_params = self._type_converter.convert_params(params)

        body = {
            "query": sql,
            "params": pg_params,
        }

        headers = self._build_headers(
            array_mode=options.array_mode,
            auth_token=options.auth_token,
        )

        response = await self._request(body, headers, timeout=options.timeout)
        result = self._parse_query_response(response, array_mode=options.array_mode)

        # Convert row values from text to Python types
        if result.rows and result.fields:
            result.rows = [
                self._type_converter.convert_row(row, result.fields, options.array_mode)
                for row in result.rows
            ]

        return result

    async def transaction(
        self,
        queries: list[tuple[str, list[Any] | tuple[Any, ...] | None]],
        options: TransactionOptions | None = None,
    ) -> list[QueryResult]:
        """Execute multiple queries in a single transaction.

        All queries are sent in a single HTTP request and executed atomically.
        If any query fails, the entire transaction is rolled back.

        Args:
            queries: List of (sql, params) tuples.
            options: Transaction execution options.

        Returns:
            List of QueryResult, one per query.

        Example:
            results = await client.transaction([
                ("INSERT INTO users (name) VALUES ($1)", ["Alice"]),
                ("SELECT * FROM users", None),
            ])
        """
        options = options or TransactionOptions()

        # Convert all query parameters
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

        response = await self._request(body, headers, timeout=options.timeout)

        # Transaction response contains "results" array
        if "results" in response:
            results_data = response["results"]
        elif isinstance(response, list):
            results_data = response
        else:
            raise NeonTransactionError(
                f"Unexpected transaction response format: {response}"
            )

        results = []
        for result_data in results_data:
            result = self._parse_query_response(
                result_data, array_mode=options.array_mode
            )

            # Convert row values from text to Python types
            if result.rows and result.fields:
                result.rows = [
                    self._type_converter.convert_row(
                        row, result.fields, options.array_mode
                    )
                    for row in result.rows
                ]

            results.append(result)

        return results

    async def close(self) -> None:
        """Close the HTTP client (if owned by this instance)."""
        if self._http_client:
            await self._http_client.close()
            self._http_client = None

    async def __aenter__(self) -> "AsyncNeonHTTPClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    def __del__(self) -> None:
        """Destructor."""
        if not hasattr(self, "_http_client"):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            loop.create_task(self.close())
        else:
            loop.run_until_complete(self.close())


# Keep legacy exports for compatibility during transition
# These will be removed in favor of AsyncNeonHTTPClient
NeonHTTPClient = AsyncNeonHTTPClient
