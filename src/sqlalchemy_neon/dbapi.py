"""
PEP 249 Database API v2.0 compliant module for Neon HTTP.

This module provides a DBAPI-compatible interface for SQLAlchemy.
It uses SQLAlchemy's await_only to ensure execution within greenlet context:
- Works with async engine only
- Raises error if used with sync engine

Note: Each query executes immediately via HTTP. The Neon HTTP API
is stateless, so true transaction semantics are not available through
this driver. Each SQL statement runs in its own implicit transaction.

For atomic multi-statement operations, use the HTTP client's
transaction() method directly.

Usage:
    from sqlalchemy_neon import dbapi

    # Used internally by SQLAlchemy async engines
"""

from __future__ import annotations

import re
from typing import Any, Iterator

from sqlalchemy.util.concurrency import await_only

# from .concurrency import await_only_allow_missing as await_only
from sqlalchemy.engine import AdaptedConnection

from sqlalchemy.connectors.asyncio import AsyncAdapt_terminate

from .errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from .neon_http_client import (
    AsyncNeonHTTPClient,
    IsolationLevel,
    QueryOptions,
    QueryResult,
    TransactionOptions,
)
from .types import TypeConverter, build_cursor_description


# PEP 249 module globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

# Re-export exceptions for DBAPI compliance
__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    # Primary class names (async-adapted)
    "AsyncAdapt_neon_connection",
    "AsyncAdapt_neon_cursor",
    # Aliases for sync compatibility
    "Connection",
    "Cursor",
    # Exceptions
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


def connect(
    dsn: str | None = None,
    *,
    http_client: Any | None = None,
    auth_token: str | None = None,
    timeout: float | None = 30.0,
    **kwargs: Any,
) -> "AsyncAdapt_neon_connection":
    """Create a new async-adapted database connection.

    Args:
        dsn: PostgreSQL connection URI.
        http_client: Optional external httpx.AsyncClient to use.
        auth_token: Optional JWT token for Row-Level Security.
        timeout: Default request timeout in seconds.
        **kwargs: Additional connection options.

    Returns:
        AsyncAdapt_neon_connection object.
    """
    if dsn is None:
        raise InterfaceError("dsn (connection string) is required")

    return AsyncAdapt_neon_connection(
        dsn=dsn,
        http_client=http_client,
        auth_token=auth_token,
        timeout=timeout,
    )


class AsyncAdapt_neon_connection(AdaptedConnection):
    """Async-adapted DBAPI connection for SQLAlchemy.

    This class wraps AsyncNeonHTTPClient and uses SQLAlchemy's
    await_only() to ensure execution within greenlet context.

    This driver is designed for use with create_async_engine() only.

    Note: Each query executes immediately via HTTP. True transaction
    semantics are not available since Neon's HTTP API executes each
    request independently. For atomic multi-statement operations,
    use the HTTP client's transaction() method directly.
    """

    __slots__ = (
        "_dsn",
        "_auth_token",
        "_timeout",
        "_client",
        "_closed",
        "_in_transaction",
        "_autocommit",
        "_isolation_level",
        "_connection",
    )

    # Use SQLAlchemy's await_only to ensure we are in a greenlet context
    # This enforces that the driver is used only with async engines

    await_ = staticmethod(await_only)

    def __init__(
        self,
        dsn: str,
        *,
        http_client: Any | None = None,
        auth_token: str | None = None,
        timeout: float | None = 30.0,
    ) -> None:
        """Initialize the async connection.

        Args:
            dsn: PostgreSQL connection URI.
            http_client: Optional external httpx.AsyncClient.
            auth_token: Optional JWT token.
            timeout: Default request timeout.
        """
        self._dsn = dsn
        self._auth_token = auth_token
        self._timeout = timeout
        self._client = AsyncNeonHTTPClient(
            dsn,
            http_client=http_client,
            auth_token=auth_token,
            timeout=timeout,
        )
        self._closed = False
        self._in_transaction = False
        self._autocommit = True
        self._isolation_level = IsolationLevel.READ_COMMITTED
        self._connection = None

    def _check_closed(self) -> None:
        """Raise if connection is closed."""
        if self._closed:
            raise InterfaceError("Connection is closed")

    @property
    def autocommit(self) -> bool:
        """Get autocommit mode."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit mode."""
        self._autocommit = value

    def cursor(self) -> "AsyncAdapt_neon_cursor":
        """Create a new cursor."""
        self._check_closed()
        return AsyncAdapt_neon_cursor(self)

    def commit(self) -> None:
        """Commit the current transaction.

        Note: With HTTP-based connections, each query executes immediately
        in its own implicit transaction. This method exists for SQLAlchemy
        compatibility but doesn't provide true transaction semantics.
        """
        self._check_closed()
        self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction.

        Note: With HTTP-based connections, each query executes immediately.
        Rollback cannot undo already-executed queries. This method exists
        for SQLAlchemy compatibility.
        """
        self._check_closed()
        self._in_transaction = False

    async def _close_async(self) -> None:
        """Close the connection asynchronously."""
        if not self._closed:
            self._in_transaction = False
            await self._client.close()
            self._closed = True

    def close(self) -> None:
        """Close the connection.

        Uses await_fallback to bridge to async close in sync/async contexts.
        """
        if not self._closed:
            self._in_transaction = False
            try:
                self.await_(self._client.close())
            except Exception:
                pass  # Ignore errors during close
            self._closed = True

    def begin(self) -> None:
        """Begin a new transaction."""
        self._check_closed()
        if not self._autocommit:
            self._in_transaction = True

    def set_isolation_level(self, level: int | str) -> None:
        """Set transaction isolation level."""
        level_map = {
            0: IsolationLevel.READ_UNCOMMITTED,
            1: IsolationLevel.READ_UNCOMMITTED,
            2: IsolationLevel.READ_COMMITTED,
            3: IsolationLevel.REPEATABLE_READ,
            4: IsolationLevel.SERIALIZABLE,
            "READ UNCOMMITTED": IsolationLevel.READ_UNCOMMITTED,
            "READ COMMITTED": IsolationLevel.READ_COMMITTED,
            "REPEATABLE READ": IsolationLevel.REPEATABLE_READ,
            "SERIALIZABLE": IsolationLevel.SERIALIZABLE,
        }

        if isinstance(level, str):
            level = level.upper()

        if level in level_map:
            self._isolation_level = level_map[level]
        else:
            raise ProgrammingError(f"Unknown isolation level: {level}")

    def ping(self) -> bool:
        """Ping the database to verify connectivity."""
        try:
            self.await_(self._client.query("SELECT 1"))
            return True
        except Exception:
            return False

    def terminate(self) -> None:
        """Terminate the connection immediately."""
        self.close()

    def __enter__(self) -> "AsyncAdapt_neon_connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


class AsyncAdapt_neon_cursor:
    """Async-adapted DBAPI cursor for SQLAlchemy.

    Executes queries via the async NeonHTTPClient using await_only()
    for greenlet-based async/sync bridging.

    Supports transaction buffering when connection is in transaction state.
    """

    __slots__ = (
        "_adapt_connection",
        "_connection",
        "_rows",
        "_cursor",
        "description",
        "rowcount",
        "arraysize",
        "_index",
        "_closed",
        "_type_converter",
        "_last_result",
    )

    _PYFORMAT_PATTERN = re.compile(r"%s|%\([^)]+\)s")

    def __init__(self, adapt_connection: AsyncAdapt_neon_connection) -> None:
        """Initialize the cursor.

        Args:
            adapt_connection: Parent async connection.
        """
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._client
        self._rows: list[tuple] = []
        self._cursor = None
        self.description: tuple[tuple, ...] | None = None
        self.rowcount: int = -1
        self.arraysize: int = 1
        self._index: int = 0
        self._closed: bool = False
        self._type_converter = TypeConverter()
        self._last_result: QueryResult | None = None

    def _check_closed(self) -> None:
        """Raise if cursor or connection is closed."""
        if self._closed:
            raise InterfaceError("Cursor is closed")
        if self._adapt_connection._closed:
            raise InterfaceError("Connection is closed")

    def _convert_params(
        self, query: str, params: tuple | dict | None
    ) -> tuple[str, list]:
        """Convert pyformat to numeric placeholders."""
        if params is None:
            return query, []

        if isinstance(params, dict):
            param_names = []
            param_values = []

            def replace_named(match):
                s = match.group(0)
                if s == "%s":
                    raise ProgrammingError("Cannot mix %s and %(name)s styles")
                name = s[2:-2]
                if name not in params:
                    raise ProgrammingError(f"Missing parameter: {name}")

                if name not in param_names:
                    param_names.append(name)
                    param_values.append(params[name])
                    return f"${len(param_values)}"
                else:
                    return f"${param_names.index(name) + 1}"

            converted = self._PYFORMAT_PATTERN.sub(replace_named, query)
            return converted, param_values

        else:
            param_list = list(params) if params else []
            counter = [0]

            def replace_positional(match):
                s = match.group(0)
                if s != "%s":
                    raise ProgrammingError("Cannot mix %s and %(name)s styles")
                counter[0] += 1
                return f"${counter[0]}"

            converted = self._PYFORMAT_PATTERN.sub(replace_positional, query)

            if counter[0] != len(param_list):
                raise ProgrammingError(
                    f"Parameter count mismatch: query has {counter[0]} "
                    f"placeholders but {len(param_list)} parameters provided"
                )

            return converted, param_list

    def _process_result(self, result: QueryResult) -> None:
        """Process query result into cursor state."""
        self._last_result = result
        self.description = build_cursor_description(result.fields)
        self.rowcount = result.row_count

        self._rows = []
        for row in result.rows:
            if isinstance(row, dict):
                ordered = tuple(row.get(f["name"]) for f in result.fields)
                self._rows.append(ordered)
            elif isinstance(row, (list, tuple)):
                self._rows.append(tuple(row))
            else:
                self._rows.append((row,))

        self._index = 0

    async def _async_soft_close(self) -> None:
        return

    def execute(
        self,
        operation: str,
        parameters: tuple | dict | None = None,
    ) -> "AsyncAdapt_neon_cursor":
        """Execute a SQL operation.

        Uses await_fallback() to run async query in sync/async contexts.

        Note: All queries execute immediately via individual HTTP requests.
        Transaction buffering is not used because SQLAlchemy ORM requires
        immediate results from RETURNING clauses. Each query runs in its own
        implicit transaction on the Neon server.

        For atomic multi-statement transactions, use the HTTP client's
        transaction() method directly.
        """
        self._check_closed()

        query, params = self._convert_params(operation, parameters)

        # Always execute immediately - buffering breaks SQLAlchemy ORM's
        # expectation of immediate RETURNING results
        result = self._adapt_connection.await_(self._connection.query(query, params))
        # result = await self._connection.query(query, params)
        self._process_result(result)

        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[tuple | dict],
    ) -> "AsyncAdapt_neon_cursor":
        """Execute operation multiple times."""
        self._check_closed()

        total_rowcount = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.rowcount >= 0:
                total_rowcount += self.rowcount

        self.rowcount = total_rowcount
        return self

    def fetchone(self) -> tuple | None:
        """Fetch next row."""
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self._check_closed()

        if self._index >= len(self._rows):
            return None

        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch multiple rows."""
        self._check_closed()

        if size is None:
            size = self.arraysize

        rows = self._rows[self._index : self._index + size]
        self._index += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        self._check_closed()

        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._rows = []
        self._index = 0
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        await_only(self._connection.close())

    def setinputsizes(self, sizes) -> None:
        """Set input sizes (no-op)."""
        pass

    def setoutputsize(self, size, column=None) -> None:
        """Set output size (no-op)."""
        pass

    def __iter__(self) -> Iterator[tuple]:
        return self

    def __next__(self) -> tuple:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "AsyncAdapt_neon_cursor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# Aliases for compatibility
Connection = AsyncAdapt_neon_connection
Cursor = AsyncAdapt_neon_cursor
