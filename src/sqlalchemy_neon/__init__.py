"""
SQLAlchemy dialect for Neon serverless PostgreSQL over HTTP.

This package provides a SQLAlchemy-compatible async dialect that communicates
with Neon's serverless PostgreSQL via HTTP instead of TCP connections.

Usage:
    # Async engine (recommended)
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("postgresql+neonhttp_async://user:pass@host.neon.tech/db")

    # With auth token for Row-Level Security
    engine = create_async_engine(
        "postgresql+neonhttp_async://user:pass@host.neon.tech/db?auth_token=YOUR_JWT"
    )

    # With custom HTTP client
    import httpx
    from httpx_aiohttp import HttpxAiohttpClient

    async with HttpxAiohttpClient() as http_client:
        engine = create_async_engine(
            "postgresql+neonhttp_async://...",
            connect_args={"http_client": http_client}
        )

Features:
    - Fully async HTTP connections using httpx + aiohttp
    - Full SQLAlchemy ORM support
    - Transaction buffering (queries batch on commit)
    - PostgreSQL type conversion via psycopg
    - JWT auth token support for Row-Level Security
    - HTTP/2 support

Limitations:
    - No interactive transactions (statements are batched)
    - No server-side cursors
    - No COPY command
    - No LISTEN/NOTIFY
"""

from __future__ import annotations

__version__ = "0.1.0"

# Import DBAPI module
from . import dbapi

# Alias for backwards compatibility
dbapi_async = dbapi

# Import dialect classes
from .dialect import NeonHTTPDialect, NeonHTTPDialect_async

# Import HTTP client for direct use
from .neon_http_client import (
    AsyncNeonHTTPClient,
    NeonHTTPClient,  # Alias for AsyncNeonHTTPClient
    QueryResult,
    QueryOptions,
    TransactionOptions,
    IsolationLevel,
)

# Import type converter
from .types import TypeConverter, PostgresOID

# Import exceptions
from .errors import (
    Error,
    Warning,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    NeonError,
    NeonConnectionError,
    NeonHTTPError,
    NeonAuthenticationError,
    NeonQueryError,
    NeonTransactionError,
    NeonTypeError,
    NeonConfigurationError,
)

# Import session class
from .session import NeonAsyncSession
from .pool import PoolQuery, PoolQueryResult, execute_pool_query, execute_pool_queries
from .native_async_engine import (
    NeonNativeAsyncEngine,
    NativeAsyncResult,
    create_neon_native_async_engine,
    compile_sql,
)

__all__ = [
    # Version
    "__version__",
    # DBAPI modules
    "dbapi",
    "dbapi_async",
    # Dialect classes
    "NeonHTTPDialect",
    "NeonHTTPDialect_async",
    # Session
    "NeonAsyncSession",
    "PoolQuery",
    "PoolQueryResult",
    "execute_pool_query",
    "execute_pool_queries",
    "NeonNativeAsyncEngine",
    "NativeAsyncResult",
    "create_neon_native_async_engine",
    "compile_sql",
    # HTTP client
    "AsyncNeonHTTPClient",
    "NeonHTTPClient",
    "QueryResult",
    "QueryOptions",
    "TransactionOptions",
    "IsolationLevel",
    # Types
    "TypeConverter",
    "PostgresOID",
    # PEP 249 exceptions
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
    # Neon-specific exceptions
    "NeonError",
    "NeonConnectionError",
    "NeonHTTPError",
    "NeonAuthenticationError",
    "NeonQueryError",
    "NeonTransactionError",
    "NeonTypeError",
    "NeonConfigurationError",
]
