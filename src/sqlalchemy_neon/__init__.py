"""Native async SQLAlchemy execution for Neon over HTTP/WebSocket."""

from __future__ import annotations

__version__ = "0.1.0"

# Import HTTP client for direct use
from .neon_http_client import (
    AsyncNeonHTTPClient,
    AsyncNeonWebSocketClient,
    AsyncNeonWebSocketPool,
    NeonHTTPClient,  # Alias for AsyncNeonHTTPClient
    NeonWebSocketClient,  # Alias for AsyncNeonWebSocketClient
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

from .native_async_engine import (
    NeonNativeAsyncEngine,
    NativeAsyncResult,
    create_neon_http_engine,
    create_neon_ws_engine,
    create_neon_native_async_engine,
    compile_sql,
)

__all__ = [
    # Version
    "__version__",
    # Native engine
    "NeonNativeAsyncEngine",
    "NativeAsyncResult",
    "create_neon_http_engine",
    "create_neon_ws_engine",
    "create_neon_native_async_engine",
    "compile_sql",
    # HTTP client
    "AsyncNeonHTTPClient",
    "AsyncNeonWebSocketClient",
    "AsyncNeonWebSocketPool",
    "NeonHTTPClient",
    "NeonWebSocketClient",
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
