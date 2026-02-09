"""Native async SQLAlchemy execution for Neon over HTTP."""

from __future__ import annotations

__version__ = "0.1.0"

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

from .native_async_engine import (
    NeonNativeAsyncEngine,
    NativeAsyncResult,
    create_neon_native_async_engine,
    compile_sql,
)

__all__ = [
    # Version
    "__version__",
    # Native engine
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
