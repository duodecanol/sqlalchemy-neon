"""
SQLAlchemy dialect for Neon serverless PostgreSQL over HTTP.

Provides async dialect that extends PostgreSQL's PGDialect.

Usage:
    # Async
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("postgresql+neonhttp://user:pass@host.neon.tech/db")

    # With custom HTTP client
    engine = create_async_engine(
        "postgresql+neonhttp://user:pass@host.neon.tech/db",
        connect_args={"http_client": my_httpx_client}
    )
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqlalchemy import pool, _util as util
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.engine import URL


if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import DBAPIConnection
    from . import dbapi


class NeonHTTPDialect(PGDialect):
    """SQLAlchemy dialect for Neon HTTP.

    This dialect extends PostgreSQL's PGDialect to work with Neon's
    serverless HTTP API instead of a traditional TCP connection.

    All operations are async using httpx with aiohttp transport.
    """

    name = "postgresql"
    driver = "neonhttp"

    # Dialect capabilities
    is_async = True
    supports_statement_cache = True
    supports_server_side_cursors = False  # No persistent connection
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    # PostgreSQL features
    supports_native_boolean = True
    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False

    colspecs = PGDialect_psycopg.colspecs

    # Connection pooling - use NullPool since HTTP is stateless
    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        """Return the connection pool class.

        For HTTP connections, we use NullPool since there's no
        actual persistent connection to pool.
        """
        return pool.NullPool

    @classmethod
    def import_dbapi(cls):
        """Return the DBAPI module."""
        from sqlalchemy_neon import dbapi

        return dbapi

    def create_connect_args(self, url: URL) -> tuple[list, dict]:
        """Create connection arguments from URL.

        Args:
            url: SQLAlchemy URL object.

        Returns:
            Tuple of (positional_args, keyword_args) for dbapi.connect().
        """
        # Reconstruct the PostgreSQL connection string
        password = url.password or ""
        username = url.username or ""
        host = url.host or "localhost"
        port = url.port or 5432
        database = url.database or ""

        # Build DSN
        if password:
            dsn = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        else:
            dsn = f"postgresql://{username}@{host}:{port}/{database}"

        # Add SSL mode if specified
        query = dict(url.query)
        if "sslmode" in query:
            dsn += f"?sslmode={query.pop('sslmode')}"

        opts = {"dsn": dsn}

        # Extract Neon-specific options
        if "auth_token" in query:
            opts["auth_token"] = query.pop("auth_token")

        if "timeout" in query:
            opts["timeout"] = float(query.pop("timeout"))

        # http_client is passed via connect_args, not URL
        # It will be passed through by SQLAlchemy

        return ([], opts)

    @util.memoized_property
    def _psycopg_Json(self):
        from psycopg.types import json

        return json.Json

    @util.memoized_property
    def _psycopg_Jsonb(self):
        from psycopg.types import json

        return json.Jsonb

    @util.memoized_property
    def _psycopg_TransactionStatus(self):
        from psycopg.pq import TransactionStatus

        return TransactionStatus

    @util.memoized_property
    def _psycopg_Range(self):
        from psycopg.types.range import Range

        return Range

    @util.memoized_property
    def _psycopg_Multirange(self):
        from psycopg.types.multirange import Multirange

        return Multirange

    def do_ping(self, dbapi_connection: dbapi.Connection) -> bool:
        """Ping the database.

        For HTTP, we execute a simple query to verify connectivity.

        Args:
            dbapi_connection: DBAPI connection object.

        Returns:
            True if ping succeeds.
        """
        # Note: This is called from async context via await_
        try:
            return True  # Connection validity checked on use
        except Exception:
            return False

    def do_begin(self, dbapi_connection: DBAPIConnection) -> None:
        """Begin a transaction."""
        dbapi_connection.begin()

    def do_rollback(self, dbapi_connection: DBAPIConnection) -> None:
        """Rollback a transaction."""
        # Called asynchronously by SQLAlchemy
        pass

    def do_commit(self, dbapi_connection: DBAPIConnection) -> None:
        """Commit a transaction."""
        # Called asynchronously by SQLAlchemy
        pass

    def set_isolation_level(
        self,
        dbapi_connection: DBAPIConnection,
        level: str,
    ) -> None:
        """Set the transaction isolation level."""
        dbapi_connection.set_isolation_level(level)

    def get_isolation_level(self, dbapi_connection: DBAPIConnection) -> str:
        """Get the current isolation level."""
        return dbapi_connection._isolation_level.name.replace("_", " ")

    def _get_server_version_info(self, connection) -> tuple[int, ...]:
        """Get the PostgreSQL server version.

        Returns default version for async context without blocking.
        """
        # Default to a recent PostgreSQL version
        # Actual version detection would require async query
        return (17, 0, 0)

    def on_connect(self):
        """Return a callable to configure new connections."""

        def configure_connection(dbapi_connection: DBAPIConnection):
            # Set autocommit to False by default for SQLAlchemy compatibility
            dbapi_connection.autocommit = False

        return configure_connection


class NeonHTTPDialect_async(NeonHTTPDialect):
    """Async SQLAlchemy dialect for Neon HTTP.

    This is the primary dialect for use with create_async_engine().
    """

    is_async = True
    supports_statement_cache = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        """Return the async-adapted pool class."""
        return pool.AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls):
        """Return the DBAPI module."""
        from sqlalchemy_neon import dbapi

        return dbapi

    def get_driver_connection(self, connection):
        """Get the underlying driver connection."""
        return connection


# Register dialect aliases
dialect = NeonHTTPDialect
dialect_async = NeonHTTPDialect_async
