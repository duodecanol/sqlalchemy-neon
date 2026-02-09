"""Tests for SQLAlchemy dialect."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text, pool
from sqlalchemy.engine import URL

from sqlalchemy_neon.dialect import NeonHTTPDialect, NeonHTTPDialect_async


class TestNeonHTTPDialect:
    """Tests for NeonHTTPDialect class."""

    def test_dialect_name(self):
        """Test dialect name."""
        assert NeonHTTPDialect.name == "postgresql"

    def test_dialect_driver(self):
        """Test dialect driver."""
        assert NeonHTTPDialect.driver == "neonhttp"

    def test_dbapi(self):
        """Test dbapi() returns the dbapi module."""
        dbapi = NeonHTTPDialect.import_dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "Connection")
        assert hasattr(dbapi, "Cursor")

    def test_get_pool_class(self):
        """Test pool class is NullPool."""
        url = URL.create("postgresql+neonhttp", host="test")
        pool_class = NeonHTTPDialect.get_pool_class(url)
        assert pool_class is pool.NullPool

    def test_create_connect_args(self):
        """Test URL to connect args conversion."""
        dialect = NeonHTTPDialect()
        url = URL.create(
            "postgresql+neonhttp",
            username="testuser",
            password="testpass",
            host="test-host.neon.tech",
            port=5432,
            database="testdb",
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == []
        assert "dsn" in kwargs
        assert "testuser" in kwargs["dsn"]
        assert "testpass" in kwargs["dsn"]
        assert "test-host.neon.tech" in kwargs["dsn"]
        assert "testdb" in kwargs["dsn"]

    def test_create_connect_args_with_auth_token(self):
        """Test auth_token extraction from URL."""
        dialect = NeonHTTPDialect()
        url = URL.create(
            "postgresql+neonhttp",
            username="testuser",
            password="testpass",
            host="test-host.neon.tech",
            database="testdb",
            query={"auth_token": "my-jwt-token"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert kwargs["auth_token"] == "my-jwt-token"

    def test_create_connect_args_with_timeout(self):
        """Test timeout extraction from URL."""
        dialect = NeonHTTPDialect()
        url = URL.create(
            "postgresql+neonhttp",
            username="testuser",
            password="testpass",
            host="test-host.neon.tech",
            database="testdb",
            query={"timeout": "60"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert kwargs["timeout"] == 60.0


class TestNeonHTTPDialectAsync:
    """Tests for NeonHTTPDialect_async class."""

    def test_is_async(self):
        """Test dialect is marked as async."""
        assert NeonHTTPDialect_async.is_async is True

    def test_dbapi(self):
        """Test async dbapi module."""
        dbapi = NeonHTTPDialect_async.dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "AsyncAdapt_neon_connection")

    def test_get_pool_class(self):
        """Test pool class is AsyncAdaptedQueuePool."""
        url = URL.create("postgresql+neonhttp", host="test")
        pool_class = NeonHTTPDialect_async.get_pool_class(url)
        assert pool_class is pool.AsyncAdaptedQueuePool


class TestDialectRegistration:
    """Tests for dialect registration with SQLAlchemy."""

    def test_create_engine_url_parsing(self, mock_connection_string: str):
        """Test that SQLAlchemy can parse the URL.

        Note: This doesn't actually connect, just tests URL parsing.
        """
        # Replace postgresql:// with postgresql+neonhttp://
        url = mock_connection_string.replace(
            "postgresql://", "postgresql+neonhttp://"
        )

        # Create engine with NullPool so no connection is made
        engine = create_engine(
            url,
            poolclass=pool.NullPool,
        )

        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "neonhttp"
