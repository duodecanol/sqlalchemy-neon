"""Tests for DBAPI module."""

from __future__ import annotations

import pytest

from sqlalchemy_neon import dbapi
from sqlalchemy_neon.errors import InterfaceError, ProgrammingError


class TestDBAPIModule:
    """Tests for DBAPI module-level attributes."""

    def test_apilevel(self):
        """Test API level is 2.0."""
        assert dbapi.apilevel == "2.0"

    def test_threadsafety(self):
        """Test thread safety level."""
        assert dbapi.threadsafety == 1

    def test_paramstyle(self):
        """Test parameter style."""
        assert dbapi.paramstyle == "pyformat"


class TestConnect:
    """Tests for dbapi.connect() function."""

    def test_connect_requires_dsn(self):
        """Test that connect() requires dsn."""
        with pytest.raises(InterfaceError, match="dsn.*required"):
            dbapi.connect(None)

    def test_connect_returns_connection(self, mock_connection_string: str):
        """Test that connect() returns a Connection."""
        conn = dbapi.connect(mock_connection_string)
        assert isinstance(conn, dbapi.Connection)


class TestConnection:
    """Tests for Connection class."""

    @pytest.fixture
    def connection(self, mock_connection_string: str) -> dbapi.Connection:
        """Create a connection for testing."""
        conn = dbapi.connect(mock_connection_string)
        yield conn

    def test_cursor(self, connection: dbapi.Connection):
        """Test cursor creation."""
        cursor = connection.cursor()
        assert isinstance(cursor, dbapi.Cursor)
        cursor.close()

    def test_autocommit_default(self, connection: dbapi.Connection):
        """Test autocommit defaults to True."""
        assert connection.autocommit is True

    def test_autocommit_setter(self, connection: dbapi.Connection):
        """Test setting autocommit."""
        connection.autocommit = False
        assert connection.autocommit is False

    def test_context_manager(self, mock_connection_string: str):
        """Test connection as context manager (outside greenlet)."""
        conn = dbapi.connect(mock_connection_string)
        assert not conn._closed
        # Note: proper close requires greenlet context for await_only


class TestCursor:
    """Tests for Cursor class."""

    @pytest.fixture
    def cursor(self, mock_connection_string: str) -> dbapi.Cursor:
        """Create a cursor for testing."""
        conn = dbapi.connect(mock_connection_string)
        cursor = conn.cursor()
        yield cursor
        cursor.close()

    def test_description_initial(self, cursor: dbapi.Cursor):
        """Test description is None before execute."""
        assert cursor.description is None

    def test_rowcount_initial(self, cursor: dbapi.Cursor):
        """Test rowcount is -1 before execute."""
        assert cursor.rowcount == -1

    def test_arraysize_default(self, cursor: dbapi.Cursor):
        """Test arraysize defaults to 1."""
        assert cursor.arraysize == 1

    def test_close(self, cursor: dbapi.Cursor):
        """Test cursor close."""
        cursor.close()
        assert cursor._closed

    def test_fetchone_after_close(self, cursor: dbapi.Cursor):
        """Test fetchone() fails after close."""
        cursor.close()
        with pytest.raises(InterfaceError, match="closed"):
            cursor.fetchone()

    def test_context_manager(self, mock_connection_string: str):
        """Test cursor as context manager."""
        conn = dbapi.connect(mock_connection_string)
        with conn.cursor() as cursor:
            assert not cursor._closed
        assert cursor._closed


class TestCursorParamConversion:
    """Tests for cursor parameter conversion."""

    @pytest.fixture
    def cursor(self, mock_connection_string: str) -> dbapi.Cursor:
        """Create a cursor for testing."""
        conn = dbapi.connect(mock_connection_string)
        cursor = conn.cursor()
        yield cursor
        cursor.close()

    def test_convert_positional_params(self, cursor: dbapi.Cursor):
        """Test %s -> $n conversion."""
        query, params = cursor._convert_params(
            "SELECT * FROM users WHERE id = %s AND name = %s",
            (1, "Alice"),
        )
        assert query == "SELECT * FROM users WHERE id = $1 AND name = $2"
        assert params == [1, "Alice"]

    def test_convert_named_params(self, cursor: dbapi.Cursor):
        """Test %(name)s -> $n conversion."""
        query, params = cursor._convert_params(
            "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s",
            {"id": 1, "name": "Alice"},
        )
        assert query == "SELECT * FROM users WHERE id = $1 AND name = $2"
        assert params == [1, "Alice"]

    def test_convert_named_params_reuse(self, cursor: dbapi.Cursor):
        """Test reusing named parameters."""
        query, params = cursor._convert_params(
            "SELECT * FROM t WHERE a = %(x)s AND b = %(x)s",
            {"x": 42},
        )
        assert query == "SELECT * FROM t WHERE a = $1 AND b = $1"
        assert params == [42]

    def test_convert_no_params(self, cursor: dbapi.Cursor):
        """Test query without parameters."""
        query, params = cursor._convert_params("SELECT 1", None)
        assert query == "SELECT 1"
        assert params == []

    def test_mixed_params_error(self, cursor: dbapi.Cursor):
        """Test mixing %s and %(name)s raises error."""
        with pytest.raises(ProgrammingError, match="Cannot mix"):
            cursor._convert_params(
                "SELECT * FROM t WHERE a = %s AND b = %(x)s",
                (1,),
            )

    def test_missing_named_param(self, cursor: dbapi.Cursor):
        """Test missing named parameter raises error."""
        with pytest.raises(ProgrammingError, match="Missing parameter"):
            cursor._convert_params(
                "SELECT * FROM t WHERE a = %(missing)s",
                {"other": 1},
            )

    def test_param_count_mismatch(self, cursor: dbapi.Cursor):
        """Test parameter count mismatch raises error."""
        with pytest.raises(ProgrammingError, match="mismatch"):
            cursor._convert_params(
                "SELECT * FROM t WHERE a = %s AND b = %s",
                (1,),  # Only 1 param, but 2 placeholders
            )
