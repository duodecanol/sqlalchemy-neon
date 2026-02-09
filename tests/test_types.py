"""Tests for type conversion utilities."""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal

import pytest

from sqlalchemy_neon.types import TypeConverter, PostgresOID, build_cursor_description

@pytest.fixture
def converter() -> TypeConverter:
    """Create a TypeConverter instance."""
    return TypeConverter()

class TestTypeConverter:
    """Tests for TypeConverter class."""


    def test_python_to_pg_none(self, converter: TypeConverter):
        """Test None -> NULL conversion."""
        assert converter.python_to_pg(None) is None

    def test_python_to_pg_int(self, converter: TypeConverter):
        """Test integer conversion."""
        result = converter.python_to_pg(42)
        assert result == "42"

    def test_python_to_pg_float(self, converter: TypeConverter):
        """Test float conversion."""
        result = converter.python_to_pg(3.14)
        assert  result == "3.14"

    def test_python_to_pg_str(self, converter: TypeConverter):
        """Test string conversion."""
        result = converter.python_to_pg("hello")
        assert result == "hello"

    def test_python_to_pg_bool(self, converter: TypeConverter):
        """Test boolean conversion."""
        assert converter.python_to_pg(True) == "t"
        assert converter.python_to_pg(False) == "f"

    def test_python_to_pg_bytes(self, converter: TypeConverter):
        """Test bytes -> hex conversion."""
        result = converter.python_to_pg(b"\xde\xad\xbe\xef")
        assert result == "\\xdeadbeef"

    def test_python_to_pg_date(self, converter: TypeConverter):
        """Test date conversion."""
        d = datetime.date(2024, 1, 15)
        result = converter.python_to_pg(d)
        assert "2024-01-15" in result

    def test_python_to_pg_datetime(self, converter: TypeConverter):
        """Test datetime conversion."""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        result = converter.python_to_pg(dt)
        assert "2024-01-15" in result
        assert "10:30" in result

    def test_python_to_pg_uuid(self, converter: TypeConverter):
        """Test UUID conversion."""
        u = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        result = converter.python_to_pg(u)
        assert "550e8400" in result.lower()

    def test_python_to_pg_decimal(self, converter: TypeConverter):
        """Test Decimal conversion."""
        d = Decimal("123.45")
        result = converter.python_to_pg(d)
        assert "123.45" in result

    def test_python_to_pg_dict(self, converter: TypeConverter):
        """Test dict -> JSON conversion."""
        d = {"key": "value", "num": 42}
        result = converter.python_to_pg(d)
        assert "key" in result
        assert "value" in result

    def test_convert_params(self, converter: TypeConverter):
        """Test batch parameter conversion."""
        params = [1, "hello", True, None]
        result = converter.convert_params(params)
        assert result == ["1", "hello", "t", None]

    def test_convert_params_none(self, converter: TypeConverter):
        """Test None params returns empty list."""
        assert converter.convert_params(None) == []

    def test_pg_to_python_none(self, converter: TypeConverter):
        """Test NULL -> None conversion."""
        assert converter.pg_to_python(None, PostgresOID.TEXT) is None

    def test_pg_to_python_int(self, converter: TypeConverter):
        """Test text -> int conversion."""
        result = converter.pg_to_python("42", PostgresOID.INT4)
        assert result == 42
        assert isinstance(result, int)

    def test_pg_to_python_bool(self, converter: TypeConverter):
        """Test text -> bool conversion."""
        assert converter.pg_to_python("t", PostgresOID.BOOL) is True
        assert converter.pg_to_python("f", PostgresOID.BOOL) is False

    def test_pg_to_python_text(self, converter: TypeConverter):
        """Test text -> str conversion."""
        result = converter.pg_to_python("hello", PostgresOID.TEXT)
        assert result == "hello"


class TestBuildCursorDescription:
    """Tests for build_cursor_description function."""

    def test_empty_fields(self):
        """Test empty fields returns None."""
        assert build_cursor_description([]) is None

    def test_single_field(self):
        """Test single field description."""
        fields = [{"name": "id", "dataTypeID": 23, "dataTypeSize": 4}]
        result = build_cursor_description(fields)

        assert len(result) == 1
        assert result[0][0] == "id"
        assert result[0][1] == 23  # type_code
        assert result[0][3] == 4   # internal_size

    def test_multiple_fields(self):
        """Test multiple fields description."""
        fields = [
            {"name": "id", "dataTypeID": 23, "dataTypeSize": 4},
            {"name": "name", "dataTypeID": 1043, "dataTypeSize": -1},
        ]
        result = build_cursor_description(fields)

        assert len(result) == 2
        assert result[0][0] == "id"
        assert result[1][0] == "name"
