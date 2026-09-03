"""
Type conversion utilities for Neon HTTP API.

Uses psycopg's type system for Python <-> PostgreSQL type conversion.
All values are transmitted as text over HTTP, with OIDs for type identification.
"""

from __future__ import annotations

import datetime
import re
from typing import Any, Sequence

from psycopg.adapt import PyFormat, Transformer
from psycopg.postgres import register_default_adapters, register_default_types, types
from psycopg.pq import Format
from psycopg.types.json import Jsonb

from .errors import NeonTypeError


_POSTGRES_INTERVAL_RE = re.compile(
    rb"""
    (?: ([-+]?\d+) \s+ years? \s* )?
    (?: ([-+]?\d+) \s+ mons? \s* )?
    (?: ([-+]?\d+) \s+ days? \s* )?
    (?: ([-+])? (\d+) : (\d+) : (\d+ (?:\.\d+)?))? \s*
    """,
    re.VERBOSE,
)


def _parse_postgres_interval(value: str) -> datetime.timedelta:
    """Parse PostgreSQL ``IntervalStyle=postgres`` text without psycopg globals."""
    match = _POSTGRES_INTERVAL_RE.fullmatch(value.encode())
    if not match:
        raise ValueError("invalid PostgreSQL interval")

    years, months, days_value, sign, hours, minutes, seconds_value = match.groups()
    days = 0
    seconds = 0.0
    if years:
        days += 365 * int(years)
    if months:
        days += 30 * int(months)
    if days_value:
        days += int(days_value)
    if hours:
        seconds = 3600 * int(hours) + 60 * int(minutes) + float(seconds_value)
        if sign == b"-":
            seconds = -seconds

    return datetime.timedelta(days=days, seconds=seconds)


class TypeConverter:
    """Handles type conversion between Python and PostgreSQL for Neon HTTP API.

    All values are serialized to text format for HTTP transmission.
    Response values come back as text with PostgreSQL OIDs for type identification.
    """

    def __init__(self) -> None:
        """Initialize the type converter with a psycopg Transformer."""
        register_default_types(types)
        self._transformer = Transformer()
        register_default_adapters(self._transformer)

    def python_to_pg(self, value: Any) -> str | None:
        """Convert a Python value to PostgreSQL text format.

        Args:
            value: Any Python value to convert.

        Returns:
            String representation for PostgreSQL, or None for NULL.

        Raises:
            NeonTypeError: If the value cannot be converted.
        """
        if value is None:
            return None
        # Handle dict -> JSONB conversion
        if isinstance(value, dict):
            value = Jsonb(value)
        # Handle bytes -> hex format with \\x prefix
        if isinstance(value, bytes):
            value = "\\x" + value.hex()

        try:
            # Use psycopg's dumper for text format
            dumper = self._transformer.get_dumper(value, PyFormat.TEXT)
            result = dumper.dump(value)

            # Ensure we return a string
            if isinstance(result, bytes):
                result = result.decode("utf-8")
            elif isinstance(result, bytearray):
                result = result.decode("utf-8")
            elif isinstance(result, memoryview):
                result = bytes(result).decode("utf-8")
        except Exception as e:
            raise NeonTypeError(
                f"Failed to convert Python value to PostgreSQL: {e}"
            ) from e
        return result

    def pg_to_python(self, value: str | None, oid: int) -> Any:
        """Convert a PostgreSQL text value to Python using the OID.

        Unknown OIDs preserve their raw text value. Malformed values for known
        OIDs raise NeonTypeError without including the value in the error.

        Args:
            value: Text value from PostgreSQL response, or None for NULL.
            oid: PostgreSQL OID (dataTypeID) for type identification.

        Returns:
            Converted Python value.

        Raises:
            NeonTypeError: If a known PostgreSQL type cannot convert the value.
        """
        if value is None:
            return None

        type_info = types.get(oid)
        if type_info is None:
            return value

        try:
            if oid == PostgresOID.INTERVAL:
                return _parse_postgres_interval(value)
            loader = self._transformer.get_loader(oid, Format.TEXT)
            return loader.load(value.encode())
        except Exception:
            raise NeonTypeError(
                f"Failed to convert PostgreSQL value for OID {oid} ({type_info.name})"
            ) from None

    def convert_params(
        self, params: Sequence[Any] | tuple[Any, ...] | None
    ) -> list[str | None]:
        """Convert a list of Python parameters to PostgreSQL text format.

        Args:
            params: List or tuple of Python values.

        Returns:
            List of string values for the Neon HTTP API.
        """
        if params is None:
            return []

        return [self.python_to_pg(p) for p in params]

    def convert_row(
        self,
        row: dict[str, Any] | Sequence[Any],
        fields: Sequence[dict[str, Any]],
        array_mode: bool = False,
    ) -> tuple[Any, ...] | dict[str, Any]:
        """Convert a row from Neon response to Python types.

        Args:
            row: Row data (dict in object mode, list in array mode).
            fields: Field metadata with dataTypeID for each column.
            array_mode: Whether the response is in array mode.

        Returns:
            Tuple of converted values (for DBAPI compatibility) or dict.
        """
        if array_mode:
            # Array mode: row is a list of values
            converted = []
            for i, value in enumerate(row):
                oid = fields[i].get("dataTypeID", 25)  # Default to text
                converted.append(self.pg_to_python(value, oid))
            return tuple(converted)
        else:
            # Object mode: row is a dict
            converted = {}
            field_map = {f["name"]: f for f in fields}
            for key, value in row.items():
                field = field_map.get(key, {})
                oid = field.get("dataTypeID", 25)  # Default to text
                converted[key] = self.pg_to_python(value, oid)
            return converted


# Common PostgreSQL OIDs for reference
class PostgresOID:
    """Common PostgreSQL type OIDs."""

    BOOL = 16
    BYTEA = 17
    CHAR = 18
    NAME = 19
    INT8 = 20
    INT2 = 21
    INT4 = 23
    TEXT = 25
    OID = 26
    JSON = 114
    FLOAT4 = 700
    FLOAT8 = 701
    MONEY = 790
    VARCHAR = 1043
    DATE = 1082
    TIME = 1083
    TIMESTAMP = 1114
    TIMESTAMPTZ = 1184
    INTERVAL = 1186
    TIMETZ = 1266
    NUMERIC = 1700
    UUID = 2950
    JSONB = 3802

    # Array types (base OID + some offset, typically)
    INT4_ARRAY = 1007
    TEXT_ARRAY = 1009
    FLOAT8_ARRAY = 1022


def build_cursor_description(fields: list[dict[str, Any]]) -> tuple[tuple, ...] | None:
    """Build PEP 249 cursor.description from Neon field metadata.

    Args:
        fields: List of field metadata dicts from Neon response.

    Returns:
        Tuple of 7-tuples as per PEP 249, or None if no fields.

    Each tuple contains:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
    """
    if not fields:
        return None

    description = []
    for field in fields:
        name = field.get("name", "")
        type_code = field.get("dataTypeID", PostgresOID.TEXT)
        internal_size = field.get("dataTypeSize", -1)

        # PEP 249 requires 7-tuple
        description.append(
            (
                name,  # name
                type_code,  # type_code (OID)
                None,  # display_size (not provided)
                internal_size,  # internal_size
                None,  # precision (not provided)
                None,  # scale (not provided)
                None,  # null_ok (not provided)
            )
        )

    return tuple(description)
