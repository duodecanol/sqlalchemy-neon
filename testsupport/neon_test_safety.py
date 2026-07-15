"""Safety checks for destructive live database tests."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import unquote, urlsplit

NEON_TEST_ALLOW_DESTRUCTIVE_ENV = "NEON_TEST_ALLOW_DESTRUCTIVE"
NEON_TEST_ALLOWED_DATABASES_ENV = "NEON_TEST_ALLOWED_DATABASES"


class DestructiveTestTargetError(RuntimeError):
    """Raised when a live test target is not explicitly approved for DDL."""


def validate_destructive_test_database(
    database_url: str,
    environment: Mapping[str, str],
) -> str:
    """Return the approved database name or refuse destructive test setup."""
    if environment.get(NEON_TEST_ALLOW_DESTRUCTIVE_ENV) != "1":
        raise DestructiveTestTargetError(
            f"Set {NEON_TEST_ALLOW_DESTRUCTIVE_ENV}=1 to permit destructive tests."
        )

    allowed_databases = {
        database_name.strip()
        for database_name in environment.get(
            NEON_TEST_ALLOWED_DATABASES_ENV, ""
        ).split(",")
        if database_name.strip()
    }
    if not allowed_databases:
        raise DestructiveTestTargetError(
            f"Set {NEON_TEST_ALLOWED_DATABASES_ENV} to the dedicated test database name."
        )

    database_name = _database_name(database_url)
    if database_name not in allowed_databases:
        raise DestructiveTestTargetError(
            f"Database {database_name!r} is not in {NEON_TEST_ALLOWED_DATABASES_ENV}."
        )

    return database_name


def _database_name(database_url: str) -> str:
    parsed = urlsplit(database_url)
    database_name = unquote(parsed.path.removeprefix("/"))
    if (
        not parsed.scheme.startswith("postgresql")
        or not parsed.netloc
        or not database_name
        or "/" in database_name
    ):
        raise DestructiveTestTargetError(
            "NEON_TEST_DATABASE_URL must be a PostgreSQL URL with one database name."
        )

    return database_name
