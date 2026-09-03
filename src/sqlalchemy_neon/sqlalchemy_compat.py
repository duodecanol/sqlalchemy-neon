"""Compatibility boundary for SQLAlchemy private metadata.

The native engine depends on a small set of SQLAlchemy internals for ORM
loader inspection and result type processing. Keep those accesses here so a
SQLAlchemy minor-version change has one audited boundary.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import ClauseElement

from .errors import NeonConfigurationError

SUPPORTED_SQLALCHEMY_MIN = (2, 0)
SUPPORTED_SQLALCHEMY_MAX_EXCLUSIVE = (2, 1)


def ensure_supported_sqlalchemy() -> None:
    """Reject SQLAlchemy versions outside the tested 2.0 minor line."""
    version_parts = sa.__version__.split(".")
    try:
        version = (int(version_parts[0]), int(version_parts[1]))
    except (IndexError, ValueError) as exc:
        raise NeonConfigurationError(
            f"Unable to determine supported SQLAlchemy version from {sa.__version__!r}."
        ) from exc

    if not (
        SUPPORTED_SQLALCHEMY_MIN
        <= version
        < SUPPORTED_SQLALCHEMY_MAX_EXCLUSIVE
    ):
        raise NeonConfigurationError(
            "Unsupported SQLAlchemy version "
            f"{sa.__version__}; supported versions are >=2.0,<2.1."
        )


def postgres_dialect(*, paramstyle: str | None = None) -> Any:
    """Construct the psycopg dialect used for native statement compilation."""
    dialect = postgresql.psycopg.PGDialect_psycopg
    return dialect(paramstyle=paramstyle) if paramstyle is not None else dialect()


def statement_loader_options(statement: ClauseElement) -> tuple[Any, ...]:
    """Return loader options through the supported compatibility boundary."""
    return getattr(statement, "_with_options", ())


def loader_contexts(load_option: Any) -> tuple[Any, ...]:
    """Return loader option contexts."""
    return getattr(load_option, "context", ())


def loader_path(load_option: Any) -> Any:
    """Return the ORM path represented by a loader option."""
    return getattr(load_option, "path", None)


def loader_strategy(context: Any) -> tuple[tuple[str, Any], ...]:
    """Return a loader context's strategy entries."""
    return getattr(context, "strategy", ())


def strip_loader_options(statement: ClauseElement) -> ClauseElement:
    """Clone a statement without ORM loader options."""
    statement_internal: Any = statement
    stripped = statement_internal._generate()
    stripped._with_options = ()
    return stripped


def compiled_result_columns(compiled: Any) -> Any:
    """Return compiled result metadata for native type processing."""
    return getattr(compiled, "_result_columns", None)
