from __future__ import annotations

import sqlalchemy as sa
import pytest

from sqlalchemy_neon import sqlalchemy_compat
from sqlalchemy_neon.errors import NeonConfigurationError
from sqlalchemy_neon.native_async_engine import compile_sql


def test_installed_sqlalchemy_version_is_supported():
    sqlalchemy_compat.ensure_supported_sqlalchemy()


def test_unsupported_sqlalchemy_minor_version_is_rejected(monkeypatch):
    monkeypatch.setattr(sa, "__version__", "2.1.0")

    with pytest.raises(NeonConfigurationError, match="supported versions"):
        compile_sql("select 1")


def test_unreadable_sqlalchemy_version_is_rejected(monkeypatch):
    monkeypatch.setattr(sa, "__version__", "development")

    with pytest.raises(NeonConfigurationError, match="Unable to determine"):
        sqlalchemy_compat.ensure_supported_sqlalchemy()
