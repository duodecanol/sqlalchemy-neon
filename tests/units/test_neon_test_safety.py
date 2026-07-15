from __future__ import annotations

import pytest

from testsupport.neon_test_safety import (
    NEON_TEST_ALLOWED_DATABASES_ENV,
    NEON_TEST_ALLOW_DESTRUCTIVE_ENV,
    DestructiveTestTargetError,
    validate_destructive_test_database,
)

DATABASE_URL = "postgresql://testuser:testpass@test-host.neon.tech:5432/neondb_test"


def test_destructive_tests_require_explicit_opt_in():
    with pytest.raises(DestructiveTestTargetError, match="NEON_TEST_ALLOW_DESTRUCTIVE=1"):
        validate_destructive_test_database(
            DATABASE_URL,
            {NEON_TEST_ALLOWED_DATABASES_ENV: "neondb_test"},
        )


def test_destructive_tests_require_a_database_allowlist():
    with pytest.raises(DestructiveTestTargetError, match="NEON_TEST_ALLOWED_DATABASES"):
        validate_destructive_test_database(
            DATABASE_URL,
            {NEON_TEST_ALLOW_DESTRUCTIVE_ENV: "1"},
        )


def test_destructive_tests_reject_databases_outside_allowlist():
    with pytest.raises(DestructiveTestTargetError, match="not in NEON_TEST_ALLOWED_DATABASES"):
        validate_destructive_test_database(
            DATABASE_URL,
            {
                NEON_TEST_ALLOW_DESTRUCTIVE_ENV: "1",
                NEON_TEST_ALLOWED_DATABASES_ENV: "production",
            },
        )


def test_destructive_tests_reject_malformed_database_urls():
    with pytest.raises(DestructiveTestTargetError, match="PostgreSQL URL"):
        validate_destructive_test_database(
            "not-a-url",
            {
                NEON_TEST_ALLOW_DESTRUCTIVE_ENV: "1",
                NEON_TEST_ALLOWED_DATABASES_ENV: "neondb_test",
            },
        )


def test_destructive_tests_accept_an_explicitly_allowed_database():
    database_name = validate_destructive_test_database(
        DATABASE_URL,
        {
            NEON_TEST_ALLOW_DESTRUCTIVE_ENV: "1",
            NEON_TEST_ALLOWED_DATABASES_ENV: "neondb_test, another_test_database",
        },
    )

    assert database_name == "neondb_test"
