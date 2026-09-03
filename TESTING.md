# Testing Guide for SQLAlchemy Neon Driver

This repository tests the native async statement executor against mocked
transports and, when explicitly configured, a protected live Neon database.
It is not a SQLAlchemy dialect or `Session` implementation.

> [!WARNING]
> Integration fixtures run `drop_all()` and `create_all()` against the target
> database's `public` schema. Use a dedicated, disposable Neon branch or database
> only. The suite refuses DDL unless its test-only URL, an explicit destructive
> opt-in, and an exact database-name allowlist are all configured.

## Credentials and Safety

Keep local credentials in ignored files such as `.envrc`; never commit, paste,
or print them. The `Secret scan` workflow checks pushes and pull requests.
The integration database is intentionally not provisioned by this repository.
Do not use production credentials or rotate external credentials as part of a
local test run.

## CI Workflows

- **Offline checks** (`.github/workflows/tests.yml`) run on every pull request
  and push. They run `uv build`, production-source Ruff checks, a mypy check
  for the SQLAlchemy compatibility boundary, and the offline unit suite across
  Python 3.11/3.12 and SQLAlchemy 2.0.0/2.0.46. They do not receive database
  credentials.
- **Secret scan** (`.github/workflows/secret-scan.yml`) runs on every pull
  request and push.
- **Protected integration** (`.github/workflows/integration.yml`) runs only
  from the canonical repository by manual dispatch or the scheduled
  main-branch trigger. It requires approval for the `neon-integration`
  environment and receives only its configured protected secrets. The
  environment must point to a disposable Neon branch/database; the workflow
  does not provision or target a shared production database. The test safety
  fixture still requires the exact database allowlist before issuing DDL.

The protected integration command is:

```bash
uv run --group dev pytest tests/integration -v
```

## Test Structure

### Unit Tests (No Database Required)

Unit tests use fake HTTP/WebSocket clients and protocol fixtures:

- `tests/units/test_native_async_engine.py`
- `tests/units/test_types.py`
- `tests/units/test_http_client.py`
- `tests/units/test_pg_protocol.py`
- `tests/units/test_neon_test_safety.py`

Run them from a synced checkout:

```bash
ENABLE_TEST_TELEMETRY=0 LOGFIRE_TOKEN= \
  uv run --group dev pytest tests/units -q
```

### Integration Tests (Protected Live Database)

Integration tests require a dedicated disposable Neon database. They use
`NeonNativeAsyncEngine` directly; they do not test `Session` or
`AsyncSession` semantics. The current integration tree contains:

- `tests/integration/test_integration_basics.py`: async CRUD, filters,
  aggregates, relationships, and type coverage.
- `tests/integration/test_integration_hard.py`: nested loader options and
  concurrent native queries.
- `tests/integration/test_pipeline.py`: WebSocket protocol integration.

The integration models cover users, posts, comments, tags, products, and
complex JSON/type values. DDL is destructive and guarded by the environment
variables below.

## Running Integration Tests

### Prerequisites

1. Provision a dedicated disposable Neon database.
2. Ensure its database name is known for the exact allowlist.
3. Use credentials only in local environment variables or ignored files.

### Setup

From the repository root:

```bash
uv sync --group dev
export NEON_TEST_DATABASE_URL="postgresql://username:password@ep-xyz.us-east-1.aws.neon.tech/neondb_test"
export NEON_TEST_ALLOWED_DATABASES="neondb_test"
export NEON_TEST_ALLOW_DESTRUCTIVE=1
```

### Run Tests

```bash
uv run --group dev pytest tests/integration -v
```

Run a specific class or test:

```bash
uv run --group dev pytest \
  tests/integration/test_integration_basics.py::TestAsyncBasicCRUD -v
uv run --group dev pytest \
  tests/integration/test_integration_basics.py::TestAsyncBasicCRUD::test_insert_single_user -v
```

Run with detailed output:

```bash
uv run --group dev pytest tests/integration -v -s
```

Without all three `NEON_TEST_*` variables, destructive integration fixtures
skip or fail closed before issuing DDL. A successful run reports the ordinary
pytest pass/skip summary.

## Query Patterns Tested

The integration suite uses `NeonNativeAsyncEngine` directly:

```python
import sqlalchemy as sa

result = await neondb.execute(
    sa.text("SELECT username FROM users WHERE is_active = :active"),
    {"active": True},
)
active_names = result.scalars().all()
```

For one mapped entity:

```python
stmt = sa.select(User).where(User.username == "alice")
result = await neondb.execute(stmt)
user = result.scalar_one()
```

Supported relationship options include `selectinload`, `subqueryload`,
`joinedload` (implemented as a separate native relationship query), and
`noload`. Multi-entity and entity-plus-column ORM selections, `lazyload`, and
`raiseload` are explicitly unsupported and raise `NotSupportedError`.

Use Core columns for multi-value results:

```python
stmt = sa.select(User.username, sa.func.count(User.id)).group_by(User.username)
result = await neondb.execute(stmt)
rows = result.all()
```

Native transaction batches are explicit:

```python
results = await neondb.transaction(
    [
        (sa.text("UPDATE users SET is_active = :active WHERE id = :id"), {
            "active": False,
            "id": user.id,
        }),
    ],
)
```

`engine.transaction(...)` commits only after every statement succeeds. A
server error, transport error, or cancellation attempts `ROLLBACK`, quarantines
the affected transport, and re-raises the original exception. The native
engine does not provide `Session.commit()`, rollback methods, savepoints, or
autocommit mode; savepoints are unsupported.

## Troubleshooting

### "NEON_TEST_DATABASE_URL environment variable not set"

Set a dedicated test database URL before running live tests:
```bash
export NEON_TEST_DATABASE_URL="postgresql://user:pass@host.neon.tech/neondb_test"
export NEON_TEST_ALLOWED_DATABASES="neondb_test"
export NEON_TEST_ALLOW_DESTRUCTIVE=1
```

### "Database ... is not in NEON_TEST_ALLOWED_DATABASES"

The fixture failed before issuing DDL. Verify that the database name in
`NEON_TEST_DATABASE_URL` exactly matches the dedicated test database listed in
`NEON_TEST_ALLOWED_DATABASES`; do not add a shared, staging, or production
database to the allowlist.

### "Connection refused" or timeout errors

- Verify your connection string is correct
- Check that your Neon database is not suspended (free tier)
- Ensure your IP is allowed (check Neon dashboard)
- Try pinging the host: `ping ep-xyz.us-east-1.aws.neon.tech`

### Test failures due to existing data

The tests use `scope="module"` fixtures that clean up after themselves. If tests fail:

1. Check for leftover data only in the dedicated test database.
2. Manually clean up only that disposable test database if needed.
3. Re-run tests.

### Missing development dependencies

Synchronize the configured development group from the repository root:

```bash
uv sync --group dev
```

## Coverage Report

Generate a unit-test coverage report without modifying project dependencies:

```bash
uv run --with pytest-cov pytest tests/units -q \
  --cov=sqlalchemy_neon --cov-report=term-missing
```

## Continuous Integration

The repository currently runs the secret scan workflow on pushes and pull
requests. Run the offline unit lane with:

```bash
uv sync --group dev
ENABLE_TEST_TELEMETRY=0 LOGFIRE_TOKEN= \
  uv run --group dev pytest tests/units -q
```

If a protected integration lane is configured, provide
`NEON_TEST_DATABASE_URL`, `NEON_TEST_ALLOWED_DATABASES`, and
`NEON_TEST_ALLOW_DESTRUCTIVE=1` only to that job. It must use a dedicated
disposable database and the integration command above.

## Adding New Tests

Use the existing `test_*` naming convention and keep tests aligned with the
native executor contract:

1. Put isolated component tests under `tests/units/` with fake transports.
2. Put live database tests under `tests/integration/`.
3. Use the `neondb` fixture for native async integration tests.
4. Add behavior-level coverage for supported or explicitly rejected semantics.
5. Keep destructive setup and cleanup inside the protected integration fixtures.
