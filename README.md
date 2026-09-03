# SQLAlchemy Neon Driver

A native async SQLAlchemy execution layer for Neon serverless PostgreSQL over HTTP.

## Scope

This package is a native async SQLAlchemy statement executor for Neon
serverless PostgreSQL over HTTP or WebSocket. It is not a SQLAlchemy dialect and
does not provide `Session` or `AsyncSession` behavior.

- **Native async execution**: Execute SQLAlchemy Core statements directly.
- **Partial ORM support**: Hydrate one bare mapped entity and supported
  relationship loader strategies.
- **HTTP/WebSocket transports**: Use HTTP for request execution or WebSocket for
  PostgreSQL protocol execution.
- **Type conversion**: Convert supported PostgreSQL values through psycopg;
  unknown OIDs remain raw values.
- **Batch operations**: Execute explicit statement batches through
  `engine.transaction(...)` and object helpers such as `add_all(...)`.

## Installation

```bash
pip install sqlalchemy-neon
```

Or install from source:

```bash
git clone https://github.com/duodecanol/sqlalchemy-neon.git
cd sqlalchemy-neon
python -m pip install -e .
```

## Usage

### Native Async Usage

```python
import sqlalchemy as sa
from sqlalchemy_neon import create_neon_native_async_engine

engine = create_neon_native_async_engine(
    "postgresql://user:pass@host.neon.tech:5432/db"
)

async def main():
    result = await engine.execute(
        sa.text("SELECT now() AS ts")
    )
    print(result.scalar_one())

    await engine.dispose()
```
Single ORM column or expression selections remain positional scalar results.

### Native ORM result contract

The native engine hydrates one bare mapped entity per statement:

```python
result = await engine.execute(sa.select(User))
user = result.scalar_one()
```

Multi-entity statements such as `sa.select(User, Post)` and entity-plus-column
statements such as `sa.select(User, Post.title)` raise `NotSupportedError` before
execution. Select Core columns explicitly or issue separate queries when a
multi-value result is required. Object-form responses with duplicate field names
are also rejected instead of being normalized into potentially incorrect values.

Relationship loader strategy support:

- `selectinload` and `subqueryload` issue separate relationship queries.
- `joinedload` is accepted as a separate-query equivalent; it does not add a SQL
  join to the root query.
- `noload` initializes the relationship as empty/`None` without querying it.
- `lazyload` and `raiseload` are rejected with `NotSupportedError`.

## Connection String Format

The connection URL may include PostgreSQL or Neon query parameters, such as
`sslmode` or `options`. The driver preserves those parameters. Driver configuration
in the URL is ignored, removed before the connection string is forwarded, and emits
a `UserWarning`; use Python keyword arguments when the setting must take effect.

```text
postgresql://username:password@host:port/database?sslmode=require
```

Percent-encode reserved characters in the username, password, and database name.
WebSocket startup decodes those URI components before PostgreSQL authentication;
the HTTP transport preserves the encoded connection URL.

### Driver keyword-only parameters

- `auth_token`: JWT token for Row-Level Security on the HTTP transport. WebSocket transport ignores it and emits a `UserWarning`.
- `timeout`: Request timeout in seconds (default: 30)
- `transport`: `"http"` (default) or `"websocket"`
- `websocket_pool_size`: Max pooled WS connections when `transport="websocket"`
- `fetch_endpoint`: Override Neon HTTP endpoint URL (or provide resolver callable)
- `fetch_function`: Inject custom async HTTP transport callable

These options are ignored when present in the connection URL: `auth_token`,
`timeout`, `transport`, `websocket_pool_size`, `fetch_endpoint`, or
`fetch_function`. The driver emits a `UserWarning` and suppresses them.

Example with parameters:

```python
engine = create_neon_native_async_engine(
    "postgresql://user:pass@host.neon.tech/db",
    auth_token="your_jwt_token",
    timeout=60,
)

# WebSocket transport + pooled connections
# WebSocket transport ignores auth_token and emits a UserWarning. Use HTTP for JWT/RLS access.
ws_engine = create_neon_native_async_engine(
    "postgresql://user:pass@host.neon.tech/db",
    transport="websocket",
    websocket_pool_size=10,
)
```

## Development

### Setup Development Environment

The repository uses uv dependency groups. From a clean checkout:

```bash
git clone https://github.com/duodecanol/sqlalchemy-neon.git
cd sqlalchemy-neon
uv sync --group dev
```

### Development Telemetry (Optional)

Logfire is development-only and opt-in. It is not imported by the runtime
package and is not included in the published wheel's dependencies.

```bash
ENABLE_TEST_TELEMETRY=1 LOGFIRE_TOKEN=<write-token> \
  uv run --group dev pytest -o addopts="--logfire" tests/units -q
```

Request and response headers, bodies, PostgreSQL frames, SQL parameters, and
query results are never captured or exported by the test telemetry setup.

### Running Tests

Unit tests do not require a database:

```bash
ENABLE_TEST_TELEMETRY=0 LOGFIRE_TOKEN= \
  uv run --group dev pytest tests/units -q
```

The pull-request CI is credential-free: `.github/workflows/tests.yml` runs
offline tests, packaging, and production quality checks; the separate
`.github/workflows/secret-scan.yml` checks secrets. Live integration uses the
protected manual/scheduled workflow described in `TESTING.md`.

### Test Coverage (Optional)

```bash
uv run --with pytest-cov pytest tests/units -q \
  --cov=sqlalchemy_neon --cov-report=term-missing
```

## Architecture

### Components

1. **Native Async Engine** (`native_async_engine.py`)
   - Statement compilation and execution without SQLAlchemy async proxying
   - SQLAlchemy-style `Result` API support
   - Strategy-aware ORM relationship hydration

2. **HTTP Client** (`neon_http_client.py`): HTTP communication layer
   - Query execution via Neon HTTP API
   - Transaction batching
   - Async/await support

3. **Type Conversion** (`types.py`): PostgreSQL ↔ Python type mapping
   - Uses psycopg's type system
   - Text-based serialization over HTTP
   - OID-based type identification

### How It Works

The native engine compiles SQLAlchemy statements to PostgreSQL SQL + bind parameters,
executes them over Neon HTTP, and then maps results back into SQLAlchemy-compatible
result objects and ORM entities.

## Capability Matrix

| Area | Contract |
| --- | --- |
| Core statements | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, parameters, and result wrappers are supported. |
| Native ORM | Partial: one bare mapped entity, object helpers, and supported relationship loaders. |
| ORM result shapes | Multi-entity and entity-plus-column results raise `NotSupportedError`. |
| Relationships | `selectinload`, `subqueryload`, `joinedload` (separate-query equivalent), and `noload` are supported. `lazyload` and `raiseload` are rejected. |
| Transactions | Explicit statement batches through `engine.transaction(...)`; `add`/`add_all`/`delete`/`delete_all` use a write transaction. |
| Type conversion | Supported PostgreSQL types use psycopg processors; unknown OIDs remain raw values. |
| Dialect and sessions | No SQLAlchemy dialect registration, `Session`, or `AsyncSession` implementation. |
| Stateless-server features | Server-side cursors, `LISTEN`/`NOTIFY`, `COPY`, and two-phase commit are unsupported. |

## Troubleshooting

### Connection Errors

If you get connection errors, verify:

1. Your Neon connection string is correct
2. The database endpoint is accessible
3. SSL mode is appropriate (usually `require` for Neon)

### Transaction Scope

The native engine does not expose `Session.commit()`, `Session.rollback()`,
savepoints, or autocommit mode. Use `engine.transaction(...)` for an explicit
statement batch and `TransactionOptions` for isolation, read-only, and
deferrable settings.

### Type Conversion Errors

The driver uses psycopg processors for supported PostgreSQL types. Unknown OIDs
are returned as raw values; malformed values for known types raise a safe
conversion error.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Credits

This driver builds on:

- [SQLAlchemy](https://www.sqlalchemy.org/) - The Python SQL toolkit and ORM
- [psycopg](https://www.psycopg.org/) - PostgreSQL adapter for Python
- [Neon](https://neon.tech/) - Serverless PostgreSQL

## Links

- [Neon Documentation](https://neon.tech/docs)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [GitHub Repository](https://github.com/duodecanol/sqlalchemy-neon)
