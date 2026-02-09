# SQLAlchemy Neon Driver

A SQLAlchemy dialect for Neon serverless PostgreSQL over HTTP.

## Features

- **Serverless HTTP connectivity**: Connect to Neon databases via HTTP instead of traditional TCP
- **Full SQLAlchemy ORM support**: Use SQLAlchemy's powerful ORM with Neon's serverless PostgreSQL
- **Both sync and async**: Supports both synchronous and asynchronous operations
- **Transaction support**: Buffered transactions sent as batch operations
- **Type conversions**: Complete PostgreSQL type support including JSON, UUID, arrays, and more
- **Connection pooling**: Uses appropriate pooling strategies for stateless HTTP connections

## Installation

```bash
pip install sqlalchemy-neon
```

Or install from source:

```bash
git clone https://github.com/yourusername/sqlalchemy-neon-driver.git
cd sqlalchemy-neon-driver
pip install -e .
```

## Usage

### Synchronous Usage

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column

# Create engine with neonhttp dialect
engine = create_engine("postgresql+neonhttp://user:pass@host.neon.tech:5432/db")

# Define your models
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str]
    email: Mapped[str]

# Create tables
Base.metadata.create_all(engine)

# Use the session
with Session(engine) as session:
    user = User(username="alice", email="alice@example.com")
    session.add(user)
    session.commit()

    # Query
    stmt = select(User).where(User.username == "alice")
    result = session.execute(stmt).scalar_one()
    print(f"User: {result.username}, Email: {result.email}")
```

### Asynchronous Usage

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import select

# Create async engine
engine = create_async_engine("postgresql+neonhttp://user:pass@host.neon.tech:5432/db")

async def main():
    async with AsyncSession(engine) as session:
        user = User(username="bob", email="bob@example.com")
        session.add(user)
        await session.commit()

        # Query
        stmt = select(User).where(User.username == "bob")
        result = await session.execute(stmt)
        user = result.scalar_one()
        print(f"User: {user.username}")

import asyncio
asyncio.run(main())
```

### Native Async Usage (No SQLAlchemy AsyncEngine Proxy)

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

## Connection String Format

The connection string format is:

```
postgresql+neonhttp://username:password@host:port/database?param=value
```

### Optional Parameters

- `auth_token`: JWT token for Row-Level Security
- `timeout`: Request timeout in seconds (default: 30)
- `sslmode`: SSL mode (same as PostgreSQL)

Example with parameters:

```python
engine = create_engine(
    "postgresql+neonhttp://user:pass@host.neon.tech/db"
    "?auth_token=your_jwt_token&timeout=60"
)
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/sqlalchemy-neon-driver.git
cd sqlalchemy-neon-driver

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

#### Unit Tests

Run unit tests without requiring a live database:

```bash
pytest tests/test_dbapi.py tests/test_types.py tests/test_http_client.py
```

#### Integration Tests

Integration tests require a live Neon database connection. Set the `NEON_DATABASE_URL` environment variable:

```bash
export NEON_DATABASE_URL="postgresql://user:pass@host.neon.tech:5432/db"
pytest tests/test_integration.py -v
```

The integration tests cover:

- **Basic CRUD operations**: Create, Read, Update, Delete
- **Query patterns**: Filtering, joins, aggregations, ordering, pagination
- **Relationships**: One-to-many, many-to-one, many-to-many
- **Transactions**: Commit, rollback, nested transactions
- **Data types**: JSONB, UUID, Decimal, Date/DateTime, arrays
- **Both sync and async**: Full coverage for both dialects

#### Run All Tests

```bash
# Unit tests only (no database required)
pytest tests/ -k "not integration"

# All tests (requires NEON_DATABASE_URL)
export NEON_DATABASE_URL="postgresql://user:pass@host.neon.tech/5432/db"
pytest tests/ -v
```

### Test Coverage

To run tests with coverage:

```bash
pip install pytest-cov
pytest tests/ --cov=sqlalchemy_neon --cov-report=html
```

View the coverage report at `htmlcov/index.html`.

## Architecture

### Components

1. **Dialect** (`dialect.py`): SQLAlchemy dialect implementation
   - `NeonHTTPDialect`: Synchronous dialect
   - `NeonHTTPDialect_async`: Asynchronous dialect

2. **DBAPI** (`dbapi.py`, `dbapi_async.py`): PEP 249-compliant database API
   - Connection and Cursor classes
   - Transaction buffering for stateless HTTP
   - Parameter conversion (pyformat → numeric)

3. **HTTP Client** (`neon_http_client.py`): HTTP communication layer
   - Query execution via Neon HTTP API
   - Transaction batching
   - Async/await support

4. **Type Conversion** (`types.py`): PostgreSQL ↔ Python type mapping
   - Uses psycopg's type system
   - Text-based serialization over HTTP
   - OID-based type identification

### How It Works

Since Neon's HTTP API is stateless, this driver implements transactions by:

1. Buffering SQL statements when `autocommit=False`
2. Sending all buffered statements in a single batch on `commit()`
3. Rolling back by simply discarding the buffer

This allows SQLAlchemy's transaction semantics to work over HTTP while maintaining efficiency.

## Supported Features

### ✅ Supported

- All basic SQL operations (SELECT, INSERT, UPDATE, DELETE)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Prepared statements with parameter binding
- All PostgreSQL data types via psycopg
- SQLAlchemy ORM (declarative, relationships, etc.)
- Async support via SQLAlchemy's async engine
- Connection pooling (NullPool for sync, AsyncAdaptedQueuePool for async)

### ❌ Not Supported

- Server-side cursors (stateless HTTP)
- LISTEN/NOTIFY (requires persistent connection)
- COPY commands (may be added in future)
- Two-phase commit (XA transactions)

## Troubleshooting

### Connection Errors

If you get connection errors, verify:

1. Your Neon connection string is correct
2. The database endpoint is accessible
3. SSL mode is appropriate (usually `require` for Neon)

### Transaction Issues

Remember that transactions are buffered and sent on commit. If you need immediate execution:

1. Use `autocommit=True` mode
2. Or call `commit()` after each statement

### Type Conversion Errors

The driver uses psycopg's type system. If you encounter type conversion issues:

1. Ensure your Python types match the PostgreSQL column types
2. Use SQLAlchemy's type annotations for clarity
3. Check that psycopg supports the PostgreSQL type

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
- [GitHub Repository](https://github.com/yourusername/sqlalchemy-neon-driver)
