# SQLAlchemy Neon Driver

A native async SQLAlchemy execution layer for Neon serverless PostgreSQL over HTTP.

## Features

- **Serverless HTTP connectivity**: Connect to Neon databases via HTTP instead of traditional TCP
- **Full SQLAlchemy ORM support**: Use SQLAlchemy's powerful ORM with Neon's serverless PostgreSQL
- **Native async engine**: Execute SQLAlchemy Core/ORM statements without AsyncEngine sync-proxying
- **Relationship loading**: Strategy-aware eager loading support for ORM options
- **Type conversions**: Complete PostgreSQL type support including JSON, UUID, arrays, and more
- **Batch utilities**: Run concurrent query workloads with helper utilities

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

## Connection String Format

The connection string format is:

```
postgresql://username:password@host:port/database?param=value
```

### Optional Parameters

- `auth_token`: JWT token for Row-Level Security
- `timeout`: Request timeout in seconds (default: 30)
- `sslmode`: SSL mode (same as PostgreSQL)

Example with parameters:

```python
engine = create_neon_native_async_engine(
    "postgresql://user:pass@host.neon.tech/db"
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
pytest tests/test_native_async_engine.py tests/test_types.py tests/test_http_client.py
```

#### Run All Tests

```bash
# Test suite (no database required)
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

## Supported Features

### ✅ Supported

- All basic SQL operations (SELECT, INSERT, UPDATE, DELETE)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Prepared statements with parameter binding
- All PostgreSQL data types via psycopg
- SQLAlchemy ORM (declarative, relationships, etc.)
- Native async support via `NeonNativeAsyncEngine`
- Strategy-aware relationship loading for ORM options

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
