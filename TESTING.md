# Testing Guide for SQLAlchemy Neon Driver

## Overview

This package includes comprehensive integration tests that validate all major SQLAlchemy query patterns and database operations against a live Neon database.

## Test Structure

### Unit Tests (No Database Required)

These tests mock the database connection and test individual components:

- `tests/test_dbapi.py` - DBAPI module compliance tests
- `tests/test_types.py` - Type conversion tests
- `tests/test_http_client.py` - HTTP client tests
- `tests/test_dialect.py` - Dialect configuration tests

Run unit tests:
```bash
pytest tests/ -k "not integration" -v
```

### Integration Tests (Requires Live Database)

`tests/test_integration.py` contains comprehensive integration tests covering:

#### Test Models

The integration tests use a realistic blog-style schema:

- **User**: Users with profiles (JSONB), UUIDs, dates
- **Post**: Blog posts with author relationship
- **Comment**: Comments with relationships to posts and users
- **Tag**: Tags with many-to-many relationship to posts
- **Product**: Products with decimal pricing (for numeric type testing)

#### Test Coverage

**Synchronous Tests (`TestSync*` classes):**

1. **Basic CRUD** (`TestSyncBasicCRUD`)
   - Insert single/multiple records
   - Select by ID, select all
   - Update attributes
   - Delete records

2. **Query Patterns** (`TestSyncQueryPatterns`)
   - Equality/inequality filters
   - AND/OR conditions
   - IN operator
   - LIKE pattern matching
   - NULL checks
   - ORDER BY (ascending/descending)
   - LIMIT and OFFSET

3. **Aggregations** (`TestSyncAggregations`)
   - COUNT (all, with filters)
   - SUM, AVG, MIN, MAX
   - GROUP BY with aggregates

4. **Relationships** (`TestSyncRelationships`)
   - One-to-many (User → Posts)
   - Many-to-one (Post → User)
   - Many-to-many (Post ↔ Tags)
   - Nested relationships (User → Post → Comments)
   - JOIN queries

5. **Transactions** (`TestSyncTransactions`)
   - Commit operations
   - Rollback operations
   - Nested transactions with savepoints

6. **Data Types** (`TestSyncJSONB`, `TestSyncNumeric`, `TestSyncDateTime`, `TestSyncUUID`)
   - JSONB insert/update/query
   - Decimal precision
   - Date and DateTime handling
   - UUID generation and querying

**Asynchronous Tests (`TestAsync*` classes):**

Parallel async versions of the key test categories:
- Basic CRUD operations
- Query patterns with filters
- Joins and relationships
- Aggregations
- Transaction management

## Running Integration Tests

### Prerequisites

1. Set up a Neon database (free tier works fine)
2. Get your connection string from the Neon dashboard
3. Export it as an environment variable

### Setup

```bash
# Set your Neon database URL
export NEON_DATABASE_URL="postgresql://username:password@ep-xyz.us-east-1.aws.neon.tech/neondb"

# Activate virtual environment
source .venv/bin/activate

# Install test dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
# Run all integration tests
pytest tests/test_integration.py -v

# Run specific test class
pytest tests/test_integration.py::TestSyncBasicCRUD -v

# Run specific test
pytest tests/test_integration.py::TestSyncBasicCRUD::test_insert_single_user -v

# Run only sync tests
pytest tests/test_integration.py -k "Sync" -v

# Run only async tests
pytest tests/test_integration.py -k "Async" -v

# Show detailed output
pytest tests/test_integration.py -v -s
```

### Expected Results

All tests should pass with a live Neon connection:

```
tests/test_integration.py::TestSyncBasicCRUD::test_insert_single_user PASSED
tests/test_integration.py::TestSyncBasicCRUD::test_insert_multiple_users PASSED
tests/test_integration.py::TestSyncBasicCRUD::test_select_all_users PASSED
...
tests/test_integration.py::TestAsyncBasicCRUD::test_insert_and_select_async PASSED
tests/test_integration.py::TestAsyncBasicCRUD::test_update_async PASSED
...

================= XX passed in X.XXs =================
```

## Query Patterns Tested

The integration tests demonstrate authentic SQLAlchemy query patterns:

### Simple Queries
```python
# Select all
stmt = select(User)
users = session.execute(stmt).scalars().all()

# Filter by equality
stmt = select(User).where(User.username == "alice")
user = session.execute(stmt).scalar_one()

# Filter with AND
stmt = select(User).where(
    and_(User.is_active == True, User.username.like("%a%"))
)
```

### Joins and Relationships
```python
# JOIN query
stmt = (
    select(Post, User)
    .join(User, Post.author_id == User.id)
    .where(User.username == "alice")
)

# Access relationships
user.posts  # All posts by user
post.author  # Post's author
post.tags  # Many-to-many tags
```

### Aggregations
```python
# COUNT
stmt = select(func.count()).select_from(User)
count = session.execute(stmt).scalar()

# GROUP BY
stmt = (
    select(User.is_active, func.count(User.id))
    .group_by(User.is_active)
)
```

### Transactions
```python
# Auto-commit
session.add(user)
session.commit()

# Rollback
session.add(user)
session.rollback()

# Nested transactions
with session.begin_nested():
    session.add(user)
    session.rollback()
```

## Troubleshooting

### "NEON_DATABASE_URL environment variable not set"

Set the environment variable before running tests:
```bash
export NEON_DATABASE_URL="postgresql://user:pass@host.neon.tech/db"
```

### "Connection refused" or timeout errors

- Verify your connection string is correct
- Check that your Neon database is not suspended (free tier)
- Ensure your IP is allowed (check Neon dashboard)
- Try pinging the host: `ping ep-xyz.us-east-1.aws.neon.tech`

### Test failures due to existing data

The tests use `scope="module"` fixtures that clean up after themselves. If tests fail:

1. Check if there's test data left over: Connect to your DB and check for tables like `users`, `posts`, etc.
2. Manually drop test tables if needed
3. Re-run tests

### Async test failures

Ensure you have `pytest-asyncio` installed:
```bash
pip install pytest-asyncio
```

## Coverage Report

Generate a coverage report:

```bash
pytest tests/test_integration.py --cov=sqlalchemy_neon --cov-report=html --cov-report=term
```

View the HTML report:
```bash
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Continuous Integration

To run these tests in CI/CD:

1. Set `NEON_DATABASE_URL` as a secret environment variable
2. Use a dedicated test database (not production!)
3. Consider using Neon's branching feature for isolated test databases

Example GitHub Actions workflow:

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -e ".[dev]"
      - run: pytest tests/test_integration.py -v
        env:
          NEON_DATABASE_URL: ${{ secrets.NEON_DATABASE_URL }}
```

## Adding New Tests

When adding new integration tests:

1. Follow the existing naming convention (`test_*`)
2. Use appropriate fixtures (`sync_session`, `async_session`)
3. Clean up test data (fixtures handle this automatically)
4. Test both sync and async versions if applicable
5. Document what SQL pattern the test demonstrates

Example:

```python
class TestSyncNewFeature:
    """Test new feature with sync engine."""

    def test_new_query_pattern(self, sync_session: Session):
        """Test a new query pattern - demonstrates CTEs."""
        # Your test here
        pass
```
