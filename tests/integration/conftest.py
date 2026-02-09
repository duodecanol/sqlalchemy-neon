from datetime import date

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    AsyncSession,
)

from sqlalchemy_neon import NeonAsyncSession

from tests.integration.models import User, Base

import logfire

# ============================================================================
# Pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def object_propagator(require_neon):
    from sqlalchemy import create_engine

    with logfire.span("Pytest: object_propagator"):
        neon_url = require_neon.replace("postgresql://", "postgresql+psycopg://")
        engine = create_engine(neon_url, echo=True)

        # Create all tables
        with engine.begin():
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)

        yield engine

        engine.dispose()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def async_engine(require_neon, object_propagator):
    """Create an async engine connected to Neon.

    Uses the neonhttp_async dialect which wraps the synchronous
    geventhttpclient-based HTTP client with SQLAlchemy's greenlet
    async adapter.
    """
    neon_url = require_neon.replace("postgresql://", "postgresql+neonhttp_async://")
    engine = create_async_engine(
        neon_url,
        isolation_level="AUTOCOMMIT",
        echo=False,
        expire_on_commit=False,
        connect_args={
            # "http_client": http_client,
        },
    )

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def async_session(async_engine: AsyncEngine) -> NeonAsyncSession:
    """Create a new async session for each test.

    Uses SQLAlchemy's AsyncSession which internally uses greenlet-based
    context switching to bridge the async interface with the synchronous
    HTTP client.
    """
    sessionmaker = sa.async_sessionmaker(
        async_engine,
        class_=NeonAsyncSession,
        expire_on_commit=False,
    )
    async with sessionmaker() as sess:
        yield sess

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def asyncpg_engine(require_neon, object_propagator):
    """Create an async engine connected to Neon.

    Uses the neonhttp_async dialect which wraps the synchronous
    geventhttpclient-based HTTP client with SQLAlchemy's greenlet
    async adapter.
    """
    neon_url = require_neon.replace("postgresql://", "postgresql+asyncpg://")
    neon_url = neon_url.replace(".aws.neon.tech", "-pooler.aws.neon.tech")
    engine = create_async_engine(
        neon_url,
        echo=False,
        future=True,
        expire_on_commit=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def asyncpg_session(asyncpg_engine: AsyncEngine) -> AsyncSession:
    """Create a new async session for each test.

    Uses SQLAlchemy's AsyncSession which internally uses greenlet-based
    context switching to bridge the async interface with the synchronous
    HTTP client.
    """
    sessionmaker = sa.async_sessionmaker(
        asyncpg_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with sessionmaker() as sess:
        yield sess


@pytest.fixture
def sample_users() -> list[User]:
    """Provide sample user data for testing."""
    return [
        User(
            username="alice",
            email="alice@example.com",
            full_name="Alice Johnson",
            is_active=True,
            birth_date=date(1990, 5, 15),
            profile={"role": "admin", "preferences": {"theme": "dark"}},
        ),
        User(
            username="bob",
            email="bob@example.com",
            full_name="Bob Smith",
            is_active=True,
            birth_date=date(1985, 8, 22),
            profile={"role": "user", "preferences": {"theme": "light"}},
        ),
        User(
            username="charlie",
            email="charlie@example.com",
            full_name="Charlie Brown",
            is_active=False,
            birth_date=date(1995, 12, 1),
            profile={"role": "user", "preferences": {"theme": "auto"}},
        ),
    ]
