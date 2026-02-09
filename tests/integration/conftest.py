from datetime import date
import re
import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncEngine,
    AsyncSession,
)

from typing import AsyncGenerator

import aiohttp
from sqlalchemy_neon import create_neon_native_async_engine, NeonNativeAsyncEngine

from testsupport.models import User, Base

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
async def neondb(
    require_neon, object_propagator
) -> AsyncGenerator[NeonNativeAsyncEngine, None]:
    """Create an async engine connected to Neon."""

    async def client_factory():
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
        )

    engine = create_neon_native_async_engine(
        require_neon,
        # http_client=aiohttp.ClientSession(
        #     timeout=aiohttp.ClientTimeout(total=15),
        # ),
        http_client=client_factory,
    )

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def asyncpg_engine(require_neon, object_propagator):
    """Create an async engine connected to Neon.

    Uses the neonhttp_async dialect which wraps the synchronous
    geventhttpclient-based HTTP client with SQLAlchemy's greenlet
    async adapter.
    """
    neon_url = require_neon.replace("postgresql://", "postgresql+asyncpg://")
    neon_url = neon_url.replace(".aws.neon.tech", "-pooler.aws.neon.tech")
    neon_url = neon_url.rpartition("?")[0]
    neon_url = re.sub(r"(\.[\w-]+\.aws\.neon\.tech)", "-pooler\\1", neon_url)
    engine = create_async_engine(
        neon_url,
        echo=False,
        future=True,
        expire_on_commit=False,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=20,
    )

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def asyncpg_session(
    asyncpg_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    """Create a new async session for each test.

    Uses SQLAlchemy's AsyncSession which internally uses greenlet-based
    context switching to bridge the async interface with the synchronous
    HTTP client.
    """
    sessionmaker = async_sessionmaker(
        asyncpg_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
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
