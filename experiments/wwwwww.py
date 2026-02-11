import logfire
logfire.configure(send_to_logfire=True, service_name="neon-serverless", scrubbing=False)
# logfire.install_auto_tracing("sqlalchemy_neon.pg_protocol", min_duration=0.02)
logfire.install_auto_tracing("aiohttp.client", min_duration=0.02)
logfire.instrument_aiohttp_client(capture_all=True)
logfire.instrument_psycopg("psycopg")

from typing import AsyncGenerator

import asyncio
import uuid
import contextlib
import os
import re


import aiohttp
import rich
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
    async_scoped_session,
)
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool
from sqlalchemy_neon import create_neon_native_async_engine
from testsupport.models import Base, Comment, Post, Product, Tag, User, post_tags



NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "fffff")
neon_url = NEON_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
# neon_url = NEON_DATABASE_URL.replace("postgresql://", "postgresql+psycopg://")
neon_url_asyncpg = neon_url.rpartition("?")[0]
neon_url_asyncpg = re.sub(
    r"(\.[\w-]+\.aws\.neon\.tech)", "-pooler\\1", neon_url_asyncpg
)
print(neon_url_asyncpg)
#########################################################
engine = create_async_engine(
    neon_url_asyncpg,
    echo=True,
    pool_pre_ping=False,
    pool_recycle=300,
    pool_size=50,
    poolclass=AsyncAdaptedQueuePool,
    connect_args=dict(
        ############### asyncpg specific options ###############
        prepared_statement_name_func=lambda: f"__asyncpg_{uuid.uuid4()}__",
        # statement_cache_size=0,
        ssl=True,
        direct_tls=True,
        ############### psycopg specific options ###############
        # sslmode="require",
        # channel_binding="require",
    ),
)
logfire.instrument_sqlalchemy(engine)

ssm = async_scoped_session(
    async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    ),
    asyncio.current_task,
)


@contextlib.asynccontextmanager
async def getsession() -> AsyncGenerator[AsyncSession, None]:
    async with ssm() as session:
        yield session


async def client_session_factory() -> aiohttp.ClientSession:
    return aiohttp.ClientSession()


nengine = create_neon_native_async_engine(
    NEON_DATABASE_URL,
    http_client=client_session_factory,
    transport="websocket",
    websocket_pool_size=20,
)
#########################################################
EAGER_OPTIONS = (
    orm.joinedload(Post.author),
    orm.subqueryload(Post.tags),
    orm.subqueryload(Post.comments).selectinload(Comment.author),
)


@logfire.instrument("fetchtest_concurrent_dbsession")
async def fetchtest_concurrent_dbsession():
    """
    SQLAlchemy 2.0 introduced a new system described at Session raises proactively when illegal concurrent
    or reentrant access is detected, which proactively detects concurrent methods being invoked on an individual
    instance of the Session object and by extension the AsyncSession proxy object.

    https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
    """
    post_ids = [1, 2, 3, 4, 5]

    async def run_one(post_id: int) -> Post:
        async with getsession() as session:
            stmt = sa.select(Post).where(Post.id == post_id).options(*EAGER_OPTIONS)
            result = await session.execute(stmt)
            rich.print(f"######################## {engine.dialect = }")
            rich.print(f"######################## {engine.pool = }")
            rich.print(f"######################## {engine.pool.status() = }")
            post = result.unique().scalar_one()
            rich.print(
                f"Fetched post {post.id} titled '{post.title}' by {post.author.username}"
            )
            rich.print(f"Comments: {len(post.comments)} {post.comments}")

        return post

    futures = []
    async with asyncio.TaskGroup() as tg:
        for post_id in post_ids:
            future = tg.create_task(run_one(post_id))
            futures.append(future)
    results = [future.result() for future in futures]
    rich.print(results)


@logfire.instrument("fetchtest_native_async_engine")
async def fetchtest_native_async_engine():
    post_ids = [1, 2, 3, 4, 5]
    # post_ids = [1, 2]

    async def run_one(post_id: int) -> None:

        stmt = sa.select(Post).where(Post.id == post_id).options(*EAGER_OPTIONS)
        result = await nengine.execute(stmt)
        post = result.unique().scalar_one()
        rich.print(
            f"Fetched post {post.id} titled '{post.title}' by {post.author.username}"
        )
        rich.print(f"Comments: {len(post.comments)} {post.comments}")

        return post

    futures = []
    async with asyncio.TaskGroup() as tg:
        for post_id in post_ids:
            future = tg.create_task(run_one(post_id))
            futures.append(future)
    results = [future.result() for future in futures]
    rich.print(repr(results))


async def main():
    try:
        await fetchtest_concurrent_dbsession()
        await fetchtest_concurrent_dbsession()
        await fetchtest_concurrent_dbsession()

        await fetchtest_native_async_engine()
        await fetchtest_native_async_engine()
        await fetchtest_native_async_engine()

        ########################################
    finally:
        await engine.dispose()
        await nengine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
