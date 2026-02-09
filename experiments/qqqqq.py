import logfire
import re
import aiohttp
import asyncio
import contextlib
import os
from typing import AsyncGenerator

import aiohttp
import logfire
import rich
import sqlalchemy as sa
from models import Comment, Post, Tag, User
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    create_async_pool_from_url,
    async_sessionmaker,
)

from sqlalchemy_neon import (
    PoolQuery,
    create_neon_native_async_engine,
    execute_pool_queries,
)

logfire.configure(send_to_logfire=True, service_name="neon-serverless", scrubbing=False)
logfire.instrument_aiohttp_client(capture_all=True)

NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")
neon_url = NEON_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
neon_url_asyncpg = neon_url.rpartition("?")[0]
neon_url_asyncpg = re.sub(
    r"(\.[\w-]+\.aws\.neon\.tech)", "-pooler\\1", neon_url_asyncpg
)
print(neon_url_asyncpg)
engine = create_async_engine(
    neon_url_asyncpg,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=20,
    connect_args=dict(
        # sslmode="require",
        # channel_binding="require",
    )
)
logfire.instrument_sqlalchemy(engine)

ssm = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


@contextlib.asynccontextmanager
async def getsession() -> AsyncGenerator[sa.ext.asyncio.AsyncSession, None]:
    async with ssm() as session:
        yield session


async def client_session_factory() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        # conn_timeout=8.0,
        # read_timeout=60.0,
        # connector=aiohttp.TCPConnector(
        #     limit=210,
        # ),
    )


nengine = create_neon_native_async_engine(
    NEON_DATABASE_URL,
    http_client=client_session_factory,
)

EAGER_OPTIONS = (
    orm.joinedload(Post.author),
    orm.subqueryload(Post.tags),
    orm.subqueryload(Post.comments).selectinload(Comment.author),
)


@logfire.instrument("add_all_inspect")
async def add_all_inspect():
    unique_prefix = "test-xxxxxx-yyy-"
    users = [
        User(
            username=f"{unique_prefix}_user_{i}",
            email=f"{unique_prefix}_user_{i}@example.com",
            full_name=f"Advanced User {i}",
        )
        for i in range(55)
    ]
    tags = [Tag(name=f"{unique_prefix}_tag_{i}") for i in range(240)]
    # posts = []
    # for i in range(10):
    #     author = users[i % 5]
    #     post_tags = [tags[i % 5], tags[(i + 1) % 5]]
    #     post = Post(
    #         title=f"{unique_prefix}_post_{i}",
    #         content=f"Complex content for post {i}",
    #         author=author,
    #         published=True,
    #         tags=post_tags,
    #     )
    #     posts.append(post)

    to_add = users + tags
    async with getsession() as session:
        await session.add_all_cheat(to_add)

        # await session.bulk_save_objects(users)
        await session.flush()


@logfire.instrument("fetchtest_concurrent_dbsession")
async def fetchtest_concurrent_dbsession():
    """
    SQLAlchemy 2.0 introduced a new system described at Session raises proactively when illegal concurrent
    or reentrant access is detected, which proactively detects concurrent methods being invoked on an individual
    instance of the Session object and by extension the AsyncSession proxy object.

    https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
    """
    post_ids = [1, 2, 3, 4, 5]

    async def run_one(post_id: int) -> None:
        async with getsession() as session:
            stmt = sa.select(Post).where(Post.id == post_id).options(*EAGER_OPTIONS)
            result = await session.execute(stmt)
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
    # await add_all_inspect()
    with logfire.span("main_all_tests"):
        await fetchtest_concurrent_dbsession()
        await fetchtest_concurrent_dbsession()
        # await fetchtest_concurrent_conns()
        await fetchtest_native_async_engine()
        await fetchtest_native_async_engine()

    ########################################
    await engine.dispose()
    await nengine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
