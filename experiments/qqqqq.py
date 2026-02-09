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
)

from sqlalchemy_neon import (
    NeonAsyncSession,
    PoolQuery,
    create_neon_native_async_engine,
    execute_pool_queries,
)

logfire.configure(send_to_logfire=True, service_name="neon-serverless", scrubbing=False)
logfire.instrument_httpx(capture_all=True)
logfire.instrument_psycopg()
logfire.instrument_aiohttp_client(capture_all=True)

NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")
neon_url = NEON_DATABASE_URL.replace("postgresql://", "postgresql+neonhttp_async://")
engine = create_async_engine(
    neon_url,
    # isolation_level="SERIALIZABLE",
    echo=False,
    connect_args={
        "expire_on_commit": False,
        # "http_client": http_client,
    },
)


async def client_session_factory() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        conn_timeout=8.0,
        read_timeout=60.0,
        connector=aiohttp.TCPConnector(
            limit=210,
        ),
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


@contextlib.asynccontextmanager
async def getsession() -> AsyncGenerator[NeonAsyncSession, None]:
    async with NeonAsyncSession(engine, expire_on_commit=False) as session:
        yield session


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

        |     conn = self._connection_for_bind(bind)
    |   File "/home/duodecanol/xxDev/prototypes/sqlalchemy-neon-driver/.venv/lib/python3.13/site-packages/sqlalchemy/orm/session.py", line 2108, in _connection_for_bind
    |     return trans._connection_for_bind(engine, execution_options)
    |            ~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |   File "<string>", line 2, in _connection_for_bind
    |   File "/home/duodecanol/xxDev/prototypes/sqlalchemy-neon-driver/.venv/lib/python3.13/site-packages/sqlalchemy/orm/state_changes.py", line 101, in _go
    |     self._raise_for_prerequisite_state(fn.__name__, current_state)
    |     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |   File "/home/duodecanol/xxDev/prototypes/sqlalchemy-neon-driver/.venv/lib/python3.13/site-packages/sqlalchemy/orm/session.py", line 988, in _raise_for_prerequisite_state
    |     raise sa_exc.InvalidRequestError(
    |     ...<3 lines>...
    |     )
    | sqlalchemy.exc.InvalidRequestError: This session is provisioning a new connection; concurrent operations are not permitted (Background on this error at: https://sqlalche.me/e/20/isce)
    """
    post_ids = [1, 2, 3, 4, 5]

    async def run_one(post_id: int) -> None:
        async with getsession() as session:
            stmt = sa.select(Post).where(Post.id == post_id).options(*EAGER_OPTIONS)
            result = await session.execute(stmt)
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


@logfire.instrument("fetchtest_concurrent_conns")
async def fetchtest_concurrent_conns():
    post_ids = [1, 2, 3, 4, 5]

    queries = [
        PoolQuery(
            sa.select(Post.id, Post.title, User.username)
            .join(User, Post.author_id == User.id)
            .where(Post.id == post_id)
        )
        for post_id in post_ids
    ]
    results = await execute_pool_queries(engine, queries, max_concurrency=5)
    for result in results:
        if not result.rows:
            continue
        post_id, post_title, author_username = result.rows[0]
        rich.print(
            f"Fetched post {post_id} titled '{post_title}' by {author_username}"
        )


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
    await fetchtest_concurrent_dbsession()
    # await fetchtest_concurrent_conns()
    await fetchtest_native_async_engine()

    ########################################
    await engine.dispose()
    await nengine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
