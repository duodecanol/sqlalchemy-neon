"""
Hard integration tests for SQLAlchemy Neon driver.
Focuses on complex fetch patterns and parallel execution.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.orm import joinedload, selectinload, subqueryload
from sqlalchemy_neon import NeonNativeAsyncEngine

import logfire
from testsupport.models import Comment, Post, Tag, User, post_tags


def get_unique_suffix() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def unique_prefix() -> str:
    return f"hard_{get_unique_suffix()}"


@pytest_asyncio.fixture(scope="module")
@logfire.instrument("Pytest: seeded_data", new_trace=True)
async def seeded_data(neondb: NeonNativeAsyncEngine, unique_prefix: str):
    """Seed the database once for all tests in this module."""
    users = [
        User(
            username=f"{unique_prefix}_user_{i}",
            email=f"{unique_prefix}_user_{i}@example.com",
            full_name=f"Advanced User {i}",
        )
        for i in range(5)
    ]
    await neondb.add_all(users)

    tags = [Tag(name=f"{unique_prefix}_tag_{i}") for i in range(5)]
    await neondb.add_all(tags)

    posts: list[Post] = []
    post_tag_links: list[dict[str, int]] = []
    for i in range(10):
        author = users[i % 5]
        post = Post(
            title=f"{unique_prefix}_post_{i}",
            content=f"Complex content for post {i}",
            author_id=author.id,
            published=True,
        )
        posts.append(post)
    await neondb.add_all(posts)

    for i, post in enumerate(posts):
        tag_a = tags[i % 5]
        tag_b = tags[(i + 1) % 5]
        post_tag_links.append({"post_id": post.id, "tag_id": tag_a.id})
        post_tag_links.append({"post_id": post.id, "tag_id": tag_b.id})

    await neondb.execute(sa.insert(post_tags).values(post_tag_links))

    comments = []
    for i in range(25):
        post = posts[i % 10]
        commenter = users[(i + 2) % 5]
        comment = Comment(
            content=f"Detailed comment {i} on post {post.title}",
            post_id=post.id,
            author_id=commenter.id,
        )
        comments.append(comment)
    await neondb.add_all(comments)

    try:
        yield {
            "user_ids": [u.id for u in users],
            "post_ids": [p.id for p in posts],
            "tag_ids": [t.id for t in tags],
        }
    finally:
        # !!!!!!!!!!!!!!!!!!!!!!!! deactivated cleanup for debugging !!!!!!!!!!!!!!!!!!!!!!!!!
        ...
        # await neondb.execute(
        #     sa.delete(User).where(User.username.like(f"{unique_prefix}_user_%"))
        # )
        # await neondb.execute(
        #     sa.delete(Tag).where(Tag.name.like(f"{unique_prefix}_tag_%"))
        # )


@pytest.mark.asyncio(loop_scope="session")
class TestHardIntegration:
    """Complex integration tests."""

    @logfire.instrument("Pytest: test_complex_fetch_query_options", new_trace=True)
    async def test_complex_fetch_query_options(
        self,
        neondb: NeonNativeAsyncEngine,
        seeded_data,
        unique_prefix,
    ):
        """Test complex fetch with selectinload, subqueryload, and joinedload."""
        user_ids = seeded_data["user_ids"]
        stmt = (
            sa.select(User)
            .where(User.id.in_(user_ids))
            .options(
                subqueryload(User.posts).options(
                    joinedload(Post.author),
                    joinedload(Post.tags),
                ),
                selectinload(User.comments),
            )
        )
        result = await neondb.execute(stmt)
        users = result.unique().scalars().all()

        assert len(users) == 5
        for user in users:
            assert f"{unique_prefix}_user" in user.username
            assert len(user.posts) > 0

    @logfire.instrument("Pytest: test_parallel_query_execution", new_trace=True)
    async def test_parallel_query_execution(
        self,
        neondb: NeonNativeAsyncEngine,
        seeded_data,
        unique_prefix,
    ):
        """
        Test parallel execution using asyncio.gather.
        Verifies genuine parallelism by measuring execution time with pg_sleep.
        """
        user_ids = seeded_data["user_ids"]
        sleep_duration = 1.0
        num_tasks = 3

        async def fetch_stats(user_id):
            await neondb.execute(sa.select(sa.func.pg_sleep(sleep_duration)))

            u_stmt = sa.select(User).where(User.id == user_id)
            u_res = await neondb.execute(u_stmt)
            user: User = u_res.scalar_one()

            p_stmt = sa.select(sa.func.count(Post.id)).where(Post.author_id == user_id)
            p_res = await neondb.execute(p_stmt)
            post_count = p_res.scalar()

            return {"username": user.username, "posts": post_count}

        start_time = time.perf_counter()
        tasks = [fetch_stats(uid) for uid in user_ids[:num_tasks]]
        results = await asyncio.gather(*tasks)
        end_time = time.perf_counter()

        total_duration = end_time - start_time
        sequential_duration = sleep_duration * num_tasks

        assert len(results) == num_tasks
        for res in results:
            assert f"{unique_prefix}_user_" in res["username"]

        assert total_duration < sequential_duration * 0.8, (
            f"Genuine parallelism failed: total_duration={total_duration:.2f}s, "
            f"sequential_duration={sequential_duration:.2f}s"
        )
        assert total_duration >= sleep_duration

    @logfire.instrument("Pytest: test_complex_fetch_plus_parallel", new_trace=True)
    async def test_complex_fetch_plus_parallel(
        self,
        neondb: NeonNativeAsyncEngine,
        seeded_data,
        unique_prefix,
    ):
        """Test complex fetch with parallel execution."""
        post_ids = seeded_data["post_ids"]

        async def fetch_post_with_details(post_id):
            stmt = (
                sa.select(Post)
                .where(Post.id == post_id)
                .options(
                    joinedload(Post.author),
                    selectinload(Post.tags),
                    selectinload(Post.comments).selectinload(Comment.author),
                )
            )
            result = await neondb.execute(stmt)
            return result.unique().scalar_one()

        tasks = [fetch_post_with_details(pid) for pid in post_ids[:4]]
        posts = await asyncio.gather(*tasks)
        assert len(posts) == 4
        for post in posts:
            assert f"{unique_prefix}_post_" in post.title
            assert post.author is not None
            assert len(post.tags) >= 1
            assert len(post.comments) >= 1
