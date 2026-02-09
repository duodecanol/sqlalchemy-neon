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
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload, subqueryload, joinedload
from sqlalchemy_neon import NeonAsyncSession
from tests.integration.models import User, Post, Comment, Tag

import logfire


def get_unique_suffix() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def unique_prefix() -> str:
    return f"hard_{get_unique_suffix()}"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
@logfire.instrument("Pytest: seeded_data", new_trace=True)
async def seeded_data(async_engine, unique_prefix: str):
    """Seed the database once for all tests in this module."""
    async with NeonAsyncSession(async_engine, expire_on_commit=False) as session:
        # 1. Create Users
        users = [
            User(
                username=f"{unique_prefix}_user_{i}",
                email=f"{unique_prefix}_user_{i}@example.com",
                full_name=f"Advanced User {i}",
            )
            for i in range(5)
        ]
        session.add_all(users)
        await session.flush()

        # 2. Create Tags
        tags = [Tag(name=f"{unique_prefix}_tag_{i}") for i in range(5)]
        session.add_all(tags)
        await session.flush()

        # 3. Create Posts
        posts = []
        for i in range(10):
            author = users[i % 5]
            post_tags = [tags[i % 5], tags[(i + 1) % 5]]
            post = Post(
                title=f"{unique_prefix}_post_{i}",
                content=f"Complex content for post {i}",
                author=author,
                published=True,
                tags=post_tags,
            )
            posts.append(post)
        session.add_all(posts)
        await session.flush()

        # 4. Create Comments
        comments = []
        for i in range(25):
            post = posts[i % 10]
            commenter = users[(i + 2) % 5]
            comment = Comment(
                content=f"Detailed comment {i} on post {post.title}",
                post=post,
                author=commenter,
            )
            comments.append(comment)
        session.add_all(comments)

        await session.commit()

        return {
            "user_ids": [u.id for u in users],
            "post_ids": [p.id for p in posts],
            "tag_ids": [t.id for t in tags],
        }


@pytest.mark.asyncio
class TestHardIntegration:
    """Complex integration tests."""

    @logfire.instrument("Pytest: test_complex_fetch_query_options", new_trace=True)
    async def test_complex_fetch_query_options(
        self, async_engine, seeded_data, unique_prefix
    ):
        """Test complex fetch with selectinload, subqueryload, and joinedload."""
        user_ids = seeded_data["user_ids"]
        async with NeonAsyncSession(async_engine, expire_on_commit=False) as session:
            stmt = (
                select(User)
                .where(User.id.in_(user_ids))
                .options(
                    subqueryload(User.posts).options(
                        joinedload(Post.author), joinedload(Post.tags)
                    ),
                    selectinload(User.comments),
                )
            )
            result = await session.execute(stmt)
            users = result.unique().scalars().all()

            assert len(users) == 5
            for user in users:
                assert f"{unique_prefix}_user" in user.username
                assert len(user.posts) > 0

    @logfire.instrument("Pytest: test_parallel_query_execution", new_trace=True)
    async def test_parallel_query_execution(
        self, async_engine, seeded_data, unique_prefix
    ):
        """
        Test parallel execution using asyncio.gather.
        Verifies genuine parallelism by measuring execution time with pg_sleep.
        """
        user_ids = seeded_data["user_ids"]
        sleep_duration = 1.0
        num_tasks = 3

        async def fetch_stats(user_id):
            async with NeonAsyncSession(async_engine) as session:
                # Use DB-side sleep to ensure we can measure overlapping execution
                await session.execute(select(func.pg_sleep(sleep_duration)))

                u_stmt = select(User).where(User.id == user_id)
                u_res = await session.execute(u_stmt)
                user = u_res.scalar_one()

                p_stmt = select(func.count(Post.id)).where(Post.author_id == user_id)
                p_res = await session.execute(p_stmt)
                post_count = p_res.scalar()

                return {"username": user.username, "posts": post_count}

        start_time = time.perf_counter()
        tasks = [fetch_stats(uid) for uid in user_ids[:num_tasks]]
        results = await asyncio.gather(*tasks)
        end_time = time.perf_counter()

        total_duration = end_time - start_time
        sequential_duration = sleep_duration * num_tasks

        # Verify result integrity
        assert len(results) == num_tasks
        for res in results:
            assert f"{unique_prefix}_user_" in res["username"]

        # Verify genuine parallelism:
        # Total duration should be close to sleep_duration,
        # definitely much less than sequential_duration.
        assert total_duration < sequential_duration * 0.8, (
            f"Genuine parallelism failed: total_duration={total_duration:.2f}s, "
            f"sequential_duration={sequential_duration:.2f}s"
        )
        # It should be around sleep_duration + network overhead
        assert total_duration >= sleep_duration

    @logfire.instrument("Pytest: test_complex_fetch_plus_parallel", new_trace=True)
    async def test_complex_fetch_plus_parallel(
        self, async_engine, seeded_data, unique_prefix
    ):
        """Test complex fetch with parallel execution."""
        post_ids = seeded_data["post_ids"]

        async def fetch_post_with_details(post_id):
            async with NeonAsyncSession(async_engine) as session:
                stmt = (
                    select(Post)
                    .where(Post.id == post_id)
                    .options(
                        joinedload(Post.author),
                        selectinload(Post.tags),
                        selectinload(Post.comments).selectinload(Comment.author),
                    )
                )
                result = await session.execute(stmt)
                return result.unique().scalar_one()

        tasks = [fetch_post_with_details(pid) for pid in post_ids[:4]]
        posts = await asyncio.gather(*tasks)
        assert len(posts) == 4
