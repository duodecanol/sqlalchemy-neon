"""
Integration tests for SQLAlchemy Neon driver with comprehensive query patterns.

These tests require a live Neon database connection. Set the NEON_DATABASE_URL
environment variable to run these tests:

    export NEON_DATABASE_URL="postgresql://user:pass@host.neon.tech/db"
    pytest tests/integration/test_integration_basics.py

Tests cover:
- Basic CRUD operations (Create, Read, Update, Delete)
- Various query patterns (filters, joins, aggregations, ordering)
- Different SQLAlchemy ORM features
- Transaction management
- Type conversions for different PostgreSQL data types
- Async dialect only (sync is disabled)

Note: This file has been converted from sync to async.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, UTC
from decimal import Decimal
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy import (
    func,
    select,
    and_,
    or_,
    desc,
    asc,
)
from sqlalchemy.orm import (
    selectinload,
)
from sqlalchemy_neon import NeonAsyncSession
from tests.integration.models import User, Post, Comment, Tag, Product


def get_unique_name(name: str) -> str:
    """Append a short unique suffix to a name."""
    return f"{name}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_users(sample_users: list[User]) -> list[User]:
    """Provide sample users with unique usernames."""
    for user in sample_users:
        user.username = get_unique_name(user.username)
        user.email = get_unique_name(user.email.split("@")[0]) + "@example.com"
    return sample_users


@pytest.mark.asyncio
class TestAsyncBasicCRUD:
    """Test basic CRUD operations with async engine."""

    async def test_insert_single_user(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test inserting a single user."""
        user = sample_users[0]
        user.username = get_unique_name("alice")
        async_session.add(user)
        await async_session.commit()

        assert user.id is not None
        assert user.uuid is not None
        assert "alice" in user.username

        q = sa.delete(User).where(User.id == user.id)
        await async_session.execute(q)
        await async_session.commit()

    async def test_insert_multiple_users(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test bulk insert of multiple users."""

        async_session.add_all(unique_users)
        await async_session.commit()

        for user in unique_users:
            assert user.id is not None

        q = sa.delete(User).where(User.id.in_([u.id for u in unique_users]))
        await async_session.execute(q)
        await async_session.commit()

    async def test_select_all_users(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test selecting all users."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        result = await async_session.execute(
            select(User).where(User.username.in_(usernames))
        )
        users = result.scalars().all()

        assert len(users) == 3
        assert {u.username for u in users} == usernames

        q = sa.delete(User).where(User.id.in_([u.id for u in unique_users]))
        await async_session.execute(q)
        await async_session.commit()

    async def test_select_by_id(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test selecting a user by primary key."""
        async_session.add_all(unique_users)
        await async_session.commit()
        user_id = unique_users[0].id

        user = await async_session.get(User, user_id)
        assert user is not None
        assert user.username == unique_users[0].username

        q = sa.delete(User).where(User.id.in_([u.id for u in unique_users]))
        await async_session.execute(q)
        await async_session.commit()

    async def test_update_user(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test updating user attributes."""
        user = sample_users[0]
        user.username = get_unique_name("update_user")
        async_session.add(user)
        await async_session.commit()

        user.full_name = "Alice Wonder"
        user.is_active = False
        await async_session.commit()

        await async_session.refresh(user)
        assert user.full_name == "Alice Wonder"
        assert user.is_active is False

        await async_session.delete(user)
        await async_session.commit()

    async def test_delete_user(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test deleting a user."""
        user = sample_users[0]
        user.username = get_unique_name("delete_user")
        async_session.add(user)
        await async_session.commit()
        user_id = user.id

        await async_session.delete(user)
        await async_session.commit()

        deleted_user = await async_session.get(User, user_id)
        assert deleted_user is None


@pytest.mark.asyncio
class TestAsyncQueryPatterns:
    """Test various query patterns and filtering."""

    async def test_filter_by_equality(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test filtering with equality conditions."""
        async_session.add_all(unique_users)
        await async_session.commit()

        bob_username = next(u.username for u in unique_users if "bob" in u.username)
        stmt = select(User).where(User.username == bob_username)
        result = await async_session.execute(stmt)
        user = result.scalar_one()
        assert "bob" in user.email

    async def test_filter_by_inequality(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test filtering with inequality conditions."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = select(User).where(and_(User.is_active, User.username.in_(usernames)))
        result = await async_session.execute(stmt)
        active_users = result.scalars().all()
        assert len(active_users) == 2
        assert all(u.is_active for u in active_users)

    async def test_filter_with_and(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test AND conditions."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = select(User).where(
            and_(
                User.is_active,
                User.username.in_(usernames),
                User.username.like("%alice%"),
            )
        )
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1
        assert "alice" in users[0].username

    async def test_filter_with_or(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test OR conditions."""
        async_session.add_all(unique_users)
        await async_session.commit()

        u1, u2 = unique_users[0].username, unique_users[1].username
        stmt = select(User).where(or_(User.username == u1, User.username == u2))
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        found_usernames = {u.username for u in users}
        assert u1 in found_usernames
        assert u2 in found_usernames

    async def test_filter_with_in(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test IN operator."""
        async_session.add_all(unique_users)
        await async_session.commit()

        u1, u3 = unique_users[0].username, unique_users[2].username
        stmt = select(User).where(User.username.in_([u1, u3]))
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        found_usernames = {u.username for u in users}
        assert u1 in found_usernames
        assert u3 in found_usernames

    async def test_filter_with_like(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test LIKE pattern matching."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = select(User).where(
            and_(User.username.in_(usernames), User.full_name.like("%Smith%"))
        )
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1
        assert "bob" in users[0].username

    async def test_filter_with_null(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test NULL checks."""
        # Create a user without full_name
        uname = get_unique_name("dave")
        user = User(username=uname, email=f"{uname}@example.com")
        async_session.add(user)
        await async_session.commit()

        stmt = (
            select(User).where(User.username == uname).where(User.full_name.is_(None))
        )
        result = await async_session.execute(stmt)
        user = result.scalar_one()
        assert user.username == uname

    async def test_order_by_asc(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test ORDER BY ascending."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = sorted([u.username for u in unique_users])
        stmt = (
            select(User)
            .where(User.username.in_(usernames))
            .order_by(asc(User.username))
        )
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert [u.username for u in users] == usernames

    async def test_order_by_desc(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test ORDER BY descending."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = (
            select(User)
            .where(User.username.in_(usernames))
            .order_by(desc(User.created_at))
        )
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 3

    async def test_limit(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test LIMIT clause."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = select(User).where(User.username.in_(usernames)).limit(2)
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 2

    async def test_offset(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test OFFSET clause."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = (
            select(User)
            .where(User.username.in_(usernames))
            .order_by(User.id)
            .offset(1)
            .limit(2)
        )
        result = await async_session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 2


@pytest.mark.asyncio
class TestAsyncAggregations:
    """Test aggregation functions."""

    async def test_count_all(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test COUNT(*)."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = (
            select(func.count()).select_from(User).where(User.username.in_(usernames))
        )
        result = await async_session.execute(stmt)
        count = result.scalar()
        assert count == 3

    async def test_count_with_filter(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test COUNT with WHERE clause."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = (
            select(func.count())
            .select_from(User)
            .where(and_(User.is_active, User.username.in_(usernames)))
        )
        result = await async_session.execute(stmt)
        count = result.scalar()
        assert count == 2

    async def test_sum_aggregation(self, async_session: NeonAsyncSession):
        """Test SUM aggregation."""
        pname = get_unique_name("Widget Sum")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.50"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("25.00"), stock=50),
            Product(name=f"{pname}_C", price=Decimal("5.25"), stock=200),
        ]
        async_session.add_all(products)
        await async_session.commit()

        stmt = select(func.sum(Product.stock)).where(Product.name.like(f"{pname}%"))
        result = await async_session.execute(stmt)
        total_stock = result.scalar()
        assert total_stock == 350

    async def test_avg_aggregation(self, async_session: NeonAsyncSession):
        """Test AVG aggregation."""
        pname = get_unique_name("Widget Avg")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.00"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("20.00"), stock=50),
        ]
        async_session.add_all(products)
        await async_session.commit()

        stmt = select(func.avg(Product.price)).where(Product.name.like(f"{pname}%"))
        result = await async_session.execute(stmt)
        avg_price = result.scalar()
        assert float(avg_price) == 15.0

    async def test_min_max_aggregation(self, async_session: NeonAsyncSession):
        """Test MIN and MAX aggregations."""
        pname = get_unique_name("Widget MM")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.50"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("25.00"), stock=50),
            Product(name=f"{pname}_C", price=Decimal("5.25"), stock=200),
        ]
        async_session.add_all(products)
        await async_session.commit()

        stmt = select(func.min(Product.price), func.max(Product.price)).where(
            Product.name.like(f"{pname}%")
        )
        result = await async_session.execute(stmt)
        min_price, max_price = result.one()
        assert min_price == Decimal("5.25")
        assert max_price == Decimal("25.00")

    async def test_group_by(
        self, async_session: NeonAsyncSession, unique_users: list[User]
    ):
        """Test GROUP BY with COUNT."""
        async_session.add_all(unique_users)
        await async_session.commit()

        usernames = {u.username for u in unique_users}
        stmt = (
            select(User.is_active, func.count(User.id))
            .where(User.username.in_(usernames))
            .group_by(User.is_active)
        )
        result = await async_session.execute(stmt)
        results = result.all()
        result_dict = {is_active: count for is_active, count in results}

        assert result_dict[True] == 2
        assert result_dict[False] == 1


@pytest.mark.asyncio
class TestAsyncRelationships:
    """Test foreign key relationships and joins."""

    async def test_one_to_many_relationship(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test one-to-many relationship (User -> Posts)."""
        user = sample_users[0]
        user.username = get_unique_name("one_to_many")
        async_session.add(user)
        await async_session.commit()

        post1 = Post(title="First Post", content="Hello World", author_id=user.id)
        post2 = Post(title="Second Post", content="Hello Again", author_id=user.id)
        async_session.add_all([post1, post2])
        await async_session.commit()

        # Refresh user with posts loaded
        user_id = user.id
        stmt = select(User).options(selectinload(User.posts)).where(User.id == user_id)
        result = await async_session.execute(stmt)
        user = result.scalar_one()

        assert len(user.posts) == 2
        assert {p.title for p in user.posts} == {"First Post", "Second Post"}

    async def test_many_to_one_relationship(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test many-to-one relationship (Post -> User)."""
        user = sample_users[0]
        user.username = get_unique_name("many_to_one")
        async_session.add(user)
        await async_session.commit()

        post = Post(title="Test Post", content="Content", author_id=user.id)
        async_session.add(post)
        await async_session.commit()

        stmt = select(Post).options(selectinload(Post.author)).where(Post.id == post.id)
        result = await async_session.execute(stmt)
        post = result.scalar_one()

        assert post.author.username == user.username

    async def test_join_query(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test JOIN between tables."""
        user = sample_users[0]
        user.username = get_unique_name("join_user")
        async_session.add(user)
        await async_session.commit()

        post = Post(
            title="Alice's Post Unique",
            content="Content",
            author_id=user.id,
            published=True,
        )
        async_session.add(post)
        await async_session.commit()

        stmt = (
            select(Post, User)
            .join(User, Post.author_id == User.id)
            .where(User.username == user.username)
        )
        result = await async_session.execute(stmt)
        results = result.all()
        assert len(results) == 1
        p, u = results[0]
        assert p.title == "Alice's Post Unique"
        assert u.username == user.username

    async def test_many_to_many_relationship(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test many-to-many relationship (Post <-> Tags)."""
        user = sample_users[0]
        user.username = get_unique_name("m2m_user")
        async_session.add(user)
        await async_session.commit()

        tag1 = Tag(name=get_unique_name("python"))
        tag2 = Tag(name=get_unique_name("sqlalchemy"))
        post = Post(
            title="SQLAlchemy Tutorial M2M",
            content="Learn SQLAlchemy",
            author_id=user.id,
            tags=[tag1, tag2],
        )
        async_session.add(post)
        await async_session.commit()

        stmt = select(Post).options(selectinload(Post.tags)).where(Post.id == post.id)
        result = await async_session.execute(stmt)
        post = result.scalar_one()

        assert len(post.tags) == 2
        assert {t.name for t in post.tags} == {tag1.name, tag2.name}

    async def test_nested_relationships(
        self, async_session: NeonAsyncSession, sample_users: list[User]
    ):
        """Test nested relationships (User -> Post -> Comments)."""
        user1, user2 = sample_users[0], sample_users[1]
        user1.username = get_unique_name("nested1")
        user2.username = get_unique_name("nested2")
        async_session.add_all([user1, user2])
        await async_session.commit()

        post = Post(title="Discussion Nested", content="Let's talk", author_id=user1.id)
        async_session.add(post)
        await async_session.commit()

        comment1 = Comment(content="Great post!", post_id=post.id, author_id=user2.id)
        comment2 = Comment(content="Thanks!", post_id=post.id, author_id=user1.id)
        async_session.add_all([comment1, comment2])
        await async_session.commit()

        # Load post with comments and comments' authors
        stmt = (
            select(Post)
            .options(selectinload(Post.comments).selectinload(Comment.author))
            .where(Post.id == post.id)
        )
        result = await async_session.execute(stmt)
        post = result.scalar_one()

        assert len(post.comments) == 2
        assert {c.author.username for c in post.comments} == {
            user1.username,
            user2.username,
        }


@pytest.mark.asyncio
class TestAsyncTransactions:
    """Test transaction handling."""

    async def test_commit_transaction(self, async_session: NeonAsyncSession):
        """Test successful transaction commit."""
        uname = get_unique_name("commit_test")
        user = User(username=uname, email=f"{uname}@example.com")
        async_session.add(user)
        await async_session.commit()

        stmt = select(User).where(User.username == uname)
        result = await async_session.execute(stmt)
        user = result.scalar_one()
        assert user.email == f"{uname}@example.com"


@pytest.mark.asyncio
class TestAsyncJSONBOperations:
    """Test JSONB column operations."""

    async def test_insert_jsonb(self, async_session: NeonAsyncSession):
        """Test inserting JSONB data."""
        uname = get_unique_name("json_user")
        user = User(
            username=uname,
            email=f"{uname}@example.com",
            profile={
                "role": "admin",
                "settings": {"theme": "dark", "notifications": True},
            },
        )
        async_session.add(user)
        await async_session.commit()

        await async_session.refresh(user)
        assert user.profile["role"] == "admin"
        assert user.profile["settings"]["theme"] == "dark"

    async def test_update_jsonb(self, async_session: NeonAsyncSession):
        """Test updating JSONB data."""
        uname = get_unique_name("json_user_upd")
        user = User(
            username=uname, email=f"{uname}@example.com", profile={"role": "user"}
        )
        async_session.add(user)
        await async_session.commit()

        user.profile = {"role": "admin", "level": 5}
        await async_session.commit()

        await async_session.refresh(user)
        assert user.profile["role"] == "admin"
        assert user.profile["level"] == 5


@pytest.mark.asyncio
class TestAsyncNumericTypes:
    """Test numeric type handling."""

    async def test_decimal_precision(self, async_session: NeonAsyncSession):
        """Test Decimal type with precision."""
        pname = get_unique_name("Decimal Prod")
        product = Product(name=pname, price=Decimal("123.45"), stock=10)
        async_session.add(product)
        await async_session.commit()

        await async_session.refresh(product)
        assert product.price == Decimal("123.45")
        assert isinstance(product.price, Decimal)

    async def test_integer_types(self, async_session: NeonAsyncSession):
        """Test integer type handling."""
        pname = get_unique_name("Integer Prod")
        product = Product(name=pname, price=Decimal("10.00"), stock=999999)
        async_session.add(product)
        await async_session.commit()

        await async_session.refresh(product)
        assert product.stock == 999999
        assert isinstance(product.stock, int)


@pytest.mark.asyncio
class TestAsyncDateTimeTypes:
    """Test date and datetime type handling."""

    async def test_date_type(self, async_session: NeonAsyncSession):
        """Test Date type."""
        uname = get_unique_name("date_user")
        user = User(
            username=uname, email=f"{uname}@example.com", birth_date=date(1990, 1, 15)
        )
        async_session.add(user)
        await async_session.commit()

        await async_session.refresh(user)
        assert user.birth_date == date(1990, 1, 15)
        assert isinstance(user.birth_date, date)

    async def test_datetime_type(self, async_session: NeonAsyncSession):
        """Test DateTime type."""
        now = datetime.now(UTC).replace(
            microsecond=0, tzinfo=None
        )  # Keep naive for now if DB expects naive, or just use now(UTC)
        uname = get_unique_name("datetime_user")
        user = User(username=uname, email=f"{uname}@example.com", created_at=now)
        async_session.add(user)
        await async_session.commit()

        await async_session.refresh(user)
        # Compare with second precision due to potential rounding
        assert abs((user.created_at - now).total_seconds()) < 1


@pytest.mark.asyncio
class TestAsyncUUIDType:
    """Test UUID type handling."""

    async def test_uuid_generation(self, async_session: NeonAsyncSession):
        """Test automatic UUID generation."""
        uname = get_unique_name("uuid_user")
        user = User(username=uname, email=f"{uname}@example.com")
        async_session.add(user)
        await async_session.commit()

        assert user.uuid is not None
        assert isinstance(user.uuid, UUID)

    async def test_uuid_query(self, async_session: NeonAsyncSession):
        """Test querying by UUID."""
        uname = get_unique_name("uuid_query")
        user = User(username=uname, email=f"{uname}@example.com")
        async_session.add(user)
        await async_session.commit()
        user_uuid = user.uuid

        stmt = select(User).where(User.uuid == user_uuid)
        result = await async_session.execute(stmt)
        user = result.scalar_one()
        assert user.username == uname
