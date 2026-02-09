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
- Type conversions for different PostgreSQL data types
- Async dialect only (sync is disabled)
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import selectinload
from sqlalchemy_neon import NeonNativeAsyncEngine

from testsupport.models import Base, Comment, Post, Product, Tag, User, post_tags


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
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test inserting a single user."""
        user = sample_users[0]
        user.username = get_unique_name("alice")
        await neondb.add(user)

        assert user.id is not None
        assert user.uuid is not None
        assert "alice" in user.username

        q = sa.delete(User).where(User.id == user.id)
        await neondb.execute(q)

    async def test_insert_multiple_users(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test bulk insert of multiple users."""
        await neondb.add_all(unique_users)

        for user in unique_users:
            assert user.id is not None

        q = sa.delete(User).where(User.id.in_([u.id for u in unique_users]))
        await neondb.execute(q)

    async def test_select_all_users(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test selecting all users."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        result = await neondb.execute(
            sa.select(User).where(User.username.in_(usernames))
        )
        users = result.scalars().all()

        assert len(users) == 3
        assert {u.username for u in users} == usernames

        await neondb.delete_all(users)

    async def test_select_by_id(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test selecting a user by primary key."""
        await neondb.add_all(unique_users)
        user_id = unique_users[0].id

        result = await neondb.execute(sa.select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        assert user is not None
        assert user.username == unique_users[0].username

        await neondb.delete_all(unique_users)

    async def test_update_user(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test updating user attributes."""
        user = sample_users[0]
        user.username = get_unique_name("update_user")
        await neondb.add(user)

        await neondb.execute(
            sa.update(User)
            .where(User.id == user.id)
            .values(full_name="Alice Wonder", is_active=False)
        )

        result = await neondb.execute(sa.select(User).where(User.id == user.id))
        updated_user = result.scalar_one()
        assert updated_user.full_name == "Alice Wonder"
        assert updated_user.is_active is False

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_delete_user(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test deleting a user."""
        user = sample_users[0]
        user.username = get_unique_name("delete_user")
        await neondb.add(user)
        user_id = user.id

        await neondb.execute(sa.delete(User).where(User.id == user_id))

        result = await neondb.execute(sa.select(User).where(User.id == user_id))
        deleted_user = result.scalar_one_or_none()
        assert deleted_user is None


@pytest.mark.asyncio
class TestAsyncQueryPatterns:
    """Test various query patterns and filtering."""

    async def test_filter_by_equality(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test filtering with equality conditions."""
        await neondb.add_all(unique_users)

        bob_username = next(u.username for u in unique_users if "bob" in u.username)
        stmt = sa.select(User).where(User.username == bob_username)
        result = await neondb.execute(stmt)
        user = result.scalar_one()
        assert "bob" in user.email

        await neondb.delete_all(unique_users)

    async def test_filter_by_inequality(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test filtering with inequality conditions."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = sa.select(User).where(
            sa.and_(User.is_active, User.username.in_(usernames))
        )
        result = await neondb.execute(stmt)
        active_users = result.scalars().all()
        assert len(active_users) == 2
        assert all(u.is_active for u in active_users)

        await neondb.delete_all(unique_users)

    async def test_filter_with_and(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test AND conditions."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = sa.select(User).where(
            sa.and_(
                User.is_active,
                User.username.in_(usernames),
                User.username.like("%alice%"),
            )
        )
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1
        assert "alice" in users[0].username

        await neondb.delete_all(unique_users)

    async def test_filter_with_or(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test OR conditions."""
        await neondb.add_all(unique_users)

        u1, u2 = unique_users[0].username, unique_users[1].username
        stmt = sa.select(User).where(sa.or_(User.username == u1, User.username == u2))
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        found_usernames = {u.username for u in users}
        assert u1 in found_usernames
        assert u2 in found_usernames

        await neondb.delete_all(unique_users)

    async def test_filter_with_in(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test IN operator."""
        await neondb.add_all(unique_users)

        u1, u3 = unique_users[0].username, unique_users[2].username
        stmt = sa.select(User).where(User.username.in_([u1, u3]))
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        found_usernames = {u.username for u in users}
        assert u1 in found_usernames
        assert u3 in found_usernames

        await neondb.delete_all(unique_users)

    async def test_filter_with_like(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test LIKE pattern matching."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = sa.select(User).where(
            sa.and_(User.username.in_(usernames), User.full_name.like("%Smith%"))
        )
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1
        assert "bob" in users[0].username

        await neondb.delete_all(unique_users)

    async def test_filter_with_null(self, neondb: NeonNativeAsyncEngine):
        """Test NULL checks."""
        uname = get_unique_name("dave")
        user = User(username=uname, email=f"{uname}@example.com")
        await neondb.add(user)

        stmt = (
            sa.select(User)
            .where(User.username == uname)
            .where(User.full_name.is_(None))
        )
        result = await neondb.execute(stmt)
        found_user = result.scalar_one()
        assert found_user.username == uname

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_order_by_asc(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test ORDER BY ascending."""
        await neondb.add_all(unique_users)

        usernames = sorted([u.username for u in unique_users])
        stmt = (
            sa.select(User)
            .where(User.username.in_(usernames))
            .order_by(sa.asc(User.username))
        )
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert [u.username for u in users] == usernames

        await neondb.delete_all(unique_users)

    async def test_order_by_desc(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test ORDER BY descending."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = (
            sa.select(User)
            .where(User.username.in_(usernames))
            .order_by(sa.desc(User.created_at))
        )
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 3

        await neondb.delete_all(unique_users)

    async def test_limit(self, neondb: NeonNativeAsyncEngine, unique_users: list[User]):
        """Test LIMIT clause."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = sa.select(User).where(User.username.in_(usernames)).limit(2)
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 2

        await neondb.delete_all(unique_users)

    async def test_offset(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test OFFSET clause."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = (
            sa.select(User)
            .where(User.username.in_(usernames))
            .order_by(User.id)
            .offset(1)
            .limit(2)
        )
        result = await neondb.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 2

        await neondb.delete_all(unique_users)


@pytest.mark.asyncio
class TestAsyncAggregations:
    """Test aggregation functions."""

    async def test_count_all(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test COUNT(*)."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = (
            sa.select(sa.func.count())
            .select_from(User)
            .where(User.username.in_(usernames))
        )
        result = await neondb.execute(stmt)
        count = result.scalar()
        assert count == 3

        await neondb.delete_all(unique_users)

    async def test_count_with_filter(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test COUNT with WHERE clause."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = (
            sa.select(sa.func.count())
            .select_from(User)
            .where(sa.and_(User.is_active, User.username.in_(usernames)))
        )
        result = await neondb.execute(stmt)
        count = result.scalar()
        assert count == 2

        await neondb.delete_all(unique_users)

    async def test_sum_aggregation(self, neondb: NeonNativeAsyncEngine):
        """Test SUM aggregation."""
        pname = get_unique_name("Widget Sum")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.50"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("25.00"), stock=50),
            Product(name=f"{pname}_C", price=Decimal("5.25"), stock=200),
        ]
        await neondb.add_all(products)

        stmt = sa.select(sa.func.sum(Product.stock)).where(
            Product.name.like(f"{pname}%")
        )
        result = await neondb.execute(stmt)
        total_stock = result.scalar()
        assert total_stock == 350

        await neondb.delete_all(products)

    async def test_avg_aggregation(self, neondb: NeonNativeAsyncEngine):
        """Test AVG aggregation."""
        pname = get_unique_name("Widget Avg")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.00"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("20.00"), stock=50),
        ]
        await neondb.add_all(products)

        stmt = sa.select(sa.func.avg(Product.price)).where(
            Product.name.like(f"{pname}%")
        )
        result = await neondb.execute(stmt)
        avg_price = result.scalar()
        assert float(avg_price) == 15.0

        await neondb.delete_all(products)

    async def test_min_max_aggregation(self, neondb: NeonNativeAsyncEngine):
        """Test MIN and MAX aggregations."""
        pname = get_unique_name("Widget MM")
        products = [
            Product(name=f"{pname}_A", price=Decimal("10.50"), stock=100),
            Product(name=f"{pname}_B", price=Decimal("25.00"), stock=50),
            Product(name=f"{pname}_C", price=Decimal("5.25"), stock=200),
        ]
        await neondb.add_all(products)

        stmt = sa.select(sa.func.min(Product.price), sa.func.max(Product.price)).where(
            Product.name.like(f"{pname}%")
        )
        result = await neondb.execute(stmt)
        min_price, max_price = result.one()
        assert min_price == Decimal("5.25")
        assert max_price == Decimal("25.00")

        await neondb.delete_all(products)

    async def test_group_by(
        self, neondb: NeonNativeAsyncEngine, unique_users: list[User]
    ):
        """Test GROUP BY with COUNT."""
        await neondb.add_all(unique_users)

        usernames = {u.username for u in unique_users}
        stmt = (
            sa.select(User.is_active, sa.func.count(User.id))
            .where(User.username.in_(usernames))
            .group_by(User.is_active)
        )
        result = await neondb.execute(stmt)
        rows = result.all()
        result_dict = {is_active: count for is_active, count in rows}

        assert result_dict[True] == 2
        assert result_dict[False] == 1

        await neondb.delete_all(unique_users)


@pytest.mark.asyncio
class TestAsyncRelationships:
    """Test foreign key relationships and joins."""

    async def test_one_to_many_relationship(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test one-to-many relationship (User -> Posts)."""
        user = sample_users[0]
        user.username = get_unique_name("one_to_many")
        await neondb.add(user)

        post1 = Post(title="First Post", content="Hello World", author_id=user.id)
        post2 = Post(title="Second Post", content="Hello Again", author_id=user.id)
        await neondb.add_all([post1, post2])

        stmt = (
            sa.select(User).options(selectinload(User.posts)).where(User.id == user.id)
        )
        result = await neondb.execute(stmt)
        found_user = result.scalar_one()

        assert len(found_user.posts) == 2
        assert {p.title for p in found_user.posts} == {"First Post", "Second Post"}

        await neondb.delete_all([user, post1, post2])

    async def test_many_to_one_relationship(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test many-to-one relationship (Post -> User)."""
        user = sample_users[0]
        user.username = get_unique_name("many_to_one")
        await neondb.add(user)

        post = Post(title="Test Post", content="Content", author_id=user.id)
        await neondb.add(post)

        stmt = (
            sa.select(Post).options(selectinload(Post.author)).where(Post.id == post.id)
        )
        result = await neondb.execute(stmt)
        found_post = result.scalar_one()

        assert found_post.author.username == user.username

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_join_query(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test JOIN between tables."""
        user = sample_users[0]
        user.username = get_unique_name("join_user")
        await neondb.add(user)

        post = Post(
            title="Alice's Post Unique",
            content="Content",
            author_id=user.id,
            published=True,
        )
        await neondb.add(post)

        stmt = (
            sa.select(Post.title, User.username)
            .join(User, Post.author_id == User.id)
            .where(User.username == user.username)
        )
        result = await neondb.execute(stmt)
        rows = result.all()
        assert len(rows) == 1
        found_title, found_username = rows[0]
        assert found_title == "Alice's Post Unique"
        assert found_username == user.username

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_many_to_many_relationship(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test many-to-many relationship (Post <-> Tags)."""
        user = sample_users[0]
        user.username = get_unique_name("m2m_user")
        await neondb.add(user)

        tag1 = Tag(name=get_unique_name("python"))
        tag2 = Tag(name=get_unique_name("sqlalchemy"))
        await neondb.add_all([tag1, tag2])

        post = Post(
            title="SQLAlchemy Tutorial M2M",
            content="Learn SQLAlchemy",
            author_id=user.id,
        )
        await neondb.add(post)

        await neondb.execute(
            sa.insert(post_tags).values(
                [
                    {"post_id": post.id, "tag_id": tag1.id},
                    {"post_id": post.id, "tag_id": tag2.id},
                ]
            )
        )

        stmt = (
            sa.select(Post).options(selectinload(Post.tags)).where(Post.id == post.id)
        )
        result = await neondb.execute(stmt)
        found_post = result.scalar_one()

        assert len(found_post.tags) == 2
        assert {t.name for t in found_post.tags} == {tag1.name, tag2.name}

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_nested_relationships(
        self, neondb: NeonNativeAsyncEngine, sample_users: list[User]
    ):
        """Test nested relationships (User -> Post -> Comments)."""
        user1, user2 = sample_users[0], sample_users[1]
        user1.username = get_unique_name("nested1")
        user2.username = get_unique_name("nested2")
        await neondb.add_all([user1, user2])

        post = Post(title="Discussion Nested", content="Let's talk", author_id=user1.id)
        await neondb.add(post)

        comment1 = Comment(content="Great post!", post_id=post.id, author_id=user2.id)
        comment2 = Comment(content="Thanks!", post_id=post.id, author_id=user1.id)
        await neondb.add_all([comment1, comment2])

        stmt = (
            sa.select(Post)
            .options(selectinload(Post.comments).selectinload(Comment.author))
            .where(Post.id == post.id)
        )
        result = await neondb.execute(stmt)
        found_post = result.scalar_one()

        assert len(found_post.comments) == 2
        assert {c.author.username for c in found_post.comments} == {
            user1.username,
            user2.username,
        }

        await neondb.execute(sa.delete(User).where(User.id.in_([user1.id, user2.id])))


@pytest.mark.asyncio
class TestAsyncTransactions:
    """Test transaction handling."""

    async def test_commit_transaction(self, neondb: NeonNativeAsyncEngine):
        """Test successful transaction commit."""
        uname = get_unique_name("commit_test")
        user = User(username=uname, email=f"{uname}@example.com")
        await neondb.add(user)

        stmt = sa.select(User).where(User.username == uname)
        result = await neondb.execute(stmt)
        found_user = result.scalar_one()
        assert found_user.email == f"{uname}@example.com"

        await neondb.delete_all([found_user])


@pytest.mark.asyncio
class TestAsyncJSONBOperations:
    """Test JSONB column operations."""

    async def test_insert_jsonb(self, neondb: NeonNativeAsyncEngine):
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
        await neondb.add(user)

        result = await neondb.execute(sa.select(User).where(User.id == user.id))
        found_user = result.scalar_one()
        assert found_user.profile["role"] == "admin"
        assert found_user.profile["settings"]["theme"] == "dark"

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_update_jsonb(self, neondb: NeonNativeAsyncEngine):
        """Test updating JSONB data."""
        uname = get_unique_name("json_user_upd")
        user = User(
            username=uname, email=f"{uname}@example.com", profile={"role": "user"}
        )
        await neondb.add(user)

        await neondb.execute(
            sa.update(User)
            .where(User.id == user.id)
            .values(profile={"role": "admin", "level": 5})
        )

        result = await neondb.execute(sa.select(User).where(User.id == user.id))
        found_user = result.scalar_one()
        assert found_user.profile["role"] == "admin"
        assert found_user.profile["level"] == 5

        await neondb.execute(sa.delete(User).where(User.id == user.id))


@pytest.mark.asyncio
class TestAsyncNumericTypes:
    """Test numeric type handling."""

    async def test_decimal_precision(self, neondb: NeonNativeAsyncEngine):
        """Test Decimal type with precision."""
        pname = get_unique_name("Decimal Prod")
        product = Product(name=pname, price=Decimal("123.45"), stock=10)
        await neondb.add(product)

        result = await neondb.execute(
            sa.select(Product).where(Product.id == product.id)
        )
        found_product = result.scalar_one()
        assert found_product.price == Decimal("123.45")
        assert isinstance(found_product.price, Decimal)

        await neondb.execute(sa.delete(Product).where(Product.id == product.id))

    async def test_integer_types(self, neondb: NeonNativeAsyncEngine):
        """Test integer type handling."""
        pname = get_unique_name("Integer Prod")
        product = Product(name=pname, price=Decimal("10.00"), stock=999999)
        await neondb.add(product)

        result = await neondb.execute(
            sa.select(Product).where(Product.id == product.id)
        )
        found_product = result.scalar_one()
        assert found_product.stock == 999999
        assert isinstance(found_product.stock, int)

        await neondb.execute(sa.delete(Product).where(Product.id == product.id))


@pytest.mark.asyncio
class TestAsyncDateTimeTypes:
    """Test date and datetime type handling."""

    async def test_date_type(self, neondb: NeonNativeAsyncEngine):
        """Test Date type."""
        uname = get_unique_name("date_user")
        user = User(
            username=uname, email=f"{uname}@example.com", birth_date=date(1990, 1, 15)
        )
        await neondb.add(user)

        result = await neondb.execute(sa.select(User).where(User.id == user.id))
        found_user = result.scalar_one()
        assert found_user.birth_date == date(1990, 1, 15)
        assert isinstance(found_user.birth_date, date)

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_datetime_type(self, neondb: NeonNativeAsyncEngine):
        """Test DateTime type."""
        now = datetime.now(UTC).replace(microsecond=0, tzinfo=None)
        uname = get_unique_name("datetime_user")
        user = User(username=uname, email=f"{uname}@example.com", created_at=now)
        await neondb.add(user)

        result = await neondb.execute(sa.select(User).where(User.id == user.id))
        found_user = result.scalar_one()
        assert abs((found_user.created_at - now).total_seconds()) < 1

        await neondb.execute(sa.delete(User).where(User.id == user.id))


@pytest.mark.asyncio
class TestAsyncUUIDType:
    """Test UUID type handling."""

    async def test_uuid_generation(self, neondb: NeonNativeAsyncEngine):
        """Test automatic UUID generation."""
        uname = get_unique_name("uuid_user")
        user = User(username=uname, email=f"{uname}@example.com")
        await neondb.add(user)

        assert user.uuid is not None
        assert isinstance(user.uuid, UUID)

        await neondb.execute(sa.delete(User).where(User.id == user.id))

    async def test_uuid_query(self, neondb: NeonNativeAsyncEngine):
        """Test querying by UUID."""
        uname = get_unique_name("uuid_query")
        user = User(username=uname, email=f"{uname}@example.com")
        await neondb.add(user)
        user_uuid = user.uuid

        stmt = sa.select(User).where(User.uuid == user_uuid)
        result = await neondb.execute(stmt)
        found_user = result.scalar_one()
        assert found_user.username == uname

        await neondb.delete(user)
