from __future__ import annotations

import asyncio
from datetime import date, datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import orm
import pytest

from sqlalchemy_neon.native_async_engine import (
    NeonNativeAsyncEngine,
    NativeAsyncResult,
    compile_sql,
)
from sqlalchemy_neon.neon_http_client import QueryResult, TransactionOptions
from sqlalchemy_neon.types import PostgresOID
from testsupport.models import User, Post, Comment


def test_compile_sql_string_with_positional_params():
    sql, params = compile_sql("select $1, $2", [1, "x"])
    assert sql == "select $1, $2"
    assert params == [1, "x"]


def test_compile_sql_core_statement_dict_params():
    users = sa.table("users", sa.column("name"))
    stmt = sa.select(users.c.name).where(users.c.name == sa.bindparam("name"))

    sql, params = compile_sql(stmt, {"name": "alice"})

    assert "$1" in sql
    assert params == ["alice"]


def test_compile_sql_core_statement_reuses_named_bind():
    stmt = sa.text("select :name as a, :name as b")
    sql, params = compile_sql(stmt, {"name": "alice"})

    assert sql.count("$1") == 2
    assert params == ["alice"]


@pytest.mark.asyncio
async def test_native_engine_execute_forwards_to_client(mock_connection_string: str):
    class FakeClient:
        def __init__(self):
            self.calls = []

        async def query(self, sql, params, options=None):
            self.calls.append((sql, params, options))
            return QueryResult(
                rows=[{"v": 1}], fields=[{"name": "v"}], row_count=1, command="SELECT"
            )

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    fake = FakeClient()
    engine._client = fake

    result = await engine.execute(sa.text("select :x as v"), {"x": 1})

    assert isinstance(result, NativeAsyncResult)
    assert result.scalar_one() == 1
    assert fake.calls
    assert fake.calls[0][0] == "select $1 as v"
    assert fake.calls[0][1] == [1]


@pytest.mark.asyncio
async def test_native_engine_transaction_forwards_compiled_statements(
    mock_connection_string: str,
):
    class FakeClient:
        async def query(self, sql, params, options=None):
            raise AssertionError("query() should not be called")

        async def transaction(self, queries, options=None):
            assert queries == [("select $1 as v", [1]), ("select $1 as v", [2])]
            assert isinstance(options, TransactionOptions)
            return [
                QueryResult(
                    rows=[[1]],
                    fields=[{"name": "v"}],
                    row_count=1,
                    command="SELECT",
                    row_as_array=True,
                ),
                QueryResult(
                    rows=[[2]],
                    fields=[{"name": "v"}],
                    row_count=1,
                    command="SELECT",
                    row_as_array=True,
                ),
            ]

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    engine._client = FakeClient()

    results = await engine.transaction(
        [
            (sa.text("select :x as v"), {"x": 1}),
            (sa.text("select :x as v"), {"x": 2}),
        ]
    )

    assert [item.scalar_one() for item in results] == [1, 2]


@pytest.mark.asyncio
async def test_native_engine_add_all_uses_single_transaction(
    mock_connection_string: str,
):
    class FakeClient:
        def __init__(self):
            self.transaction_calls = []

        async def query(self, sql, params, options=None):
            raise AssertionError("query() should not be called")

        async def transaction(self, queries, options=None):
            self.transaction_calls.append((queries, options))
            return [
                QueryResult(
                    rows=[{"id": 11, "username": "alice"}],
                    fields=[{"name": "id"}, {"name": "username"}],
                    row_count=1,
                    command="INSERT",
                ),
                QueryResult(
                    rows=[{"id": 12, "username": "bob"}],
                    fields=[{"name": "id"}, {"name": "username"}],
                    row_count=1,
                    command="INSERT",
                ),
            ]

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    fake = FakeClient()
    engine._client = fake

    users = [
        User(username="alice", email="alice@example.com"),
        User(username="bob", email="bob@example.com"),
    ]
    await engine.add_all(users)

    assert len(fake.transaction_calls) == 1
    queries, options = fake.transaction_calls[0]
    assert len(queries) == 2
    assert isinstance(options, TransactionOptions)
    assert options.read_only is False
    assert users[0].id == 11
    assert users[1].id == 12


@pytest.mark.asyncio
async def test_native_engine_delete_all_uses_single_transaction(
    mock_connection_string: str,
):
    class FakeClient:
        def __init__(self):
            self.transaction_calls = []

        async def query(self, sql, params, options=None):
            raise AssertionError("query() should not be called")

        async def transaction(self, queries, options=None):
            self.transaction_calls.append((queries, options))
            return [
                QueryResult(rows=[], fields=[], row_count=1, command="DELETE"),
                QueryResult(rows=[], fields=[], row_count=1, command="DELETE"),
            ]

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    fake = FakeClient()
    engine._client = fake

    users = [
        User(id=21, username="alice", email="alice@example.com"),
        User(id=22, username="bob", email="bob@example.com"),
    ]
    await engine.delete_all(users)

    assert len(fake.transaction_calls) == 1
    queries, options = fake.transaction_calls[0]
    assert len(queries) == 2
    assert isinstance(options, TransactionOptions)
    assert options.read_only is False


def test_native_result_unique_and_scalars():
    raw = QueryResult(
        rows=[[1], [1], [2]],
        fields=[{"name": "value"}],
        row_count=3,
        command="SELECT",
        row_as_array=True,
    )
    assert NativeAsyncResult(raw).scalars().all() == [1, 1, 2]
    assert NativeAsyncResult(raw).unique().scalars().all() == [1, 2]


def test_native_result_scalar_and_mappings():
    raw = QueryResult(
        rows=[{"value": 5}],
        fields=[{"name": "value"}],
        row_count=1,
        command="SELECT",
    )
    # SQLAlchemy Result semantics: scalar() consumes/closes the result.
    assert NativeAsyncResult(raw).scalar() == 5

    mapped = NativeAsyncResult(raw).mappings().all()
    assert mapped[0]["value"] == 5


def test_native_result_maps_single_orm_entity():
    raw = QueryResult(
        rows=[
            {
                "id": 1,
                "uuid": "123e4567-e89b-12d3-a456-426614174000",
                "username": "alice",
                "email": "alice@example.com",
                "full_name": "Alice",
                "is_active": True,
                "created_at": "2024-01-01 12:00:00",
                "birth_date": "1990-01-01",
                "profile": {"k": "v"},
            }
        ],
        fields=[
            {"name": "id", "dataTypeID": PostgresOID.INT4},
            {"name": "uuid", "dataTypeID": PostgresOID.UUID},
            {"name": "username", "dataTypeID": PostgresOID.VARCHAR},
            {"name": "email", "dataTypeID": PostgresOID.VARCHAR},
            {"name": "full_name", "dataTypeID": PostgresOID.VARCHAR},
            {"name": "is_active", "dataTypeID": PostgresOID.BOOL},
            {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
            {"name": "birth_date", "dataTypeID": PostgresOID.DATE},
            {"name": "profile", "dataTypeID": PostgresOID.JSONB},
        ],
        row_count=1,
        command="SELECT",
    )
    result = NativeAsyncResult(raw, statement=sa.select(User))

    user = result.scalars().one()
    assert isinstance(user, User)
    assert user.username == "alice"
    assert isinstance(user.uuid, UUID)
    assert isinstance(user.birth_date, date)
    assert isinstance(user.created_at, datetime)


@pytest.mark.asyncio
async def test_native_engine_hydrates_loader_option_relationships(
    mock_connection_string: str,
):
    class FakeClient:
        async def query(self, sql, params, options=None):
            sql_l = " ".join(sql.lower().split())

            if "from public.posts" in sql_l and "join" not in sql_l:
                return QueryResult(
                    rows=[
                        {
                            "id": 1,
                            "title": "hello",
                            "content": "body",
                            "author_id": 2,
                            "published": True,
                            "view_count": 0,
                            "created_at": "2024-01-01 10:00:00",
                            "updated_at": None,
                        }
                    ],
                    fields=[
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "title", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "content", "dataTypeID": PostgresOID.TEXT},
                        {"name": "author_id", "dataTypeID": PostgresOID.INT4},
                        {"name": "published", "dataTypeID": PostgresOID.BOOL},
                        {"name": "view_count", "dataTypeID": PostgresOID.INT4},
                        {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                        {"name": "updated_at", "dataTypeID": PostgresOID.TIMESTAMP},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            if "from public.posts join public.users" in sql_l or (
                "from (select public.posts.id as __pid" in sql_l
                and "join public.users" in sql_l
            ):
                return QueryResult(
                    rows=[
                        {
                            "__parent_identity": 1,
                            "id": 2,
                            "uuid": "123e4567-e89b-12d3-a456-426614174000",
                            "username": "alice",
                            "email": "alice@example.com",
                            "full_name": "Alice",
                            "is_active": True,
                            "created_at": "2024-01-01 09:00:00",
                            "birth_date": "1990-01-01",
                            "profile": {"k": "v"},
                        }
                    ],
                    fields=[
                        {"name": "__parent_identity", "dataTypeID": PostgresOID.INT4},
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "uuid", "dataTypeID": PostgresOID.UUID},
                        {"name": "username", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "email", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "full_name", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "is_active", "dataTypeID": PostgresOID.BOOL},
                        {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                        {"name": "birth_date", "dataTypeID": PostgresOID.DATE},
                        {"name": "profile", "dataTypeID": PostgresOID.JSONB},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            if "from public.posts join public.post_tags" in sql_l or (
                "from (select public.posts.id as __pid" in sql_l
                and "join public.post_tags" in sql_l
            ):
                return QueryResult(
                    rows=[{"__parent_identity": 1, "id": 7, "name": "tag-a"}],
                    fields=[
                        {"name": "__parent_identity", "dataTypeID": PostgresOID.INT4},
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "name", "dataTypeID": PostgresOID.VARCHAR},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            if "from public.posts join public.comments" in sql_l or (
                "from (select public.posts.id as __pid" in sql_l
                and "join public.comments" in sql_l
            ):
                return QueryResult(
                    rows=[
                        {
                            "__parent_identity": 1,
                            "id": 10,
                            "content": "comment",
                            "post_id": 1,
                            "author_id": 2,
                            "created_at": "2024-01-01 11:00:00",
                        }
                    ],
                    fields=[
                        {"name": "__parent_identity", "dataTypeID": PostgresOID.INT4},
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "content", "dataTypeID": PostgresOID.TEXT},
                        {"name": "post_id", "dataTypeID": PostgresOID.INT4},
                        {"name": "author_id", "dataTypeID": PostgresOID.INT4},
                        {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            if "from public.comments join public.users" in sql_l:
                return QueryResult(
                    rows=[
                        {
                            "__parent_identity": 10,
                            "id": 2,
                            "uuid": "123e4567-e89b-12d3-a456-426614174000",
                            "username": "alice",
                            "email": "alice@example.com",
                            "full_name": "Alice",
                            "is_active": True,
                            "created_at": "2024-01-01 09:00:00",
                            "birth_date": "1990-01-01",
                            "profile": {"k": "v"},
                        }
                    ],
                    fields=[
                        {"name": "__parent_identity", "dataTypeID": PostgresOID.INT4},
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "uuid", "dataTypeID": PostgresOID.UUID},
                        {"name": "username", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "email", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "full_name", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "is_active", "dataTypeID": PostgresOID.BOOL},
                        {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                        {"name": "birth_date", "dataTypeID": PostgresOID.DATE},
                        {"name": "profile", "dataTypeID": PostgresOID.JSONB},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            raise AssertionError(f"Unexpected SQL: {sql}")

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    engine._client = FakeClient()

    stmt = (
        sa.select(Post)
        .where(Post.id == 1)
        .options(
            orm.subqueryload(Post.author),
            orm.subqueryload(Post.tags),
            orm.subqueryload(Post.comments).selectinload(Comment.author),
        )
    )
    result = await engine.execute(stmt)
    post = result.unique().scalar_one()

    assert post.author is not None
    assert post.author.username == "alice"
    assert len(post.tags) == 1
    assert post.tags[0].name == "tag-a"
    assert len(post.comments) == 1
    assert post.comments[0].author is not None
    assert post.comments[0].author.username == "alice"


@pytest.mark.asyncio
async def test_native_engine_loads_sibling_relationships_concurrently(
    mock_connection_string: str,
):
    class FakeClient:
        def __init__(self):
            self.active = 0
            self.max_active = 0
            self.rel_calls = 0
            self.gate = asyncio.Event()

        async def query(self, sql, params, options=None):
            sql_l = " ".join(sql.lower().split())

            if "from public.posts" in sql_l and "join" not in sql_l:
                return QueryResult(
                    rows=[
                        {
                            "id": 1,
                            "title": "hello",
                            "content": "body",
                            "author_id": 2,
                            "published": True,
                            "view_count": 0,
                            "created_at": "2024-01-01 10:00:00",
                            "updated_at": None,
                        }
                    ],
                    fields=[
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "title", "dataTypeID": PostgresOID.VARCHAR},
                        {"name": "content", "dataTypeID": PostgresOID.TEXT},
                        {"name": "author_id", "dataTypeID": PostgresOID.INT4},
                        {"name": "published", "dataTypeID": PostgresOID.BOOL},
                        {"name": "view_count", "dataTypeID": PostgresOID.INT4},
                        {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                        {"name": "updated_at", "dataTypeID": PostgresOID.TIMESTAMP},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            is_author_rel = "from public.posts join public.users" in sql_l
            is_tags_rel = "from public.posts join public.post_tags" in sql_l
            if is_author_rel or is_tags_rel:
                self.rel_calls += 1
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                if self.rel_calls >= 2:
                    self.gate.set()
                await self.gate.wait()
                await asyncio.sleep(0.01)
                self.active -= 1

                if is_author_rel:
                    return QueryResult(
                        rows=[
                            {
                                "__parent_identity": 1,
                                "id": 2,
                                "uuid": "123e4567-e89b-12d3-a456-426614174000",
                                "username": "alice",
                                "email": "alice@example.com",
                                "full_name": "Alice",
                                "is_active": True,
                                "created_at": "2024-01-01 09:00:00",
                                "birth_date": "1990-01-01",
                                "profile": {"k": "v"},
                            }
                        ],
                        fields=[
                            {
                                "name": "__parent_identity",
                                "dataTypeID": PostgresOID.INT4,
                            },
                            {"name": "id", "dataTypeID": PostgresOID.INT4},
                            {"name": "uuid", "dataTypeID": PostgresOID.UUID},
                            {"name": "username", "dataTypeID": PostgresOID.VARCHAR},
                            {"name": "email", "dataTypeID": PostgresOID.VARCHAR},
                            {"name": "full_name", "dataTypeID": PostgresOID.VARCHAR},
                            {"name": "is_active", "dataTypeID": PostgresOID.BOOL},
                            {"name": "created_at", "dataTypeID": PostgresOID.TIMESTAMP},
                            {"name": "birth_date", "dataTypeID": PostgresOID.DATE},
                            {"name": "profile", "dataTypeID": PostgresOID.JSONB},
                        ],
                        row_count=1,
                        command="SELECT",
                    )

                return QueryResult(
                    rows=[{"__parent_identity": 1, "id": 7, "name": "tag-a"}],
                    fields=[
                        {"name": "__parent_identity", "dataTypeID": PostgresOID.INT4},
                        {"name": "id", "dataTypeID": PostgresOID.INT4},
                        {"name": "name", "dataTypeID": PostgresOID.VARCHAR},
                    ],
                    row_count=1,
                    command="SELECT",
                )

            raise AssertionError(f"Unexpected SQL: {sql}")

        async def close(self):
            return None

    engine = NeonNativeAsyncEngine(mock_connection_string)
    fake = FakeClient()
    engine._client = fake

    stmt = (
        sa.select(Post)
        .where(Post.id == 1)
        .options(
            orm.selectinload(Post.author),
            orm.selectinload(Post.tags),
        )
    )
    result = await engine.execute(stmt)
    post = result.scalar_one()

    assert post.author.username == "alice"
    assert [tag.name for tag in post.tags] == ["tag-a"]
    assert fake.max_active >= 2
