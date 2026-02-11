import asyncio
import os
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import loading, context
from sqlalchemy import util
import logfire

# Use existing models from qqqqq.py environment if possible, or define minimal ones.
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users_test_hack"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


async def main():
    # Use SQLite for testing the logic without credentials
    url = "sqlite+aiosqlite:///:memory:"

    engine = create_async_engine(
        url,
        echo=True,
    )

    try:
        async with engine.begin() as conn:
            # SQLite doesn't support schema creation inside transaction sometimes?
            # ensure usage of run_sync for DDL
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(sa.insert(User).values(id=1, name="Alice"))
            await conn.execute(sa.insert(User).values(id=2, name="Bob"))

        # Now the hack
        async with engine.connect() as conn:
            stmt = sa.select(User)
            result = await conn.execute(stmt)

            print("Using result.mappings() unpacking:")
            # result.mappings() returns an iterator of RowMapping, which behaves like a dict
            users = [User(**row) for row in result.mappings().fetchall()]

            for u in users:
                print(f"User: {u.id} {u.name}")

            # Verify that this object is "detached" (no session state)
            try:
                print(f"User state: {sa.inspect(users[0]).transient}")  # Should be True
            except Exception as e:
                print(f"Inspection error: {e}")

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
