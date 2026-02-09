from __future__ import annotations

import asyncio
from typing import Any, Callable, TypeVar, Iterable
from sqlalchemy_neon.neon_http_client import AsyncNeonHTTPClient

import sqlalchemy as sa
from sqlalchemy.orm import attributes, bulk_persistence
from sqlalchemy.util.concurrency import greenlet_spawn
from sqlalchemy.ext.asyncio.result import _ensure_sync_result


from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.base import object_mapper
from sqlalchemy.engine import URL
import rich
from .errors import NotSupportedError

_T = TypeVar("_T")


class NeonAsyncSession(AsyncSession):
    """Custom AsyncSession for Neon that enforces pure async usage.

    This session subclass disables the `run_sync` method to prevent users
    from falling back to synchronous execution, which would block the
    main thread in the Neon driver's async-only architecture.
    """

    async def run_sync(self, fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        """Disable run_sync to prevent greenlet blocking/complexity.

        The Neon driver is designed for pure async usage.
        """
        raise NotSupportedError(
            "NeonAsyncSession.run_sync() is not supported. "
            "Please use async methods directly to ensure non-blocking execution."
        )

    async def rollback(self) -> None:
        """Rollback the current transaction in progress."""
        raise NotSupportedError(
            "NeonAsyncSession.rollback() is not supported. "
            "Please use async methods directly to ensure non-blocking execution."
        )

    async def bulk_save_objects(
        self,
        objects: Iterable[object],
        return_defaults: bool = False,
        update_changed_only: bool = True,
        preserve_order: bool = True,
    ) -> None:
        raise NotSupportedError(
            "NeonAsyncSession.bulk_save_objects() is not supported. "
            "Please use async methods directly to ensure non-blocking execution."
        )

    async def add_all_cheat(self, instances: Iterable[Any]) -> None:
        """Add all instances to the session and generate corresponding INSERT statements.

        This implementation groups instances by table and extracts their data using
        SQLAlchemy mappers. Columns that are NULL across the entire batch are omitted
        to allow database-side defaults (like UUIDs or timestamps) to trigger.
        """
        # 1. Group by table and extract raw data
        by_table = {}
        for instance in instances:
            mapper = object_mapper(instance)
            table = mapper.local_table
            if table not in by_table:
                by_table[table] = []

            # Extract mapped column values, applying Python-side defaults if necessary
            values = {}
            for column in mapper.columns:
                val = getattr(instance, column.key)

                # If the value is NULL but a Python-side default is defined, apply it.
                # This ensures NOT NULL constraints are met for client-side generated values (e.g. UUIDs).
                if val is None and column.default is not None:
                    if column.default.is_callable:
                        # Attempt to call the default (handles lambda and zero-arg functions like uuid4)
                        try:
                            val = column.default.arg()
                        except TypeError:
                            # Fallback for callables expecting a context
                            val = column.default.arg(None)
                    elif column.default.is_scalar:
                        val = column.default.arg

                    # Sync back to the instance
                    setattr(instance, column.key, val)

                values[column.name] = val

            by_table[table].append(values)

        # 2. Process each table's batch to omit globally-NULL columns
        statements = []
        for table, values_list in by_table.items():
            if not values_list:
                continue

            # Identify columns that have at least one non-NULL value in this batch.
            # If a column is NULL for every row, it will be omitted from the INSERT.
            all_column_names = values_list[0].keys()

            active_columns = [
                col
                for col in all_column_names
                if any(row[col] is not None for row in values_list)
            ]

            # Re-filter the batch to only include active columns
            filtered_values = [
                {col: row[col] for col in active_columns} for row in values_list
            ]

            # If ALL columns were NULL (e.g. empty objects), we still insert
            # empty dicts to preserve the row count and trigger defaults.
            if not active_columns:
                filtered_values = [{} for _ in values_list]

            # 3. Create a lightweight table representation containing ONLY the active columns.
            # We include the original column types to ensure correct SQL literal rendering.
            lite_table = sa.table(
                table.name,
                *[
                    sa.column(col, type_=table.columns[col].type)
                    for col in active_columns
                ],
                schema=table.schema,
            )

            # 4. Create and print the INSERT statement using the lightweight table
            stmt = sa.insert(lite_table).values(filtered_values).returning(lite_table)
            statements.append(stmt)

            print(f"\n[Neon] Generated INSERT for table '{table.name}':")
            dialect = self.bind.dialect if self.bind else None
            print(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
            print("-" * (26 + len(table.name)))

        # conn = await self.connection()

        # <class 'sqlalchemy.pool.base._ConnectionFairy'>
        # conn = await self.bind.raw_connection()
        # <class 'sqlalchemy_neon.dbapi.AsyncAdapt_neon_cursor'>
        # cur = conn.cursor()

        url: URL = self.bind.url
        rich.inspect(url)
        url.drivername
        url = URL.create(
            "postgresql",
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=url.database,
        )
        client = AsyncNeonHTTPClient(url.render_as_string(hide_password=False))
        queries = []
        for stmt in statements:
            q = (
                stmt.compile(
                    dialect=self.bind.dialect, compile_kwargs={"literal_binds": True}
                ).string,
                None,
            )
            queries.append(q)

        async with client:
            results = await client.transaction(queries)

        print(results)

    # async def refresh(self, instance: Any, attribute_names: Sequence[str] | None = None, with_for_update: bool | Sequence[str] | None = None) -> None:
