from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Sequence

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Executable


@dataclass(slots=True)
class PoolQuery:
    """A single query task that runs on a pooled connection."""

    statement: Executable
    params: dict[str, Any] | tuple[Any, ...] | None = None


@dataclass(slots=True)
class PoolQueryResult:
    """Materialized query result safe to use after connection release."""

    keys: tuple[str, ...]
    rows: list[tuple[Any, ...]]
    rowcount: int


async def execute_pool_query(
    engine: AsyncEngine, query: PoolQuery
) -> PoolQueryResult:
    """Execute one statement using a dedicated pooled connection."""
    async with engine.connect() as connection:
        result = await connection.execute(query.statement, query.params)
        keys = tuple(result.keys())
        rows = [tuple(row) for row in result.fetchall()]
        return PoolQueryResult(keys=keys, rows=rows, rowcount=result.rowcount)


async def execute_pool_queries(
    engine: AsyncEngine,
    queries: Sequence[PoolQuery],
    *,
    max_concurrency: int | None = None,
) -> list[PoolQueryResult]:
    """Execute many statements in parallel through the SQLAlchemy connection pool."""
    if max_concurrency is not None and max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")

    semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    async def run_one(query: PoolQuery) -> PoolQueryResult:
        if semaphore is None:
            return await execute_pool_query(engine, query)
        async with semaphore:
            return await execute_pool_query(engine, query)

    tasks = [run_one(query) for query in queries]
    return await asyncio.gather(*tasks)
