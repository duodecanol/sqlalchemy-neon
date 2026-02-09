from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from sqlalchemy_neon.pool import PoolQuery, execute_pool_queries, execute_pool_query


class _FakeResult:
    def __init__(self, *, rows: list[tuple], keys: tuple[str, ...], rowcount: int):
        self._rows = rows
        self._keys = keys
        self.rowcount = rowcount

    def keys(self):
        return self._keys

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, engine, delay: float):
        self._engine = engine
        self._delay = delay

    async def __aenter__(self):
        self._engine.active += 1
        self._engine.max_active = max(self._engine.max_active, self._engine.active)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._engine.active -= 1

    async def execute(self, statement, params):
        await asyncio.sleep(self._delay)
        return _FakeResult(
            rows=[(str(statement), params)],
            keys=("statement", "params"),
            rowcount=1,
        )


class _FakeEngine:
    def __init__(self, *, delay: float = 0.01):
        self.delay = delay
        self.active = 0
        self.max_active = 0

    def connect(self):
        return _FakeConnection(self, self.delay)


@pytest.mark.asyncio
async def test_execute_pool_query_materializes_rows():
    engine = _FakeEngine()
    query = PoolQuery(text("SELECT 1"), params={"id": 1})

    result = await execute_pool_query(engine, query)

    assert result.keys == ("statement", "params")
    assert result.rowcount == 1
    assert len(result.rows) == 1
    assert result.rows[0][1] == {"id": 1}


@pytest.mark.asyncio
async def test_execute_pool_queries_uses_parallel_connections():
    engine = _FakeEngine(delay=0.03)
    queries = [PoolQuery(text("SELECT 1")) for _ in range(5)]

    results = await execute_pool_queries(engine, queries, max_concurrency=5)

    assert len(results) == 5
    assert engine.max_active >= 2


@pytest.mark.asyncio
async def test_execute_pool_queries_validates_max_concurrency():
    engine = _FakeEngine()

    with pytest.raises(ValueError):
        await execute_pool_queries(engine, [PoolQuery(text("SELECT 1"))], max_concurrency=0)
