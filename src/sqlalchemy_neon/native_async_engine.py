"""Native async execution layer for Neon without SQLAlchemy's sync proxy.

This module is intentionally independent from ``sqlalchemy.ext.asyncio.AsyncEngine``.
It compiles SQLAlchemy Core statements and executes them directly through
``AsyncNeonHTTPClient``.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.result import IteratorResult, SimpleResultMetaData
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.sql import ClauseElement

from .neon_http_client import AsyncNeonHTTPClient, QueryOptions, QueryResult
from .types import TypeConverter

_PYFORMAT_TOKEN = re.compile(r"%\(([^)]+)\)s")
_TYPE_CONVERTER = TypeConverter()
_PgDialect = postgresql.psycopg.PGDialect_psycopg

def _coerce_param_mapping(
    compiled: Any,
    parameters: Mapping[str, Any] | Sequence[Any] | None,
) -> Mapping[str, Any]:
    if parameters is None:
        return compiled.params

    if isinstance(parameters, Mapping):
        return compiled.construct_params(parameters)

    if compiled.positiontup is None:
        raise TypeError("Positional parameters require compiled.positiontup metadata")

    if len(parameters) != len(compiled.positiontup):
        raise ValueError(
            "Positional parameter count does not match statement bind parameters"
        )

    return dict(zip(compiled.positiontup, parameters))


def _pyformat_to_numeric(
    sql: str,
    values: Mapping[str, Any],
) -> tuple[str, list[Any]]:
    bound: dict[str, int] = {}
    ordered_values: list[Any] = []

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise KeyError(f"Missing value for SQL bind parameter '{key}'")

        if key not in bound:
            bound[key] = len(ordered_values) + 1
            ordered_values.append(values[key])

        return f"${bound[key]}"

    converted_sql = _PYFORMAT_TOKEN.sub(replace, sql)
    return converted_sql, ordered_values


def compile_sql(
    statement: str | ClauseElement,
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
) -> tuple[str, list[Any]]:
    """Compile a SQLAlchemy Core statement into Neon HTTP SQL + parameters."""
    if isinstance(statement, str):
        if parameters is None:
            return statement, []

        if isinstance(parameters, Mapping):
            raise TypeError(
                "String SQL requires positional parameters (list/tuple) with $1 binds"
            )
        return statement, list(parameters)

    compiled = statement.compile(
        dialect=_PgDialect(paramstyle="pyformat"),
        compile_kwargs={"render_postcompile": True},
    )
    mapping = _coerce_param_mapping(compiled, parameters)
    return _pyformat_to_numeric(str(compiled), mapping)


class NativeAsyncResult:
    """SQLAlchemy-compatible result wrapper for native async execution."""

    __slots__ = ("raw", "_result")

    def __init__(
        self,
        raw: QueryResult,
        *,
        statement: str | ClauseElement | None = None,
        entity_rows: list[tuple[Any, ...]] | None = None,
        entity_keys: list[str] | None = None,
    ) -> None:
        self.raw = raw
        self._result = _build_sa_result(
            raw,
            statement=statement,
            entity_rows=entity_rows,
            entity_keys=entity_keys,
        )

    def all(self) -> list[Any]:
        return self._result.all()

    def first(self) -> Any | None:
        return self._result.first()

    def one(self) -> Any:
        return self._result.one()

    def one_or_none(self) -> Any | None:
        return self._result.one_or_none()

    def scalar(self) -> Any | None:
        return self._result.scalar()

    def scalars(self, index: int | str = 0):
        return self._result.scalars(index=index)

    def mappings(self):
        return self._result.mappings()

    def unique(self, strategy=None):
        return self._result.unique(strategy=strategy)

    def scalar_one(self) -> Any:
        return self._result.scalar_one()

    def scalar_one_or_none(self) -> Any | None:
        return self._result.scalar_one_or_none()

    def __iter__(self):
        return iter(self._result)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)


def _normalize_raw_rows(raw: QueryResult) -> tuple[list[str], list[tuple[Any, ...]]]:
    keys = [field.get("name", "") for field in raw.fields]
    oid_by_key = {field.get("name", ""): field.get("dataTypeID") for field in raw.fields}
    rows: list[tuple[Any, ...]] = []

    for row in raw.rows:
        if isinstance(row, dict):
            converted = []
            for key in keys:
                value = row.get(key)
                oid = oid_by_key.get(key)
                if value is not None and isinstance(value, str) and isinstance(oid, int):
                    try:
                        value = _TYPE_CONVERTER.pg_to_python(value, oid)
                    except Exception:
                        pass
                converted.append(value)
            rows.append(tuple(converted))
        elif isinstance(row, (list, tuple)):
            converted = []
            for value, field in zip(row, raw.fields):
                oid = field.get("dataTypeID")
                if value is not None and isinstance(value, str) and isinstance(oid, int):
                    try:
                        value = _TYPE_CONVERTER.pg_to_python(value, oid)
                    except Exception:
                        pass
                converted.append(value)
            rows.append(tuple(converted))
        else:
            rows.append((row,))

    return keys, rows


def _extract_single_entity(statement: ClauseElement | None) -> tuple[str, Any] | None:
    if statement is None:
        return None

    descriptions = getattr(statement, "column_descriptions", None)
    if not descriptions or len(descriptions) != 1:
        return None

    desc = descriptions[0]
    entity = desc.get("entity")
    expr = desc.get("expr")
    if entity is None or entity is not expr:
        return None

    try:
        mapper = sa_inspect(entity)
    except Exception:
        return None

    if not hasattr(mapper, "column_attrs"):
        return None
    return desc.get("name") or entity.__name__, mapper


def _row_to_entity(
    mapper: Any,
    row: Mapping[str, Any],
    *,
    dialect: Any,
) -> Any:
    values: dict[str, Any] = {}
    for attr in mapper.column_attrs:
        column = attr.columns[0]
        candidates = (
            attr.key,
            column.key,
            column.name,
            f"{column.table.name}_{column.name}",
            f"{mapper.local_table.name}_{column.name}",
        )
        for key in candidates:
            if key in row:
                value = row[key]
                processor = column.type.result_processor(dialect, None)
                if processor is not None and value is not None:
                    try:
                        value = processor(value)
                    except Exception:
                        pass
                values[attr.key] = value
                break

    return mapper.class_(**values)


def _apply_type_processors(
    statement: ClauseElement | None,
    keys: list[str],
    rows: list[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    if statement is None:
        return rows

    compiled = statement.compile(
        dialect=_PgDialect(paramstyle="pyformat"),
        compile_kwargs={"render_postcompile": True},
    )
    result_columns = getattr(compiled, "_result_columns", None)
    if not result_columns or len(result_columns) != len(keys):
        return rows

    dialect = _PgDialect()
    processors = []
    for entry in result_columns:
        processor = entry.type.result_processor(dialect, None)
        processors.append(processor)

    if not any(processors):
        return rows

    converted_rows: list[tuple[Any, ...]] = []
    for row in rows:
        converted = list(row)
        for idx, processor in enumerate(processors):
            if processor is None:
                continue
            value = converted[idx]
            if value is None:
                continue
            try:
                converted[idx] = processor(value)
            except Exception:
                # Rows may already be converted by the HTTP layer.
                converted[idx] = value
        converted_rows.append(tuple(converted))

    return converted_rows


def _build_sa_result(
    raw: QueryResult,
    *,
    statement: str | ClauseElement | None = None,
    entity_rows: list[tuple[Any, ...]] | None = None,
    entity_keys: list[str] | None = None,
) -> IteratorResult[Any]:
    if entity_rows is not None:
        keys = entity_keys if entity_keys is not None else ["entity"]
        return IteratorResult(SimpleResultMetaData(keys), iter(entity_rows))

    keys, rows = _normalize_raw_rows(raw)

    entity_info = _extract_single_entity(statement if isinstance(statement, ClauseElement) else None)
    if entity_info is not None:
        entity_label, mapper = entity_info
        dialect = _PgDialect()
        row_maps = [dict(zip(keys, row)) for row in rows]
        entity_rows = [
            (_row_to_entity(mapper, row_map, dialect=dialect),)
            for row_map in row_maps
        ]
        return IteratorResult(SimpleResultMetaData([entity_label]), iter(entity_rows))

    if isinstance(statement, ClauseElement):
        rows = _apply_type_processors(statement, keys, rows)

    return IteratorResult(SimpleResultMetaData(keys), iter(rows))


class NeonNativeAsyncEngine:
    """Fully async Neon engine without SQLAlchemy sync/greenlet proxying."""

    def __init__(
        self,
        connection_string: str,
        *,
        http_client: Any | None = None,
        auth_token: str | None = None,
        timeout: float | None = 30.0,
    ) -> None:
        self._client = AsyncNeonHTTPClient(
            connection_string,
            http_client=http_client,
            auth_token=auth_token,
            timeout=timeout,
        )

    async def execute(
        self,
        statement: str | ClauseElement,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        options: QueryOptions | None = None,
    ) -> NativeAsyncResult:
        sql, params = compile_sql(statement, parameters)
        raw = await self._client.query(sql, params, options=options)

        entity_info = _extract_single_entity(
            statement if isinstance(statement, ClauseElement) else None
        )
        if entity_info is None:
            return NativeAsyncResult(raw, statement=statement)

        entity_label, mapper = entity_info
        keys, rows = _normalize_raw_rows(raw)
        row_maps = [dict(zip(keys, row)) for row in rows]
        dialect = _PgDialect()
        entities = [_row_to_entity(mapper, row_map, dialect=dialect) for row_map in row_maps]

        if isinstance(statement, ClauseElement):
            await self._hydrate_loader_options(
                entities,
                statement=statement,
                options=options,
            )

        entity_rows = [(entity,) for entity in entities]
        return NativeAsyncResult(
            raw,
            statement=statement,
            entity_rows=entity_rows,
            entity_keys=[entity_label],
        )

    async def transaction(
        self,
        statements: Sequence[
            tuple[str | ClauseElement, Mapping[str, Any] | Sequence[Any] | None]
        ],
    ) -> list[NativeAsyncResult]:
        queries = [compile_sql(statement, params) for statement, params in statements]
        raw_results = await self._client.transaction(queries)
        return [
            NativeAsyncResult(item, statement=statement)
            for (statement, _), item in zip(statements, raw_results)
        ]

    async def dispose(self) -> None:

        await self._client.close()

    async def __aenter__(self) -> "NeonNativeAsyncEngine":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.dispose()

    def _extract_load_plans(self, statement: ClauseElement) -> list[dict[str, Any]]:
        plans: list[dict[str, Any]] = []
        with_options = getattr(statement, "_with_options", ())
        seen: set[tuple[str, ...]] = set()

        for load_opt in with_options:
            path = getattr(load_opt, "path", None)
            if path is None:
                continue

            relationships = [token for token in list(path) if hasattr(token, "direction")]
            if not relationships:
                continue

            signature = tuple(
                f"{rel.parent.class_.__name__}.{rel.key}" for rel in relationships
            )
            if signature in seen:
                continue
            seen.add(signature)

            strategies: list[str] = []
            for ctx in getattr(load_opt, "context", ()):
                lazy_strategy = "selectin"
                for key, value in getattr(ctx, "strategy", ()):
                    if key == "lazy":
                        lazy_strategy = value
                        break
                strategies.append(lazy_strategy)

            while len(strategies) < len(relationships):
                strategies.append("selectin")

            plans.append({"relationships": relationships, "strategies": strategies})

        return plans

    def _select_strategy(self, strategies: Sequence[str]) -> str:
        if "joined" in strategies:
            return "joined"
        if "subquery" in strategies:
            return "subquery"
        return "selectin"

    async def _load_relationship_batch(
        self,
        parents: list[Any],
        rel: Any,
        *,
        strategy: str,
        options: QueryOptions | None = None,
    ) -> tuple[dict[Any, list[Any]], list[Any]]:
        if not parents:
            return {}, []

        parent_cls = rel.parent.class_
        parent_pk_col = list(rel.parent.primary_key)[0]
        target_mapper = rel.mapper
        target_cols = [c for c in target_mapper.columns]
        target_pk_cols = list(target_mapper.primary_key)

        parent_ids: list[Any] = []
        seen_parent_ids: set[Any] = set()
        for parent in parents:
            pid = getattr(parent, parent_pk_col.key)
            if pid not in seen_parent_ids:
                seen_parent_ids.add(pid)
                parent_ids.append(pid)

        if not parent_ids:
            return {}, []

        parent_id_col = parent_pk_col.label("__parent_identity")

        if strategy == "subquery":
            ids_subq = (
                sa.select(parent_pk_col.label("__pid"))
                .where(parent_pk_col.in_(parent_ids))
                .subquery()
            )
            stmt = (
                sa.select(parent_id_col, *target_cols)
                .select_from(ids_subq)
                .join(parent_cls, parent_pk_col == ids_subq.c["__pid"])
                .join(getattr(parent_cls, rel.key))
            )
        else:
            stmt = (
                sa.select(parent_id_col, *target_cols)
                .select_from(parent_cls)
                .join(getattr(parent_cls, rel.key))
                .where(parent_pk_col.in_(parent_ids))
            )

        sql, params = compile_sql(stmt)
        raw = await self._client.query(sql, params, options=options)
        keys, rows = _normalize_raw_rows(raw)
        row_maps = [dict(zip(keys, row)) for row in rows]
        dialect = _PgDialect()

        by_parent: dict[Any, list[Any]] = {pid: [] for pid in parent_ids}
        per_parent_seen: dict[Any, set[tuple[Any, ...]]] = {pid: set() for pid in parent_ids}

        for row_map in row_maps:
            pid = row_map.get("__parent_identity")
            if pid is None:
                continue
            entity = _row_to_entity(target_mapper, row_map, dialect=dialect)
            child_identity = tuple(getattr(entity, c.key) for c in target_pk_cols)
            if child_identity in per_parent_seen[pid]:
                continue
            per_parent_seen[pid].add(child_identity)
            by_parent[pid].append(entity)

        loaded_children: list[Any] = []
        for children in by_parent.values():
            loaded_children.extend(children)

        return by_parent, loaded_children

    async def _hydrate_plan_level(
        self,
        parents: list[Any],
        active_plans: list[tuple[dict[str, Any], int]],
        *,
        options: QueryOptions | None = None,
    ) -> None:
        if not parents or not active_plans:
            return

        grouped: dict[Any, list[tuple[dict[str, Any], int]]] = {}
        for plan, idx in active_plans:
            rel = plan["relationships"][idx]
            grouped.setdefault(rel, []).append((plan, idx))

        async def hydrate_group(rel: Any, entries: list[tuple[dict[str, Any], int]]) -> None:
            strategy = self._select_strategy(
                [entry_plan["strategies"][entry_idx] for entry_plan, entry_idx in entries]
            )
            by_parent, loaded_children = await self._load_relationship_batch(
                parents,
                rel,
                strategy=strategy,
                options=options,
            )

            parent_pk_key = list(rel.parent.primary_key)[0].key
            for parent in parents:
                pid = getattr(parent, parent_pk_key)
                children = by_parent.get(pid, [])
                if rel.uselist:
                    setattr(parent, rel.key, children)
                else:
                    setattr(parent, rel.key, children[0] if children else None)

            next_entries: list[tuple[dict[str, Any], int]] = []
            for plan, idx in entries:
                if idx + 1 < len(plan["relationships"]):
                    next_entries.append((plan, idx + 1))

            if next_entries and loaded_children:
                await self._hydrate_plan_level(
                    loaded_children,
                    next_entries,
                    options=options,
                )

        await asyncio.gather(
            *(hydrate_group(rel, entries) for rel, entries in grouped.items())
        )

    async def _hydrate_loader_options(
        self,
        root_entities: list[Any],
        *,
        statement: ClauseElement,
        options: QueryOptions | None = None,
    ) -> None:
        if not root_entities:
            return

        plans = self._extract_load_plans(statement)
        if not plans:
            return

        await self._hydrate_plan_level(
            root_entities,
            [(plan, 0) for plan in plans],
            options=options,
        )


def create_neon_native_async_engine(
    connection_string: str,
    *,
    http_client: Any | None = None,
    auth_token: str | None = None,
    timeout: float | None = 30.0,
) -> NeonNativeAsyncEngine:
    """Create a fully async Neon engine that bypasses SQLAlchemy AsyncEngine."""
    return NeonNativeAsyncEngine(
        connection_string,
        http_client=http_client,
        auth_token=auth_token,
        timeout=timeout,
    )
