"""Native async execution layer for Neon without SQLAlchemy's sync proxy.

This module is intentionally independent from ``sqlalchemy.ext.asyncio.AsyncEngine``.
It compiles SQLAlchemy Core statements and executes them directly through
``AsyncNeonHTTPClient``.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Awaitable, Callable, Literal, Mapping, Sequence

import aiohttp
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.result import IteratorResult, SimpleResultMetaData
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.sql import ClauseElement

from sqlalchemy_neon.neon_http_client import IsolationLevel

from .neon_http_client import (
    AsyncNeonHTTPClient,
    AsyncNeonWebSocketPool,
    QueryOptions,
    QueryResult,
    TransactionOptions,
)
from .types import TypeConverter

_PYFORMAT_TOKEN = re.compile(r"%\(([^)]+)\)s")
_TYPE_CONVERTER = TypeConverter()
_PgDialect = postgresql.psycopg.PGDialect_psycopg
_NO_DEFAULT = object()


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

    def all(self) -> Sequence[sa.Row]:
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
    oid_by_key = {
        field.get("name", ""): field.get("dataTypeID") for field in raw.fields
    }
    rows: list[tuple[Any, ...]] = []

    for row in raw.rows:
        if isinstance(row, dict):
            converted = []
            for key in keys:
                value = row.get(key)
                oid = oid_by_key.get(key)
                if (
                    value is not None
                    and isinstance(value, str)
                    and isinstance(oid, int)
                ):
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
                if (
                    value is not None
                    and isinstance(value, str)
                    and isinstance(oid, int)
                ):
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

    entity_info = _extract_single_entity(
        statement if isinstance(statement, ClauseElement) else None
    )
    if entity_info is not None:
        entity_label, mapper = entity_info
        dialect = _PgDialect()
        row_maps = [dict(zip(keys, row)) for row in rows]
        entity_rows = [
            (_row_to_entity(mapper, row_map, dialect=dialect),) for row_map in row_maps
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
        http_client: aiohttp.ClientSession
        | Callable[[], Awaitable[aiohttp.ClientSession]]
        | None = None,
        auth_token: str | None = None,
        timeout: float | None = None,
        fetch_endpoint: str | Callable[[str, int, bool], str] | None = None,
        fetch_function: (
            Callable[[str, str, dict[str, str]], Awaitable[tuple[int, str]]] | None
        ) = None,
        transport: Literal["http", "websocket"] = "http",
        websocket_pool_size: int = 10,
        ws_proxy: str | Callable[[str, int], str] | None = None,
        use_secure_websocket: bool = True,
        websocket_heartbeat: float | None = 30.0,
    ) -> None:
        # http_client type check
        if http_client is not None and not isinstance(
            http_client, aiohttp.ClientSession
        ):
            if not callable(http_client):
                raise TypeError(
                    "http_client must be an aiohttp.ClientSession or a callable returning one"
                )
        if transport == "http":
            self._client = AsyncNeonHTTPClient(
                connection_string,
                http_client=http_client,
                auth_token=auth_token,
                timeout=timeout,
                fetch_endpoint=fetch_endpoint,
                fetch_function=fetch_function,
            )
        elif transport == "websocket":
            self._client = AsyncNeonWebSocketPool(
                connection_string,
                max_size=websocket_pool_size,
                http_client=http_client,
                auth_token=auth_token,
                timeout=timeout,
                ws_proxy=ws_proxy,
                use_secure_websocket=use_secure_websocket,
                heartbeat=websocket_heartbeat,
            )
        else:
            raise ValueError(
                f"Unsupported transport '{transport}'. Expected 'http' or 'websocket'."
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
        entities = [
            _row_to_entity(mapper, row_map, dialect=dialect) for row_map in row_maps
        ]

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
        *,
        options: TransactionOptions | None = None,
    ) -> list[NativeAsyncResult]:
        queries = [compile_sql(statement, params) for statement, params in statements]

        options = (
            options
            if options is not None
            else TransactionOptions(
                read_only=True,
                isolation_level=IsolationLevel.SERIALIZABLE,
                deferrable=True,
            )
        )
        raw_results = await self._client.transaction(queries, options=options)
        return [
            NativeAsyncResult(item, statement=statement)
            for (statement, _), item in zip(statements, raw_results)
        ]

    async def add(self, instance: Any) -> None:
        """Add an instance to the database."""
        await self.add_all([instance])

    async def add_all(self, instances: Sequence[Any]) -> None:
        """Add multiple instances in one transaction."""
        if not instances:
            return

        statements: list[
            tuple[str | ClauseElement, Mapping[str, Any] | Sequence[Any] | None]
        ] = []
        tables: list[Any] = []
        for instance in instances:
            insert_stmt, params, table = self._build_insert_statement(instance)
            statements.append((insert_stmt, params))
            tables.append(table)

        results = await self.transaction(
            statements,
            options=TransactionOptions(read_only=False),
        )

        for instance, table, result in zip(instances, tables, results):
            self._apply_returning_values(instance, table, result)

    async def delete(self, instance: Any) -> None:
        """Delete a persisted instance."""
        await self.delete_all([instance])

    async def delete_all(self, instances: Sequence[Any]) -> None:
        """Delete multiple persisted instances in one transaction."""
        if not instances:
            return

        statements: list[
            tuple[str | ClauseElement, Mapping[str, Any] | Sequence[Any] | None]
        ] = []
        for instance in instances:
            mapper = sa_inspect(instance).mapper
            table = mapper.local_table
            pk_cols = list(table.primary_key)
            if not pk_cols:
                raise ValueError("Cannot delete instance without primary key columns")

            params: dict[str, Any] = {}
            criteria = []
            for pk_col in pk_cols:
                value = getattr(instance, pk_col.key, None)
                if value is None:
                    raise ValueError(
                        f"Cannot delete instance with unset primary key '{pk_col.key}'"
                    )
                bind_name = f"pk_{pk_col.key}"
                criteria.append(pk_col == sa.bindparam(bind_name))
                params[bind_name] = value

            delete_stmt = sa.delete(table).where(sa.and_(*criteria))
            statements.append((delete_stmt, params))

        await self.transaction(
            statements,
            options=TransactionOptions(read_only=False),
        )

    def _build_insert_statement(
        self, instance: Any
    ) -> tuple[ClauseElement, dict[str, Any], Any]:
        mapper = sa_inspect(instance).mapper
        table = mapper.local_table
        insert_values: dict[str, Any] = {}
        params: dict[str, Any] = {}

        for column in table.columns:
            if column.primary_key and column.autoincrement:
                continue

            value = getattr(instance, column.key, None)

            # If value is unset, prefer Python defaults, then server defaults.
            if value is None:
                resolved_default = self._resolve_python_default(column.default)
                if resolved_default is not _NO_DEFAULT:
                    value = resolved_default
                    setattr(instance, column.key, value)
                elif column.server_default is not None:
                    # Omit column so the database applies its server-side default.
                    continue

            if value is None and column.default is not None:
                default_arg = getattr(column.default, "arg", None)
                if getattr(column.default, "is_clause_element", False):
                    insert_values[column.key] = default_arg
                    continue

            insert_values[column.key] = sa.bindparam(column.key)
            params[column.key] = value

        insert_stmt = sa.insert(table).values(insert_values).returning(table)
        return insert_stmt, params, table

    def _resolve_python_default(self, default: Any) -> Any:
        if default is None:
            return _NO_DEFAULT

        if getattr(default, "is_scalar", False):
            return default.arg

        if getattr(default, "is_callable", False):
            fn = default.arg
            try:
                return fn(None)
            except TypeError:
                return fn()

        return _NO_DEFAULT

    def _apply_returning_values(
        self, instance: Any, table: Any, result: NativeAsyncResult
    ) -> None:
        row = result.mappings().first()
        if row is None:
            return

        for col in table.columns:
            if col.key in row:
                setattr(instance, col.key, row[col.key])
                continue
            if col.name in row:
                setattr(instance, col.key, row[col.name])
                continue

            prefixed_key = f"{table.name}_{col.name}"
            if prefixed_key in row:
                setattr(instance, col.key, row[prefixed_key])

    def _extract_load_plans(self, statement: ClauseElement) -> list[dict[str, Any]]:
        plans: list[dict[str, Any]] = []
        with_options = getattr(statement, "_with_options", ())
        seen: set[tuple[str, ...]] = set()

        for load_opt in with_options:
            path = getattr(load_opt, "path", None)
            if path is None:
                continue

            relationships = [
                token for token in list(path) if hasattr(token, "direction")
            ]
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
        per_parent_seen: dict[Any, set[tuple[Any, ...]]] = {
            pid: set() for pid in parent_ids
        }

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

        async def hydrate_group(
            rel: Any, entries: list[tuple[dict[str, Any], int]]
        ) -> None:
            strategy = self._select_strategy(
                [
                    entry_plan["strategies"][entry_idx]
                    for entry_plan, entry_idx in entries
                ]
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

    async def dispose(self) -> None:
        await self._client.close()
        external_client = getattr(self._client, "_external_client", False)
        raw_http_client = getattr(self._client, "_http_client", None)
        if (
            external_client
            and raw_http_client is not None
            and hasattr(raw_http_client, "closed")
            and not raw_http_client.closed
            and hasattr(self._client, "force_close")
        ):
            await self._client.force_close()

    async def __aenter__(self) -> "NeonNativeAsyncEngine":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.dispose()


def create_neon_http_engine(
    connection_string: str,
    *,
    http_client: Any | None = None,
    auth_token: str | None = None,
    timeout: float | None = 30.0,
    fetch_endpoint: str | Callable[[str, int, bool], str] | None = None,
    fetch_function: (
        Callable[[str, str, dict[str, str]], Awaitable[tuple[int, str]]] | None
    ) = None,
) -> NeonNativeAsyncEngine:
    """Create an async Neon engine using the HTTP transport."""
    return NeonNativeAsyncEngine(
        connection_string,
        http_client=http_client,
        auth_token=auth_token,
        timeout=timeout,
        fetch_endpoint=fetch_endpoint,
        fetch_function=fetch_function,
        transport="http",
    )


def create_neon_ws_engine(
    connection_string: str,
    *,
    http_client: Any | None = None,
    auth_token: str | None = None,
    timeout: float | None = 30.0,
    pool_size: int = 10,
    ws_proxy: str | Callable[[str, int], str] | None = None,
    use_secure_websocket: bool = True,
    heartbeat: float | None = 30.0,
) -> NeonNativeAsyncEngine:
    """Create an async Neon engine using the WebSocket transport."""
    return NeonNativeAsyncEngine(
        connection_string,
        http_client=http_client,
        auth_token=auth_token,
        timeout=timeout,
        transport="websocket",
        websocket_pool_size=pool_size,
        ws_proxy=ws_proxy,
        use_secure_websocket=use_secure_websocket,
        websocket_heartbeat=heartbeat,
    )


def create_neon_native_async_engine(
    connection_string: str,
    *,
    http_client: Any | None = None,
    auth_token: str | None = None,
    timeout: float | None = 30.0,
    fetch_endpoint: str | Callable[[str, int, bool], str] | None = None,
    fetch_function: (
        Callable[[str, str, dict[str, str]], Awaitable[tuple[int, str]]] | None
    ) = None,
    transport: Literal["http", "websocket"] = "http",
    websocket_pool_size: int = 10,
    ws_proxy: str | Callable[[str, int], str] | None = None,
    use_secure_websocket: bool = True,
    websocket_heartbeat: float | None = 30.0,
) -> NeonNativeAsyncEngine:
    """Create an async Neon engine. Prefer ``create_neon_http_engine`` or
    ``create_neon_ws_engine`` for clearer intent."""
    return NeonNativeAsyncEngine(
        connection_string,
        http_client=http_client,
        auth_token=auth_token,
        timeout=timeout,
        fetch_endpoint=fetch_endpoint,
        fetch_function=fetch_function,
        transport=transport,
        websocket_pool_size=websocket_pool_size,
        ws_proxy=ws_proxy,
        use_secure_websocket=use_secure_websocket,
        websocket_heartbeat=websocket_heartbeat,
    )
