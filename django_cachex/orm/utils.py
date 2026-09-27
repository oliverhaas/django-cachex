"""Cache keys and table extraction for ORM queries."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import datetime
from decimal import Decimal
from hashlib import sha1
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from django.apps import apps
from django.contrib.postgres.functions import TransactionNow
from django.db.models import Exists, QuerySet, Subquery, deletion
from django.db.models.enums import Choices
from django.db.models.expressions import RawSQL
from django.db.models.functions import Now
from django.db.models.sql import AggregateQuery, Query
from django.db.models.sql.where import ExtraWhere, NothingNode, WhereNode

from django_cachex.orm.settings import ITERABLES, orm_settings

# The on_delete of foreign keys the database enforces itself (Django 6.1+).
_DATABASE_ON_DELETE: Any = getattr(deletion, "DatabaseOnDelete", None)

# Where get_query_cache_key leaves the lowercased SQL of a compiler.
_GENERATED_SQL = "_cachex_orm_generated_sql"

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.models import Model
    from django.db.models.expressions import BaseExpression
    from django.db.models.sql.compiler import SQLCompiler


class UncachableQuery(Exception):  # noqa: N818
    pass


class IsRawQuery(Exception):  # noqa: N818
    pass


CACHABLE_PARAM_TYPES: set[type] = {
    bool,
    int,
    float,
    Decimal,
    bytearray,
    bytes,
    str,
    type(None),
    datetime.date,
    datetime.time,
    datetime.datetime,
    datetime.timedelta,
    UUID,
}
UNCACHABLE_FUNCS: set[type] = {Now, TransactionNow}


def _psycopg_param_types() -> tuple[type, ...]:
    from ipaddress import IPv4Address, IPv6Address

    from psycopg.dbapi20 import Binary
    from psycopg.types.json import Json, Jsonb
    from psycopg.types.numeric import Float4, Float8, Int2, Int4, Int8
    from psycopg.types.range import Range

    return (Binary, Range, Json, Jsonb, Int2, Int4, Int8, Float4, Float8, IPv4Address, IPv6Address)


def _psycopg2_param_types() -> tuple[type, ...]:
    from psycopg2 import Binary  # ty: ignore[unresolved-import]
    from psycopg2.extras import (  # ty: ignore[unresolved-import]
        DateRange,
        DateTimeRange,
        DateTimeTZRange,
        Inet,
        Json,
        NumericRange,
    )

    return (Binary, NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet, Json)


# Parameter types of the PostgreSQL driver Django uses: psycopg, else psycopg2.
for _driver_param_types in (_psycopg_param_types, _psycopg2_param_types):
    try:
        CACHABLE_PARAM_TYPES.update(_driver_param_types())
    except ImportError:
        continue
    break


def check_parameter_types(params: Iterable[Any]) -> None:
    for p in params:
        cl = p.__class__
        if cl not in CACHABLE_PARAM_TYPES:
            if cl in ITERABLES:
                check_parameter_types(p)
            elif cl is dict:
                check_parameter_types(p.items())
            elif issubclass(cl, Choices):
                # Choices are unique enums, so the underlying value decides.
                check_parameter_types([p.value])
            else:
                raise UncachableQuery


def get_query_cache_key(compiler: SQLCompiler) -> str:
    """Return a cache key for the query of ``compiler``, specific to its SQL and database."""
    sql, params = compiler.as_sql()
    check_parameter_types(params)
    cache_key = f"{compiler.using}:{sql}:{[str(p) for p in params]}"
    # Kept for the final SQL check, which would otherwise call as_sql() again.
    setattr(compiler, _GENERATED_SQL, sql.lower())

    return sha1(cache_key.encode("utf-8")).hexdigest()  # noqa: S324


def get_table_cache_key(db_alias: str, table: str) -> str:
    """Return a cache key for ``table`` of database ``db_alias``."""
    cache_key = f"{db_alias}:{table}"
    return sha1(cache_key.encode("utf-8")).hexdigest()  # noqa: S324


def _get_tables_from_sql(
    connection: BaseDatabaseWrapper,
    lowercased_sql: str,
    *,
    enable_quote: bool = False,
) -> set[str]:
    """Return the tables named in the final SQL of a query."""
    return {
        table
        for table in (connection.introspection.django_table_names() + orm_settings.ADDITIONAL_TABLES)
        if _quote_table_name(table, connection, enable_quote=enable_quote) in lowercased_sql
    }


def _quote_table_name(table_name: str, connection: BaseDatabaseWrapper, *, enable_quote: bool) -> str:
    """Quote ``table_name`` so ``ormtest_testparent`` does not also match ``ormtest_test``."""
    return f"{connection.ops.quote_name(table_name)}" if enable_quote else table_name


def _find_rhs_lhs_subquery(side: Any) -> Query | None:
    h_class = side.__class__
    if h_class is Query:
        return side
    if h_class is QuerySet:
        return side.query
    if h_class in (Subquery, Exists):  # Subquery allows QuerySet & Query
        return side.query.query if side.query.__class__ is QuerySet else side.query
    if h_class in UNCACHABLE_FUNCS:
        raise UncachableQuery
    return None


def _find_subqueries_in_where(children: Iterable[Any]) -> Iterator[Query]:
    for child in children:
        child_class = child.__class__
        if child_class is WhereNode:
            yield from _find_subqueries_in_where(child.children)
        elif child_class is ExtraWhere:
            raise IsRawQuery
        elif child_class is NothingNode:
            pass
        else:
            try:
                child_rhs = child.rhs
                child_lhs = child.lhs
            except AttributeError as e:
                raise UncachableQuery from e
            rhs = _find_rhs_lhs_subquery(child_rhs)
            if rhs is not None:
                yield rhs
            lhs = _find_rhs_lhs_subquery(child_lhs)
            if lhs is not None:
                yield lhs


def is_cachable(table: str) -> bool:
    whitelist = orm_settings.ONLY_CACHABLE_TABLES
    if whitelist and table not in whitelist:
        return False
    return table not in orm_settings.UNCACHABLE_TABLES


def are_all_cachable(tables: set[str]) -> bool:
    whitelist = orm_settings.ONLY_CACHABLE_TABLES
    if whitelist and not tables.issubset(whitelist):
        return False
    return tables.isdisjoint(orm_settings.UNCACHABLE_TABLES)


def filter_cachable(tables: set[str]) -> set[str]:
    whitelist = orm_settings.ONLY_CACHABLE_TABLES
    tables = tables.difference(orm_settings.UNCACHABLE_TABLES)
    if whitelist:
        return tables.intersection(whitelist)
    return tables


def _flatten(expression: BaseExpression) -> Iterator[Any]:
    """Yield ``expression`` and all its subexpressions, depth first."""
    yield expression
    for expr in expression.get_source_expressions():
        if expr:
            if hasattr(expr, "flatten"):
                yield from _flatten(expr)
            else:
                yield expr


def _get_tables(db_alias: str, query: Query, compiler: SQLCompiler | None = None) -> set[str]:  # noqa: C901, PLR0912
    from django.db import connections

    if query.select_for_update or (not orm_settings.CACHE_RANDOM and "?" in query.order_by):
        raise UncachableQuery

    try:
        if query.extra_select:
            raise IsRawQuery  # noqa: TRY301

        # Gets all tables already found by the ORM.
        tables = set(query.table_map)
        if query.get_meta():
            tables.add(query.get_meta().db_table)

        # Gets tables in subquery annotations.
        for annotation in query.annotations.values():
            if type(annotation) in UNCACHABLE_FUNCS:
                raise UncachableQuery
            for expression in _flatten(annotation):
                if isinstance(expression, Subquery):
                    tables.update(_get_tables(db_alias, expression.query))
                # Django 6.0+: Subquery.resolve_expression() returns the Query
                # itself, flagged with subquery=True.
                elif isinstance(expression, Query) and getattr(expression, "subquery", False):
                    tables.update(_get_tables(db_alias, expression))
                elif isinstance(expression, RawSQL):
                    tables.update(_get_tables_from_sql(connections[db_alias], expression.sql.lower()))
        # Gets tables in WHERE subqueries.
        for subquery in _find_subqueries_in_where(query.where.children):
            tables.update(_get_tables(db_alias, subquery))
        # Gets tables in HAVING subqueries.
        if isinstance(query, AggregateQuery):
            tables.update(_get_tables(db_alias, query.inner_query))
        # Gets tables in combined queries
        # using `.union`, `.intersection`, or `difference`.
        if query.combined_queries:
            for combined_query in query.combined_queries:
                tables.update(_get_tables(db_alias, combined_query))
    except IsRawQuery:
        sql = query.get_compiler(db_alias).as_sql()[0].lower()
        tables = _get_tables_from_sql(connections[db_alias], sql)
    else:
        # Safety net for expressions the checks above do not handle yet: any
        # table named in the final SQL counts too.
        if orm_settings.FINAL_SQL_CHECK:
            # Stored by get_query_cache_key, saving another as_sql() call; a
            # custom QUERY_KEYGEN may not store it.
            final_sql = getattr(compiler, _GENERATED_SQL, None)
            if final_sql is None:
                final_sql = query.get_compiler(db_alias).as_sql()[0].lower()
            final_check_tables = _get_tables_from_sql(connections[db_alias], final_sql, enable_quote=True)
            tables.update(final_check_tables)

    if not are_all_cachable(tables):
        raise UncachableQuery
    return tables


def models_of_tables(tables: set[str]) -> list[type[Model]]:
    """Return the installed models stored in ``tables``."""
    return [model for model in apps.get_models(include_auto_created=True) if model._meta.db_table in tables]


def _concrete(model: type[Model]) -> type[Model]:
    return model._meta.concrete_model or model


def deletion_dependents(models: Iterable[type[Model]], *, truncate: bool = False) -> set[str]:
    """Return the tables the database itself changes when rows of ``models`` are deleted."""
    # A delete reaches the tables whose foreign keys have a database-level
    # on_delete (DB_CASCADE, DB_SET_NULL, DB_SET_DEFAULT), onwards through
    # DB_CASCADE. TRUNCATE ... CASCADE reaches every table with a foreign key
    # to a truncated one, onwards through all of them.
    tables: set[str] = set()
    pending = [_concrete(model) for model in models]
    seen: set[type[Model]] = set()
    while pending:
        model = pending.pop()
        if model in seen:
            continue
        seen.add(model)
        # Reverse relations (ForeignObjectRel), which the stubs type as fields.
        relations = cast("Iterable[Any]", deletion.get_candidate_relations_to_delete(model._meta))
        for relation in relations:
            on_delete = relation.field.remote_field.on_delete
            if truncate:
                onwards = True
            elif _DATABASE_ON_DELETE is not None and isinstance(on_delete, _DATABASE_ON_DELETE):
                onwards = on_delete.operation == "CASCADE"
            else:
                continue
            related = _concrete(relation.related_model)
            tables.add(related._meta.db_table)
            if onwards:
                pending.append(related)
    return tables
