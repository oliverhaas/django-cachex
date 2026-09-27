"""Cache keys and table extraction for ORM queries."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import datetime
from decimal import Decimal
from hashlib import sha1
from time import time
from typing import TYPE_CHECKING, Any
from uuid import UUID

from django.contrib.postgres.functions import TransactionNow
from django.db.models import Exists, QuerySet, Subquery
from django.db.models.enums import Choices
from django.db.models.expressions import RawSQL
from django.db.models.functions import Now
from django.db.models.sql import AggregateQuery, Query
from django.db.models.sql.where import ExtraWhere, NothingNode, WhereNode

from django_cachex.orm.settings import ITERABLES, orm_settings
from django_cachex.orm.transaction import AtomicCache

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from django.db.backends.base.base import BaseDatabaseWrapper
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


try:
    from ipaddress import IPv4Address, IPv6Address

    from django.db.backends.postgresql.psycopg_any import (
        DateRange,
        DateTimeRange,
        DateTimeTZRange,
        Inet,
        NumericRange,
    )
    from psycopg.dbapi20 import Binary
    from psycopg.types.json import Json, Jsonb
    from psycopg.types.numeric import Float4, Float8, Int2, Int4, Int8

    CACHABLE_PARAM_TYPES.update(
        (
            NumericRange,
            DateRange,
            DateTimeRange,
            DateTimeTZRange,
            Inet,
            Json,
            Jsonb,
            Int2,
            Int4,
            Int8,
            Float4,
            Float8,
            IPv4Address,
            IPv6Address,
            Binary,
        ),
    )
except ImportError:
    try:
        from psycopg2 import Binary  # ty: ignore[unresolved-import]
        from psycopg2.extras import (  # ty: ignore[unresolved-import]
            DateRange,
            DateTimeRange,
            DateTimeTZRange,
            Inet,
            Json,
            NumericRange,
        )

        CACHABLE_PARAM_TYPES.update((Binary, NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet, Json))
    except ImportError:
        pass


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
    compiler._cachex_orm_generated_sql = sql.lower()  # ty: ignore[unresolved-attribute]

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
                    sql = expression.as_sql(None, None)[0].lower()  # ty: ignore[invalid-argument-type]
                    tables.update(_get_tables_from_sql(connections[db_alias], sql))
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
            if compiler is not None:
                # Stored by get_query_cache_key, saving another as_sql() call.
                sql = compiler._cachex_orm_generated_sql  # ty: ignore[unresolved-attribute]
            else:
                sql = query.get_compiler(db_alias).as_sql()[0].lower()
            final_check_tables = _get_tables_from_sql(connections[db_alias], sql, enable_quote=True)
            tables.update(final_check_tables)

    if not are_all_cachable(tables):
        raise UncachableQuery
    return tables


def _get_table_cache_keys(compiler: SQLCompiler) -> list[str]:
    db_alias = compiler.using
    get_table_cache_key = orm_settings.TABLE_KEYGEN
    return [get_table_cache_key(db_alias, t) for t in _get_tables(db_alias, compiler.query, compiler)]


def _invalidate_tables(cache: Any, db_alias: str, tables: Iterable[str]) -> None:
    tables = filter_cachable(set(tables))
    if not tables:
        return
    now = time()
    get_table_cache_key = orm_settings.TABLE_KEYGEN
    cache.set_many({get_table_cache_key(db_alias, t): now for t in tables}, orm_settings.TIMEOUT)

    if isinstance(cache, AtomicCache):
        cache.to_be_invalidated.update(tables)
