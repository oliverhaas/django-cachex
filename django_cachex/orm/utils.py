"""Cache keys and table extraction for ORM queries."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import datetime
from decimal import Decimal
from hashlib import sha1
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from django.apps import apps
from django.contrib.postgres.functions import RandomUUID, TransactionNow
from django.db import connections
from django.db.models import F, Lookup, QuerySet, deletion, functions
from django.db.models.constants import LOOKUP_SEP
from django.db.models.enums import Choices
from django.db.models.expressions import RawSQL
from django.db.models.functions import Now, Random
from django.db.models.sql import AggregateQuery, Query
from django.db.models.sql.where import ExtraWhere, NothingNode
from django.utils.tree import Node

from django_cachex.orm.settings import ITERABLES, orm_settings

# The on_delete of foreign keys the database enforces itself (Django 6.1+).
_DATABASE_ON_DELETE: Any = getattr(deletion, "DatabaseOnDelete", None)

# Where get_query_cache_key leaves the lowercased SQL of a compiler.
_GENERATED_SQL = "_cachex_orm_generated_sql"

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.models import Model
    from django.db.models.sql.compiler import SQLCompiler


class UncachableQuery(Exception):  # noqa: N818
    pass


# Parameters are keyed by their type and whole value, which repr() shows for
# these types. str() would key 1 and "1" alike.
_REPR_KEYED_TYPES = frozenset(
    {
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
    },
)
# Functions whose result changes over time: a query calling one is not cached.
UNCACHABLE_FUNCS: tuple[type, ...] = (Now, TransactionNow)
# Functions returning a new random value on every call: a query calling one is
# cached only with CACHE_RANDOM. UUID4 and UUID7 are new in Django 6.1.
RANDOM_FUNCS: tuple[type, ...] = (
    Random,
    RandomUUID,
    *(func for name in ("UUID4", "UUID7") if (func := getattr(functions, name, None))),
)


def _bytes_key(value: Any) -> str:
    if value.__class__ not in {bytes, bytearray, memoryview}:
        raise UncachableQuery
    return repr(bytes(value))


def _json_key(name: str, dumps: Callable[[Any], Any], value: Any) -> str:
    try:
        text = dumps(value)
    except Exception as e:
        # The query fails the same way without the cache.
        raise UncachableQuery from e
    return f"{name}({text!r})"


def _psycopg_param_keys() -> dict[type, Callable[[Any], str]]:
    from ipaddress import IPv4Address, IPv6Address

    from psycopg.dbapi20 import Binary
    from psycopg.types.json import Json, Jsonb
    from psycopg.types.numeric import Float4, Float8, Int2, Int4, Int8
    from psycopg.types.range import Range

    def json_key(param: Any) -> str:
        # Without a dumps of its own, the value is serialized by a function
        # set on the connection, which the key cannot see.
        if param.dumps is None:
            raise UncachableQuery
        return _json_key(param.__class__.__name__, param.dumps, param.obj)

    return {
        # The repr of these shortens long values.
        Binary: lambda param: f"Binary({_bytes_key(param.obj)})",
        Json: json_key,
        Jsonb: json_key,
        **dict.fromkeys((Range, Int2, Int4, Int8, Float4, Float8, IPv4Address, IPv6Address), repr),
    }


def _psycopg2_param_keys() -> dict[type, Callable[[Any], str]]:
    from psycopg2 import Binary  # ty: ignore[unresolved-import]
    from psycopg2.extras import (  # ty: ignore[unresolved-import]
        DateRange,
        DateTimeRange,
        DateTimeTZRange,
        Inet,
        Json,
        NumericRange,
    )

    return {
        # These have no repr of their own.
        Binary: lambda param: f"Binary({_bytes_key(param.adapted)})",
        Json: lambda param: _json_key("Json", param.dumps, param.adapted),
        **dict.fromkeys((NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet), repr),
    }


# Keys of the parameter types of the PostgreSQL driver Django uses: psycopg,
# else psycopg2.
_DRIVER_PARAM_KEYS: dict[type, Callable[[Any], str]] = {}
for _driver_param_keys in (_psycopg_param_keys, _psycopg2_param_keys):
    try:
        _DRIVER_PARAM_KEYS = _driver_param_keys()
    except ImportError:
        continue
    break


def _param_key(param: Any) -> str:
    """Return the text a query parameter is keyed by; raise UncachableQuery if it has none."""
    cls = param.__class__
    if cls in _REPR_KEYED_TYPES:
        return repr(param)
    if (key := _DRIVER_PARAM_KEYS.get(cls)) is not None:
        return key(param)
    if cls in ITERABLES:
        return f"{cls.__name__}({', '.join(map(_param_key, param))})"
    if cls is dict:
        return f"dict({', '.join(f'{_param_key(k)}: {_param_key(v)}' for k, v in param.items())})"
    if issubclass(cls, Choices):
        # Choices are unique enums, so the underlying value decides.
        return _param_key(param.value)
    raise UncachableQuery


def get_query_cache_key(compiler: SQLCompiler) -> str:
    """Return a cache key for the query of ``compiler``, specific to its SQL, parameters and database."""
    sql, params = compiler.as_sql()
    cache_key = f"{compiler.using!r}:{sql!r}:({', '.join(map(_param_key, params))})"
    # Kept for the final SQL check, which would otherwise call as_sql() again.
    setattr(compiler, _GENERATED_SQL, sql.lower())
    return sha1(cache_key.encode(), usedforsecurity=False).hexdigest()


def get_table_cache_key(db_alias: str, table: str) -> str:
    """Return a cache key for ``table`` of database ``db_alias``."""
    return sha1(f"{db_alias}:{table}".encode(), usedforsecurity=False).hexdigest()


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


class _TableFinder:
    """Find the tables a query and its subqueries read.

    Raise UncachableQuery if the query calls a function whose result changes
    between calls.
    """

    def __init__(self, db_alias: str) -> None:
        self.db_alias = db_alias
        self.tables: set[str] = set()
        # Raw SQL conditions may name any table, unquoted: look for all of
        # them in the final SQL.
        self.raw_sql = False
        self.check_final_sql = orm_settings.FINAL_SQL_CHECK
        self.subqueries = 0

    def add_query(self, query: Query) -> None:
        meta = query.get_meta()
        ordering: Sequence[Any] = query.order_by
        if not ordering and query.default_ordering and meta:
            ordering = meta.ordering or ()
        if query.select_for_update or (not orm_settings.CACHE_RANDOM and "?" in ordering):
            raise UncachableQuery
        if query.extra_select:
            self.raw_sql = True
        # The tables joined so far. Compiling joins the ones select_related()
        # and the ordering need, so queries are compiled before they get here.
        self.tables.update(query.table_map, query.extra_tables)
        if meta:
            self.tables.add(meta.db_table)
        self.visit([*query.annotations.values(), query.where, *query.combined_queries])
        for join in query.alias_map.values():
            if (relation := join.filtered_relation) is not None:
                self.visit((relation.condition, getattr(relation, "resolved_condition", None)))
        if isinstance(query, AggregateQuery):
            self.add_query(query.inner_query)
        self.visit_ordering(query, ordering)

    def visit_ordering(self, query: Query, ordering: Sequence[Any]) -> None:
        # ORDER BY compiles copies of the expressions it holds and of the
        # unselected annotations it names, so the joins that the ordering of
        # a subquery in them needs show in the final SQL only.
        subqueries = self.subqueries
        for item in ordering:
            if not isinstance(item, str):
                self.visit(item)
                continue
            name = item.removeprefix("-")
            if name not in query.annotations:
                name = name.split(LOOKUP_SEP, 1)[0]
            if name not in query.annotation_select:
                self.visit(query.annotations.get(name))
        if self.subqueries > subqueries:
            self.check_final_sql = True

    def visit(self, node: Any) -> None:  # noqa: C901
        """Visit ``node``, a part of a query, and the expressions it holds."""
        if isinstance(node, list | tuple):  # e.g. the right-hand side of __in or __range
            for item in node:
                self.visit(item)
        elif isinstance(node, Query | QuerySet):  # a subquery
            self.subqueries += 1
            self.add_query(node if isinstance(node, Query) else node.query)
        elif isinstance(node, Node):  # a WhereNode, or a Q not resolved yet
            self.visit(node.children)
        elif isinstance(node, ExtraWhere):
            self.raw_sql = True
        elif isinstance(node, RawSQL):
            self.tables.update(_get_tables_from_sql(connections[self.db_alias], node.sql.lower()))
        elif isinstance(node, UNCACHABLE_FUNCS) or (not orm_settings.CACHE_RANDOM and isinstance(node, RANDOM_FUNCS)):
            raise UncachableQuery
        elif isinstance(node, Lookup):
            # A right-hand side of plain values is not a source expression.
            self.visit((node.lhs, node.rhs))
        elif hasattr(node, "get_source_expressions"):
            self.visit(node.get_source_expressions())
        elif isinstance(node, F | NothingNode):
            pass
        elif hasattr(node, "as_sql"):
            # SQL this class cannot look into.
            raise UncachableQuery


def _get_tables(db_alias: str, query: Query, compiler: SQLCompiler | None = None) -> set[str]:
    """Return the tables ``query`` reads, or raise UncachableQuery if its result must not be cached."""
    finder = _TableFinder(db_alias)
    finder.add_query(query)
    tables = finder.tables
    if finder.raw_sql or finder.check_final_sql:
        # Stored by get_query_cache_key, saving another as_sql() call.
        final_sql = getattr(compiler, _GENERATED_SQL, None)
        if final_sql is None:
            final_sql = query.get_compiler(db_alias).as_sql()[0].lower()
        # The ORM quotes the tables it names, raw SQL may not.
        tables |= _get_tables_from_sql(connections[db_alias], final_sql, enable_quote=not finder.raw_sql)
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
