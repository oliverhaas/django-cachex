"""Cache keys and table extraction for ORM queries."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import datetime
import re
from decimal import Decimal
from hashlib import sha1
from ipaddress import IPv4Address, IPv6Address
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
from django.db.models.lookups import In
from django.db.models.sql import AggregateQuery, Query
from django.db.models.sql.where import ExtraWhere, NothingNode, WhereNode
from django.utils.tree import Node

from django_cachex.orm.settings import ITERABLES, SETTING_NAME, orm_settings

# The on_delete of foreign keys the database enforces itself (Django 6.1+).
_DATABASE_ON_DELETE: Any = getattr(deletion, "DatabaseOnDelete", None)

# Where query_digest leaves the lowercased SQL of a compiler.
_GENERATED_SQL = "_cachex_orm_generated_sql"

# The length past which readable_query_key cuts the table names short.
QUERY_KEY_PREFIX_MAX_LENGTH = 100

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
# Functions whose result changes between calls: a query calling one is not
# cached. UUID4 and UUID7 are new in Django 6.1.
UNCACHABLE_FUNCS: tuple[type, ...] = (
    Now,
    TransactionNow,
    Random,
    RandomUUID,
    *(func for name in ("UUID4", "UUID7") if (func := getattr(functions, name, None))),
)


def _psycopg_param_keys() -> dict[type, Callable[[Any], str]]:
    from psycopg.dbapi20 import Binary
    from psycopg.types.json import Json, Jsonb
    from psycopg.types.numeric import Float4, Float8, Int2, Int4, Int8
    from psycopg.types.range import Range

    def binary_key(param: Any) -> str:
        if param.obj.__class__ not in {bytes, bytearray, memoryview}:
            raise UncachableQuery
        return f"Binary({bytes(param.obj)!r})"

    def json_key(param: Any) -> str:
        # Without a dumps of its own, the value is serialized by a function
        # set on the connection, which the key cannot see.
        if param.dumps is None:
            raise UncachableQuery
        try:
            text = param.dumps(param.obj)
        except Exception as e:
            # The query fails the same way without the cache.
            raise UncachableQuery from e
        return f"{param.__class__.__name__}({text!r})"

    return {
        # The repr of these shortens long values.
        Binary: binary_key,
        Json: json_key,
        Jsonb: json_key,
        **dict.fromkeys((Range, Int2, Int4, Int8, Float4, Float8, IPv4Address, IPv6Address), repr),
    }


# Without psycopg, as on psycopg2, queries with parameters of driver types are not cached.
try:
    _DRIVER_PARAM_KEYS = _psycopg_param_keys()
except ImportError:
    _DRIVER_PARAM_KEYS = {}


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


def query_digest(compiler: SQLCompiler) -> str:
    """Return the SHA-1 hex digest of the database, SQL and parameters of the query of ``compiler``."""
    sql, params = compiler.as_sql()
    cache_key = f"{compiler.using!r}:{sql!r}:({', '.join(map(_param_key, params))})"
    # Kept for the final SQL check, which would otherwise call as_sql() again.
    setattr(compiler, _GENERATED_SQL, sql.lower())
    return sha1(cache_key.encode(), usedforsecurity=False).hexdigest()


def readable_query_key(*, compiler: SQLCompiler, tables: frozenset[str], digest: str) -> str:  # noqa: ARG001
    """Return ``digest`` prefixed with the sorted names of ``tables``, cut short past QUERY_KEY_PREFIX_MAX_LENGTH."""
    names = sorted(tables)
    prefix = ".".join(names)
    if len(names) > 1 and len(prefix) > QUERY_KEY_PREFIX_MAX_LENGTH:
        # The longest run of leading names that fits with a count of the others; the first name in any case.
        prefix = f"{names[0]}.+{len(names) - 1}more"
        for kept in range(2, len(names)):
            shortened = f"{'.'.join(names[:kept])}.+{len(names) - kept}more"
            if len(shortened) > QUERY_KEY_PREFIX_MAX_LENGTH:
                break
            prefix = shortened
    return f"{prefix}:{digest}"


def readable_table_key(*, db_alias: str, table: str) -> str:  # noqa: ARG001
    """Return ``table``: the database alias is the hash tag of the key already."""
    return table


def known_tables() -> set[str]:
    """Return the tables of every installed model, unmanaged ones included, and the ``ADDITIONAL_TABLES``."""
    return {model._meta.db_table for model in apps.get_models(include_auto_created=True)}.union(
        orm_settings.ADDITIONAL_TABLES,
    )


def _get_tables_from_sql(
    connection: BaseDatabaseWrapper,
    lowercased_sql: str,
    *,
    enable_quote: bool = False,
) -> set[str]:
    """Return the tables named in the final SQL of a query."""
    tables = set()
    for table in known_tables():
        name = (connection.ops.quote_name(table) if enable_quote else table).lower()
        # Whole names only: ``shop_order`` is not found inside ``shop_orderline``.
        if name in lowercased_sql and re.search(rf"(?<!\w){re.escape(name)}(?!\w)", lowercased_sql):
            tables.add(table)
    return tables


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
        # Raw SQL conditions can name any table, unquoted: look for all of
        # them in the final SQL.
        self.raw_sql = False
        self.check_final_sql = orm_settings.FINAL_SQL_CHECK
        self.subqueries = 0

    def add_query(self, query: Query) -> None:
        meta = query.get_meta()
        ordering: Sequence[Any] = query.order_by
        if not ordering and query.default_ordering and meta:
            ordering = meta.ordering or ()
        if query.select_for_update or "?" in ordering:
            raise UncachableQuery
        # Not extra_select, which leaves out those values() hides: ordering by one keeps its SQL.
        if query.extra:
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
        # ORDER BY compiles copies of its expressions and of the unselected annotations it names,
        # so the joins a subquery in them needs for its ordering show in the final SQL only.
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
                # Plain values, often thousands of __in values, hold no table or function.
                if item.__class__ not in _REPR_KEYED_TYPES:
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
        elif isinstance(node, UNCACHABLE_FUNCS):
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
        # Stored by query_digest, saving another as_sql() call.
        final_sql = getattr(compiler, _GENERATED_SQL, None)
        if final_sql is None:
            final_sql = query.get_compiler(db_alias).as_sql()[0].lower()
        # The ORM quotes the tables it names; raw SQL does not have to.
        tables |= _get_tables_from_sql(connections[db_alias], final_sql, enable_quote=not finder.raw_sql)
    if not are_all_cachable(tables):
        raise UncachableQuery
    return tables


def _sort_in_values(where: WhereNode) -> None:
    nodes = [where]
    while nodes:
        node = nodes.pop()
        for index, child in enumerate(node.children):
            if isinstance(child, WhereNode):
                nodes.append(child)
            elif isinstance(child, In) and child.rhs_is_direct_value():
                try:
                    values = sorted(child.rhs)
                except TypeError:
                    # str orders mixed types, like the (None,) a prefetch over a nullable foreign key passes.
                    values = sorted(child.rhs, key=str)
                if values != child.rhs:
                    # Clones of a query share its lookups, so a copy takes the sorted values.
                    lookup = child.copy()
                    lookup.rhs = values
                    node.children[index] = lookup


def query_key_and_tables(compiler: SQLCompiler, result_type: str) -> tuple[str, set[str]]:
    """Return the cache key and tables of the query of ``compiler``; raise UncachableQuery if it is not cachable."""
    query = compiler.query
    # Compiling only adds tables, so an uncachable one here rules out caching before as_sql() runs.
    tables = {*query.table_map, *query.extra_tables}
    if meta := query.get_meta():
        tables.add(meta.db_table)
    if not are_all_cachable(tables):
        raise UncachableQuery
    # One set of __in values gets one key in any order, such as the order prefetch_related() passes.
    _sort_in_values(query.where)
    digest = query_digest(compiler)
    # Compiled for its digest, the query has joined the tables select_related() and the ordering need.
    tables = _get_tables(compiler.connection.alias, query, compiler)
    if not tables:
        raise UncachableQuery
    query_key = orm_settings.QUERY_KEYGEN(compiler=compiler, tables=frozenset(tables), digest=digest)
    if not isinstance(query_key, str):
        msg = f"`{SETTING_NAME}['QUERY_KEYGEN']` must return a str, not {query_key!r}."
        raise TypeError(msg)
    # A SINGLE and a MULTI query can share their SQL but not their result.
    return f"{query_key}:{result_type}", tables


def models_of_tables(tables: set[str]) -> list[type[Model]]:
    """Return the installed models stored in ``tables``."""
    return [model for model in apps.get_models(include_auto_created=True) if model._meta.db_table in tables]


def _concrete(model: type[Model]) -> type[Model]:
    return model._meta.concrete_model or model


def deletion_dependents(models: Iterable[type[Model]], *, truncate: bool = False) -> set[str]:
    """Return the tables the database itself changes when rows of ``models`` are deleted.

    A delete reaches the tables whose foreign keys have a database-level on_delete (DB_CASCADE, DB_SET_NULL,
    DB_SET_DEFAULT), onwards through DB_CASCADE. With ``truncate``, TRUNCATE ... CASCADE reaches every table
    with a foreign key to a truncated one, onwards through all of them.
    """
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
