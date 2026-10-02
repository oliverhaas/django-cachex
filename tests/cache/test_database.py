"""Tests for ``django_cachex.cache.database.DatabaseCache``.

Covers the parts of the cachex contract that are specific to the cache
table: atomic compound ops, SQL pattern translation and the typed scan. The
full per-op battery lives with the RESP-backend tests via the parametrized
fixtures.
"""

import inspect
from typing import TYPE_CHECKING

import pytest
from asgiref.sync import async_to_sync
from django.core.cache import caches
from django.core.management import call_command
from django.db import connections
from django.test import override_settings
from django.test.utils import CaptureQueriesContext

from django_cachex.cache.base import BaseCachex
from django_cachex.cache.database import _MISSING, DatabaseCache, _List
from django_cachex.exceptions import NotSupportedError, WrongTypeError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterator


DATABASE_CACHES = {
    "db": {
        "BACKEND": "django_cachex.cache.DatabaseCache",
        "LOCATION": "django_cachex_test_cache",
    },
}

SMALL_DATABASE_CACHES = {
    "db_small": {
        "BACKEND": "django_cachex.cache.DatabaseCache",
        "LOCATION": "django_cachex_test_cache",
        "OPTIONS": {"MAX_ENTRIES": 4, "CULL_FREQUENCY": 2},
    },
}


@pytest.fixture
def db_cache(db) -> Iterator[DatabaseCache]:
    """DatabaseCache wired against the SQLite in-memory test DB.

    Uses ``createcachetable`` to materialize the schema each test; the
    ``db`` fixture from pytest-django gives us a wrapped transaction so
    rows don't leak between tests.
    """
    call_command("createcachetable", "django_cachex_test_cache")
    with override_settings(CACHES=DATABASE_CACHES):
        cache = caches["db"]
        cache.clear()
        yield cache


@pytest.fixture
def small_db_cache(db) -> Iterator[DatabaseCache]:
    """Like ``db_cache``, with ``MAX_ENTRIES=4`` and ``CULL_FREQUENCY=2``."""
    call_command("createcachetable", "django_cachex_test_cache")
    with override_settings(CACHES=SMALL_DATABASE_CACHES):
        cache = caches["db_small"]
        cache.clear()
        yield cache


def test_set_nx_new_key_writes(db_cache: DatabaseCache):
    assert db_cache.set("k", "v", nx=True) is True
    assert db_cache.get("k") == "v"


def test_set_nx_existing_key_no_write(db_cache: DatabaseCache):
    db_cache.set("k", "old")
    assert db_cache.set("k", "new", nx=True) is False
    assert db_cache.get("k") == "old"


def test_set_no_flags_delegates_to_django(db_cache: DatabaseCache):
    assert db_cache.set("k", "v") is None
    assert db_cache.get("k") == "v"


def test_set_xx_raises_not_supported(db_cache: DatabaseCache):
    with pytest.raises(NotSupportedError):
        db_cache.set("k", "v", xx=True)


def test_set_get_raises_not_supported(db_cache: DatabaseCache):
    with pytest.raises(NotSupportedError):
        db_cache.set("k", "v", get=True)


# Cross-backend code that catches ``WrongTypeError`` must work against DatabaseCache too.
def test_lpush_on_string_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", "abc")
    with pytest.raises(WrongTypeError):
        db_cache.lpush("k", "x")


def test_sadd_on_string_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", "abc")
    with pytest.raises(WrongTypeError):
        db_cache.sadd("k", "m")


def test_hset_on_string_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", "abc")
    with pytest.raises(WrongTypeError):
        db_cache.hset("k", "f", "v")


def test_zadd_on_string_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", "abc")
    with pytest.raises(WrongTypeError):
        db_cache.zadd("k", {"m": 1.0})


def test_wrongtype_is_typeerror_subclass(db_cache: DatabaseCache):
    db_cache.set("k", "abc")
    # Existing call sites that catch the broader TypeError must still
    # work, since WrongTypeError is a TypeError subclass.
    with pytest.raises(TypeError):
        db_cache.lpush("k", "x")


# Regression: the private ``_List``/``_Set``/``_Hash``/``_ZSet`` container leaked out of ``get()``.
def test_get_on_collection_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.rpush("lk", 1)
    with pytest.raises(WrongTypeError, match="'lk'"):
        db_cache.get("lk")


def test_get_missing_key_returns_default(db_cache: DatabaseCache):
    assert db_cache.get("absent", "fallback") == "fallback"


def test_get_many_skips_collection_keys(db_cache: DatabaseCache):
    # MGET reports a list/hash/set/zset key as nil, so RespCache omits it.
    db_cache.set("plain", 1)
    db_cache.rpush("lst", "a")
    db_cache.hset("hsh", "f", "v")
    assert db_cache.get_many(["plain", "lst", "hsh", "missing"]) == {"plain": 1}


def test_incr_on_collection_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.rpush("lk", 1)
    with pytest.raises(WrongTypeError):
        db_cache.incr("lk")


def test_wrongtype_message_uses_the_user_key(db_cache: DatabaseCache):
    db_cache.set("sk", "abc")
    with pytest.raises(WrongTypeError, match="'sk'") as exc_info:
        db_cache.lpush("sk", 1)
    assert ":1:sk" not in str(exc_info.value)


def _expires(db_cache: DatabaseCache, key: str):
    conn = connections["default"]
    quote = conn.ops.quote_name
    table = quote(db_cache._get_table_name())
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT {quote('expires')} FROM {table} WHERE {quote('cache_key')} = %s",  # noqa: S608
            [db_cache._internal_key(key)],
        )
        return cursor.fetchone()[0]


# Regression: the inherited ``BaseCache.incr`` reset ``expires`` to the default timeout and skipped the row lock.
def test_incr_returns_the_new_value(db_cache: DatabaseCache):
    db_cache.set("c", 5)
    assert db_cache.incr("c") == 6
    assert db_cache.incr("c", 4) == 10
    assert db_cache.get("c") == 10


def test_incr_keeps_the_expires_column(db_cache: DatabaseCache):
    db_cache.set("c", 5, timeout=3600)
    before = _expires(db_cache, "c")
    db_cache.incr("c")
    assert _expires(db_cache, "c") == before


def test_incr_keeps_a_persistent_key_persistent(db_cache: DatabaseCache):
    db_cache.set("c", 5, timeout=None)
    db_cache.incr("c")
    assert db_cache.ttl("c") is None


def test_decr_keeps_the_expires_column(db_cache: DatabaseCache):
    db_cache.set("c", 5, timeout=3600)
    before = _expires(db_cache, "c")
    assert db_cache.decr("c", 2) == 3
    assert _expires(db_cache, "c") == before


def test_incr_missing_key_raises(db_cache: DatabaseCache):
    with pytest.raises(ValueError, match="not found"):
        db_cache.incr("absent")
    assert db_cache.has_key("absent") is False


def test_incr_runs_through_the_locked_read_modify_write(db_cache: DatabaseCache, mocker):
    # SQLite has no ``FOR UPDATE`` and a single connection, so a real
    # two-writer race cannot be staged here; assert the path instead.
    db_cache.set("c", 5)
    # ``wraps`` rather than ``mocker.spy``: spy autospecs, which resolves
    # the TYPE_CHECKING-only ``Callable`` annotation and fails.
    spy = mocker.patch.object(db_cache, "_atomic_compound", wraps=db_cache._atomic_compound)
    assert db_cache.incr("c") == 6
    spy.assert_called_once()


def test_ttl_persistent_key_reports_none(db_cache: DatabaseCache):
    db_cache.set("forever", 1, timeout=None)
    assert db_cache.ttl("forever") is None


def test_ttl_missing_key_reports_minus_two(db_cache: DatabaseCache):
    assert db_cache.ttl("absent") == -2


def test_ttl_expiring_key_reports_seconds(db_cache: DatabaseCache):
    db_cache.set("ticking", 1, timeout=3600)
    assert 3590 <= db_cache.ttl("ticking") <= 3600


def test_compound_ops_leave_the_key_persistent(db_cache: DatabaseCache):
    db_cache.rpush("l", 1)
    db_cache.rpush("l", 2)
    assert db_cache.ttl("l") is None


def test_zset_reports_zset(db_cache: DatabaseCache):
    db_cache.zadd("zk", {"a": 1.0, "b": 2.0})
    assert db_cache.type("zk") == KeyType.ZSET


def test_zset_with_string_members_reports_zset(db_cache: DatabaseCache):
    # String members structurally resemble a hash; the tag disambiguates.
    db_cache.zadd("zs", {"x": 1.0})
    assert db_cache.type("zs") == KeyType.ZSET


def test_hash_reports_hash(db_cache: DatabaseCache):
    db_cache.hset("hk", "field", "value")
    assert db_cache.type("hk") == KeyType.HASH


def test_list_reports_list(db_cache: DatabaseCache):
    db_cache.rpush("lk", "a")
    assert db_cache.type("lk") == KeyType.LIST


def test_set_reports_set(db_cache: DatabaseCache):
    db_cache.sadd("sk", "m")
    assert db_cache.type("sk") == KeyType.SET


@pytest.mark.parametrize(
    "value",
    [[1, 2, 3], {"a": "b"}, {1, 2, 3}, "plain", 42],
    ids=["list", "dict", "set", "str", "int"],
)
def test_plain_set_value_reports_string(db_cache: DatabaseCache, value):
    db_cache.set("k", value)
    assert db_cache.type("k") == KeyType.STRING


# Regression: hashes and sorted sets are both dicts on disk, so ``zadd`` silently converted a hash.
def test_zadd_on_hash_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.hset("k", "a", "x")
    with pytest.raises(WrongTypeError):
        db_cache.zadd("k", {"b": 1.0})
    assert db_cache.type("k") == KeyType.HASH
    assert db_cache.hgetall("k") == {"a": "x"}


def test_hget_on_zset_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.zadd("k", {"m": 1.0})
    with pytest.raises(WrongTypeError):
        db_cache.hget("k", "m")
    assert db_cache.type("k") == KeyType.ZSET


def test_hset_on_zset_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.zadd("k", {"m": 1.0})
    with pytest.raises(WrongTypeError):
        db_cache.hset("k", "f", "v")


def test_lpush_on_plain_list_value_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", [1, 2, 3])
    with pytest.raises(WrongTypeError):
        db_cache.lpush("k", "x")


def test_sadd_on_plain_set_value_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", {1, 2, 3})
    with pytest.raises(WrongTypeError):
        db_cache.sadd("k", "m")


def test_hset_on_plain_dict_value_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", {"a": "b"})
    with pytest.raises(WrongTypeError):
        db_cache.hset("k", "f", "v")


def test_llen_on_plain_list_value_raises_wrongtype(db_cache: DatabaseCache):
    db_cache.set("k", [1, 2, 3])
    with pytest.raises(WrongTypeError):
        db_cache.llen("k")


# Regression: an empty container was neither ``_DELETE`` nor ``_MISSING``, so it was INSERTed as a permanent row.
def test_zadd_xx_on_missing_key_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.zadd("z", {"m": 1.0}, xx=True) == 0
    assert db_cache.has_key("z") is False
    assert db_cache.type("z") is None


def test_zadd_empty_mapping_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.zadd("z", {}) == 0
    assert db_cache.has_key("z") is False


def test_sadd_no_members_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.sadd("s") == 0
    assert db_cache.has_key("s") is False


def test_hset_no_fields_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.hset("h") == 0
    assert db_cache.has_key("h") is False


def test_lpush_no_values_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.lpush("l") == 0
    assert db_cache.has_key("l") is False


def test_rpush_no_values_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.rpush("l") == 0
    assert db_cache.has_key("l") is False


def test_push_no_values_leaves_existing_list_alone(db_cache: DatabaseCache):
    db_cache.rpush("l", "a")
    db_cache.expire("l", 100)
    assert db_cache.lpush("l") == 0
    assert db_cache.rpush("l") == 0
    assert db_cache.lrange("l", 0, -1) == ["a"]
    assert 90 <= db_cache.ttl("l") <= 100


def test_zadd_xx_on_existing_key_still_updates(db_cache: DatabaseCache):
    db_cache.zadd("z", {"m": 1.0})
    assert db_cache.zadd("z", {"m": 5.0}, xx=True) == 0
    assert db_cache.zscore("z", "m") == 5.0


# Regression: ``_atomic_compound`` pickled the ``_MISSING`` sentinel into the row on no-op calls.
def test_lpop_missing_key_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.lpop("absent") is None
    assert db_cache.has_key("absent") is False
    assert db_cache.type("absent") is None


def test_lrem_missing_key_creates_no_row(db_cache: DatabaseCache):
    assert db_cache.lrem("absent", 0, "x") == 0
    assert db_cache.has_key("absent") is False


def test_lrem_no_match_preserves_list(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b")
    assert db_cache.lrem("l", 0, "z") == 0
    assert db_cache.lrange("l", 0, -1) == ["a", "b"]


def test_hsetnx_existing_field_preserves_hash(db_cache: DatabaseCache):
    db_cache.hset("h", "f", "v")
    assert db_cache.hsetnx("h", "f", "other") is False
    assert db_cache.hgetall("h") == {"f": "v"}


def test_hdel_missing_field_preserves_hash(db_cache: DatabaseCache):
    db_cache.hset("h", "f", "v")
    assert db_cache.hdel("h", "nope") == 0
    assert db_cache.hgetall("h") == {"f": "v"}


def test_linsert_missing_pivot_preserves_list(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b")
    assert db_cache.linsert("l", "BEFORE", "nope", "x") == -1
    assert db_cache.lrange("l", 0, -1) == ["a", "b"]


def test_zrem_missing_member_preserves_zset(db_cache: DatabaseCache):
    db_cache.zadd("z", {"m": 1.0})
    assert db_cache.zrem("z", "nope") == 0
    assert db_cache.zscore("z", "m") == 1.0


# Regression: ``rpop(key, count=0)`` sliced ``existing[-0:]``, popping the entire list and deleting the row.
def test_rpop_count_zero_returns_empty_and_keeps_list(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b", "c")
    assert db_cache.rpop("l", count=0) == []
    assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]


def test_lpop_count_zero_returns_empty_and_keeps_list(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b", "c")
    assert db_cache.lpop("l", count=0) == []
    assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]


@pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
def test_zpop_count_zero_issues_no_update(db_cache: DatabaseCache, method: str):
    # Regression: the no-op pop wrote the unchanged row back.
    db_cache.zadd("z", {"a": 1.0, "b": 2.0})
    with CaptureQueriesContext(connections["default"]) as ctx:
        assert getattr(db_cache, method)("z", count=0) == []
    assert not [q["sql"] for q in ctx.captured_queries if q["sql"].startswith("UPDATE")]
    assert db_cache.zrange("z", 0, -1) == ["a", "b"]


@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_pop_negative_count_rejected(db_cache: DatabaseCache, method: str):
    db_cache.rpush("l", "a", "b", "c")
    with pytest.raises(ValueError, match="must be positive"):
        getattr(db_cache, method)("l", -2)
    assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]


@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_pop_negative_count_rejected_on_missing_key(db_cache: DatabaseCache, method: str):
    with pytest.raises(ValueError, match="must be positive"):
        getattr(db_cache, method)("absent", -1)


@pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
def test_zpop_negative_count_rejected(db_cache: DatabaseCache, method: str):
    db_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    with pytest.raises(ValueError, match="must be positive"):
        getattr(db_cache, method)("z", -2)
    assert db_cache.zcard("z") == 3


def test_lpos_rank_zero_rejected(db_cache: DatabaseCache):
    db_cache.rpush("l", "a")
    with pytest.raises(ValueError, match="RANK can't be zero"):
        db_cache.lpos("l", "a", rank=0)


def test_lpos_negative_rank_scans_the_tail_within_maxlen(db_cache: DatabaseCache):
    db_cache.rpush("l", "b", "a", "c", "b")
    assert db_cache.lpos("l", "b", rank=-1, maxlen=2) == 3
    assert db_cache.lpos("l", "b", rank=-1, count=0) == [3, 0]


def test_srandmember_negative_count_allows_repeats(db_cache: DatabaseCache):
    db_cache.sadd("s", "a")
    assert db_cache.srandmember("s", count=-3) == ["a", "a", "a"]
    assert db_cache.scard("s") == 1


def test_sinter_across_three_keys(db_cache: DatabaseCache):
    db_cache.sadd("a", "x", "y", "z")
    db_cache.sadd("b", "y", "z")
    db_cache.sadd("c", "z")
    assert db_cache.sinter(["a", "b", "c"]) == {"z"}


def test_set_algebra_missing_keys_read_empty(db_cache: DatabaseCache):
    db_cache.sadd("a", "x")
    assert db_cache.sdiff(["a", "absent"]) == {"x"}


def test_set_algebra_wrongtype_names_the_offending_key(db_cache: DatabaseCache):
    db_cache.sadd("a", "x")
    db_cache.set("b", "plain")
    with pytest.raises(WrongTypeError, match="'b'"):
        db_cache.sunion(["a", "b"])


@pytest.mark.parametrize("method", ["sdiff", "sinter", "sunion"])
def test_single_key_set_algebra_result_stores_as_a_plain_value(db_cache: DatabaseCache, method: str):
    db_cache.sadd("s", "a", "b")
    db_cache.set("copy", getattr(db_cache, method)(["s"]))
    assert db_cache.type("copy") == KeyType.STRING
    assert db_cache.get("copy") == {"a", "b"}


def test_concurrent_set_during_transform(db_cache: DatabaseCache):
    internal_key = db_cache._internal_key("racy")
    seen = []

    def transform(current):
        seen.append(current)
        if len(seen) == 1:
            # Concurrent writer inside the SELECT-then-INSERT window; same
            # connection, so the INSERT hits the unique constraint.
            db_cache.set("racy", ["a"])
        existing = [] if current is _MISSING else current
        return _List([*existing, "v"]), "ret"

    assert db_cache._atomic_compound(internal_key, transform) == "ret"
    assert seen == [_MISSING, ["a"]]
    assert db_cache.lrange("racy", 0, -1) == ["a", "v"]


def test_concurrent_compound_during_transform(db_cache: DatabaseCache):
    # Regression: the fallback UPDATE wrote the stale transform result,
    # dropping the winner's value instead of merging with it.
    internal_key = db_cache._internal_key("racy")
    raced = False

    def transform(current):
        nonlocal raced
        if not raced:
            raced = True
            db_cache.rpush("racy", "a")
        existing = [] if current is _MISSING else current
        return _List([*existing, "b"]), len(existing) + 1

    assert db_cache._atomic_compound(internal_key, transform) == 2
    assert db_cache.lrange("racy", 0, -1) == ["a", "b"]


def test_underscore_in_pattern_is_literal(db_cache: DatabaseCache):
    db_cache.set("foo_bar", 1)
    db_cache.set("fooxbar", 1)
    assert db_cache.keys("foo_bar") == ["foo_bar"]


def test_percent_in_pattern_is_literal(db_cache: DatabaseCache):
    db_cache.set("100%", 1)
    db_cache.set("100pc", 1)
    assert db_cache.keys("100%") == ["100%"]


def test_backslash_escapes_the_next_character(db_cache: DatabaseCache):
    db_cache.set(r"a\b", 1)
    db_cache.set("ab", 1)
    # Redis globs read ``\x`` as a literal ``x``, so this names the key ``ab``.
    assert db_cache.keys(r"a\b") == ["ab"]
    assert db_cache.keys(r"a\\b") == [r"a\b"]


def test_character_class_matches_one_member(db_cache: DatabaseCache):
    db_cache.set("ka", 1)
    db_cache.set("kb", 2)
    db_cache.set("kc", 3)
    assert db_cache.keys("k[ab]") == ["ka", "kb"]


def test_negated_character_class(db_cache: DatabaseCache):
    db_cache.set("ka", 1)
    db_cache.set("kb", 2)
    assert db_cache.keys("k[^a]") == ["kb"]


def test_glob_wildcards_still_translate(db_cache: DatabaseCache):
    db_cache.set("foo_bar", 1)
    db_cache.set("fooxbar", 1)
    db_cache.set("other", 1)
    assert sorted(db_cache.keys("foo*")) == ["foo_bar", "fooxbar"]
    assert sorted(db_cache.keys("foo?bar")) == ["foo_bar", "fooxbar"]


def test_delete_pattern_with_literal_underscore(db_cache: DatabaseCache):
    db_cache.set("foo_bar", 1)
    db_cache.set("fooxbar", 1)
    assert db_cache.delete_pattern("foo_bar") == 1
    assert db_cache.get("fooxbar") == 1


def test_hincrby_non_integer_field_raises(db_cache: DatabaseCache):
    db_cache.hset("h", "f", "abc")
    with pytest.raises(ValueError, match="not an integer"):
        db_cache.hincrby("h", "f", 1)
    assert db_cache.hget("h", "f") == "abc"


def test_hincrby_float_field_raises(db_cache: DatabaseCache):
    # Redis rejects HINCRBY on a float value; int() would truncate it.
    db_cache.hset("h", "f", 3.5)
    with pytest.raises(ValueError, match="not an integer"):
        db_cache.hincrby("h", "f", 1)
    assert db_cache.hget("h", "f") == 3.5


def test_hincrby_int_field_increments(db_cache: DatabaseCache):
    db_cache.hset("h", "f", 5)
    assert db_cache.hincrby("h", "f", 2) == 7


def test_hincrbyfloat_non_numeric_field_raises(db_cache: DatabaseCache):
    db_cache.hset("h", "f", "abc")
    with pytest.raises(ValueError, match="not a float"):
        db_cache.hincrbyfloat("h", "f", 1.0)
    assert db_cache.hget("h", "f") == "abc"


def test_hincrbyfloat_int_field_increments(db_cache: DatabaseCache):
    db_cache.hset("h", "f", 2)
    assert db_cache.hincrbyfloat("h", "f", 0.5) == 2.5


def test_hincrby_works_on_a_whole_hincrbyfloat_result(db_cache: DatabaseCache):
    db_cache.hincrbyfloat("h", "f", 5000.0)
    assert db_cache.hincrbyfloat("h", "f", 200.0) == 5200.0
    assert db_cache.hincrby("h", "f", 1) == 5201


def test_zadd_non_numeric_score_raises(db_cache: DatabaseCache):
    with pytest.raises(ValueError, match="not a valid float"):
        db_cache.zadd("z", {"m": "abc"})
    assert db_cache.zcard("z") == 0


def test_zadd_numeric_string_score_coerced(db_cache: DatabaseCache):
    # Redis parses numeric strings as scores.
    assert db_cache.zadd("z", {"m": "1.5"}) == 1
    assert db_cache.zscore("z", "m") == 1.5


def test_zincrby_non_numeric_amount_raises(db_cache: DatabaseCache):
    db_cache.zadd("z", {"m": 1.0})
    with pytest.raises(ValueError, match="not a valid float"):
        db_cache.zincrby("z", "abc", "m")
    assert db_cache.zscore("z", "m") == 1.0


# All compound creates share the ``_atomic_compound`` insert path and the cull check of plain ``set()``.
def test_compound_inserts_cull_when_over_max_entries(small_db_cache: DatabaseCache):
    for i in range(10):
        small_db_cache.sadd(f"k{i}", "m")
    # MAX_ENTRIES=4 with CULL_FREQUENCY=2 halves the table whenever an
    # insert finds it over the limit, so growth stays bounded.
    assert len(small_db_cache.keys("*")) <= 5


def test_compound_insert_culls_outside_its_transaction(small_db_cache: DatabaseCache, mocker):
    for i in range(5):
        small_db_cache.set(f"k{i}", i)
    conn = connections["default"]
    depth = len(conn.atomic_blocks)
    cull_depths = []
    real_cull = small_db_cache._cull

    def cull(*args):
        cull_depths.append(len(conn.atomic_blocks))
        return real_cull(*args)

    mocker.patch.object(small_db_cache, "_cull", side_effect=cull)
    small_db_cache.sadd("new", "m")
    assert cull_depths == [depth]
    assert small_db_cache.smembers("new") == {"m"}


@pytest.fixture
def zset_cache(db_cache: DatabaseCache) -> DatabaseCache:
    db_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    return db_cache


def test_negative_num_reaches_the_end(zset_cache: DatabaseCache):
    # Regression: the idiomatic ``LIMIT 0 -1`` sliced ``[0:-1]`` and
    # silently dropped the last member.
    assert zset_cache.zrangebyscore("z", "-inf", "+inf", start=0, num=-1) == ["a", "b", "c", "d"]


def test_positive_num_windows(zset_cache: DatabaseCache):
    assert zset_cache.zrangebyscore("z", "-inf", "+inf", start=1, num=2) == ["b", "c"]


@pytest.mark.parametrize("method", ["zrangebyscore", "zrevrangebyscore"])
@pytest.mark.parametrize("limit", [{"start": 1}, {"num": 2}], ids=["start-only", "num-only"])
def test_one_sided_limit_is_rejected(zset_cache: DatabaseCache, method: str, limit: dict):
    # Regression: a lone ``start`` or ``num`` silently returned the whole
    # range; redis-py raises for the same call.
    with pytest.raises(ValueError, match="start and num must both be specified"):
        getattr(zset_cache, method)("z", "-inf", "+inf", **limit)


def test_zrevrangebyscore(zset_cache: DatabaseCache):
    assert zset_cache.zrevrangebyscore("z", 3.0, 2.0) == ["c", "b"]
    assert zset_cache.zrevrangebyscore("z", "+inf", "-inf", withscores=True) == [
        ("d", 4.0),
        ("c", 3.0),
        ("b", 2.0),
        ("a", 1.0),
    ]


def test_zrevrangebyscore_limit_applies_after_reversal(zset_cache: DatabaseCache):
    assert zset_cache.zrevrangebyscore("z", "+inf", "-inf", start=1, num=2) == ["c", "b"]
    assert zset_cache.zrevrangebyscore("z", "+inf", "-inf", start=1, num=-1) == ["c", "b", "a"]


def test_zrevrangebyscore_missing_key(db_cache: DatabaseCache):
    assert db_cache.zrevrangebyscore("missing", 100.0, 0.0) == []


def test_infinite_bounds_parse(zset_cache: DatabaseCache):
    assert zset_cache.zcount("z", "-inf", "+inf") == 4


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("zrangebyscore", ("z", "(1", "+inf")),
        ("zcount", ("z", "(1", "+inf")),
        ("zremrangebyscore", ("z", "-inf", "(3")),
    ],
    ids=["zrangebyscore", "zcount", "zremrangebyscore"],
)
def test_exclusive_bound_raises_not_supported(zset_cache: DatabaseCache, method, args):
    # Regression: ``float("(1")`` raised a bare ValueError instead of
    # telling the caller the bound style is unsupported.
    with pytest.raises(NotSupportedError):
        getattr(zset_cache, method)(*args)


def test_wildcard_in_key_prefix_is_literal(db):
    # Regression: ``KEY_PREFIX="svc?1"`` translated the ``?`` into a SQL
    # ``_`` wildcard, so ``keys("*")`` matched rows of sibling prefixes.
    call_command("createcachetable", "django_cachex_test_cache")
    caches_config = {
        "wild": {**DATABASE_CACHES["db"], "KEY_PREFIX": "svc?1"},
        "sibling": {**DATABASE_CACHES["db"], "KEY_PREFIX": "svcX1"},
    }
    with override_settings(CACHES=caches_config):
        wild = caches["wild"]
        sibling = caches["sibling"]
        wild.clear()
        wild.set("mine", 1)
        sibling.set("theirs", 1)
        assert wild.keys("*") == ["mine"]


def _pipe_key(key: str, key_prefix: str, version: int) -> str:
    return f"kf|{key_prefix}|{version}|{key}"


def test_keys_and_delete_pattern_round_trip_under_a_custom_key_function(db):
    # Regression: ``keys()`` returned the raw ``kf|p|2|k`` rows, so
    # ``delete_pattern("k*")`` re-made keys that did not exist and deleted
    # nothing while reporting the matches.
    call_command("createcachetable", "django_cachex_test_cache")
    config = {"kf": {**DATABASE_CACHES["db"], "KEY_FUNCTION": _pipe_key, "KEY_PREFIX": "p", "VERSION": 2}}
    with override_settings(CACHES=config):
        cache = caches["kf"]
        cache.clear()
        cache.set("k", 1)
        cache.set("k2", 1)
        cache.set("other", 1)

        assert cache.keys("*") == ["k", "k2", "other"]
        assert cache.keys("k*") == ["k", "k2"]
        assert cache.delete_pattern("k*") == 2
        assert cache.keys("*") == ["other"]
        assert cache.get("other") == 1


def test_info_no_expiry_keys_excluded_from_expires(db_cache: DatabaseCache):
    db_cache.set("forever", 1, timeout=None)
    db_cache.set("ticking", 1, timeout=600)
    keyspace = db_cache.info()["keyspace"]["db0"]
    assert keyspace["keys"] == 2
    assert keyspace["expires"] == 1


def test_srem_missing_member_preserves_set(db_cache: DatabaseCache):
    db_cache.sadd("s", "a")
    assert db_cache.srem("s", "nope") == 0
    assert db_cache.smembers("s") == {"a"}


def test_srem_last_member_deletes_key(db_cache: DatabaseCache):
    db_cache.sadd("s", "a")
    assert db_cache.srem("s", "a") == 1
    assert db_cache.has_key("s") is False


def test_delete_pattern_deletes_all_matches(db_cache: DatabaseCache):
    for i in range(7):
        db_cache.set(f"p:{i}", i)
    db_cache.set("other", 1)
    assert db_cache.delete_pattern("p:*") == 7
    assert db_cache.keys("*") == ["other"]


def test_delete_pattern_itersize_chunks_do_not_change_the_result(db_cache: DatabaseCache):
    for i in range(7):
        db_cache.set(f"p:{i}", i)
    assert db_cache.delete_pattern("p:*", itersize=2) == 7
    assert db_cache.keys("*") == []


@pytest.fixture
def typed_cache(db_cache: DatabaseCache) -> DatabaseCache:
    db_cache.set("plain", 1)
    db_cache.rpush("alist", "a")
    db_cache.sadd("aset", "a")
    db_cache.hset("ahash", "f", "v")
    db_cache.zadd("azset", {"m": 1.0})
    return db_cache


@pytest.mark.parametrize(
    ("key_type", "expected"),
    [
        (KeyType.STRING, "plain"),
        (KeyType.LIST, "alist"),
        (KeyType.SET, "aset"),
        (KeyType.HASH, "ahash"),
        (KeyType.ZSET, "azset"),
    ],
    ids=["string", "list", "set", "hash", "zset"],
)
def test_scan_filters_by_key_type(typed_cache: DatabaseCache, key_type, expected):
    assert typed_cache.scan(pattern="*", key_type=key_type) == (0, [expected])


def test_scan_key_type_the_table_cannot_hold_matches_nothing(typed_cache: DatabaseCache):
    assert typed_cache.scan(pattern="*", key_type=KeyType.STREAM) == (0, [])


def test_scan_without_key_type_returns_everything(db_cache: DatabaseCache):
    db_cache.set("plain", 1)
    db_cache.rpush("alist", "a")
    _, keys = db_cache.scan(pattern="*")
    assert sorted(keys) == ["alist", "plain"]


def test_scan_paginates(db_cache: DatabaseCache):
    for i in range(5):
        db_cache.set(f"k{i}", i)
    cursor, page = db_cache.scan(count=2)
    pages = [page]
    while cursor:
        cursor, page = db_cache.scan(cursor=cursor, count=2)
        pages.append(page)
    assert [len(page) for page in pages] == [2, 2, 1]
    assert sorted(key for page in pages for key in page) == ["k0", "k1", "k2", "k3", "k4"]


def test_scan_returns_remaining_keys_after_earlier_pages_are_deleted(db_cache: DatabaseCache):
    for i in range(10):
        db_cache.set(f"k{i}", i)
    cursor, first = db_cache.scan(count=3)
    db_cache.delete_many(first)
    seen = []
    while cursor:
        cursor, keys = db_cache.scan(cursor, count=3)
        seen.extend(keys)
    assert sorted(seen) == sorted(db_cache.keys())


def test_scan_combines_pattern_and_type(typed_cache: DatabaseCache):
    typed_cache.rpush("blist", "b")
    assert typed_cache.scan(pattern="a*", key_type=KeyType.LIST) == (0, ["alist"])


@pytest.fixture
def mixed_case_cache(db_cache: DatabaseCache) -> DatabaseCache:
    db_cache.set("Foo", 1)
    db_cache.set("foo", 2)
    db_cache.set("FOO", 3)
    return db_cache


# SQLite's ``LIKE`` folds ASCII case, so every row is re-checked in Python.
def test_keys_are_case_sensitive(mixed_case_cache: DatabaseCache):
    assert mixed_case_cache.keys("Foo*") == ["Foo"]
    assert mixed_case_cache.keys("foo") == ["foo"]


def test_scan_is_case_sensitive(mixed_case_cache: DatabaseCache):
    assert mixed_case_cache.scan(pattern="Foo*") == (0, ["Foo"])


def test_iter_keys_is_case_sensitive(mixed_case_cache: DatabaseCache):
    assert list(mixed_case_cache.iter_keys("Foo*")) == ["Foo"]


def test_delete_pattern_only_deletes_the_exact_case(mixed_case_cache: DatabaseCache):
    assert mixed_case_cache.delete_pattern("Foo*") == 1
    assert sorted(mixed_case_cache.keys("*")) == ["FOO", "foo"]


def test_empty_pattern_matches_nothing_without_an_empty_key(db_cache: DatabaseCache):
    db_cache.set("a", 1)
    assert db_cache.keys("") == []
    assert list(db_cache.iter_keys("")) == []


def test_empty_pattern_matches_the_empty_key(db_cache: DatabaseCache):
    db_cache.set("a", 1)
    db_cache.set("", 2)
    assert db_cache.keys("") == [""]
    assert db_cache.delete_pattern("") == 1
    assert db_cache.keys("*") == ["a"]


def test_incr_version_collection_key_moves(db_cache: DatabaseCache):
    db_cache.rpush("l", 1, 2)
    assert db_cache.incr_version("l") == 2
    assert db_cache.lrange("l", 0, -1, version=2) == [1, 2]
    assert db_cache.llen("l", version=1) == 0


def test_incr_version_string_key_keeps_its_ttl(db_cache: DatabaseCache):
    db_cache.set("s", "v", timeout=100)
    db_cache.incr_version("s")
    ttl = db_cache.ttl("s", version=2)
    assert ttl is not None
    assert 90 < ttl <= 100


def test_incr_version_persistent_key_stays_persistent(db_cache: DatabaseCache):
    db_cache.set("s", "v", timeout=None)
    db_cache.incr_version("s")
    assert db_cache.ttl("s", version=2) is None


def test_incr_version_destination_is_replaced(db_cache: DatabaseCache):
    db_cache.set("k", "new", version=1)
    db_cache.set("k", "old", version=2)
    assert db_cache.incr_version("k") == 2
    assert db_cache.get("k", version=2) == "new"


def test_incr_version_missing_key_raises_and_keeps_the_destination(db_cache: DatabaseCache):
    db_cache.set("k", "old", version=2)
    with pytest.raises(ValueError, match="not found"):
        db_cache.incr_version("k")
    assert db_cache.get("k", version=2) == "old"


def test_decr_version_moves_back(db_cache: DatabaseCache):
    db_cache.rpush("l", 1, version=2)
    assert db_cache.decr_version("l", version=2) == 1
    assert db_cache.lrange("l", 0, -1, version=1) == [1]


def test_incr_version_zero_delta_is_a_no_op(db_cache: DatabaseCache):
    # Regression: the destination delete hit the source row, so the key
    # vanished and the move reported "not found".
    db_cache.set("k", "v", timeout=300)
    assert db_cache.incr_version("k", 0) == 1
    assert db_cache.get("k") == "v"
    ttl = db_cache.ttl("k")
    assert ttl is not None
    assert 290 < ttl <= 300


def test_incr_version_zero_delta_on_a_missing_key_raises(db_cache: DatabaseCache):
    with pytest.raises(ValueError, match="not found"):
        db_cache.incr_version("absent", 0)


@pytest.fixture
def tokyo_cache(db_cache: DatabaseCache) -> Iterator[DatabaseCache]:
    conn = connections["default"]
    original = conn.settings_dict["TIME_ZONE"]
    with override_settings(USE_TZ=True):
        conn.settings_dict["TIME_ZONE"] = "Asia/Tokyo"
        _reset_connection_timezone(conn)
        try:
            yield db_cache
        finally:
            conn.settings_dict["TIME_ZONE"] = original
            _reset_connection_timezone(conn)


def _reset_connection_timezone(conn) -> None:
    for attr in ("timezone", "timezone_name"):
        conn.__dict__.pop(attr, None)


# Regression: an aware ``datetime.max`` converted to a ``TIME_ZONE`` east of UTC pushed the year past 9999.
def test_compound_insert_writes_a_persistent_row_east_of_utc(tokyo_cache: DatabaseCache):
    tokyo_cache.rpush("l", 1, 2)
    assert tokyo_cache.lrange("l", 0, -1) == [1, 2]
    assert tokyo_cache.ttl("l") is None


def test_persist_clears_the_expiry_east_of_utc(tokyo_cache: DatabaseCache):
    tokyo_cache.set("k", 1, timeout=300)
    assert tokyo_cache.persist("k") is True
    assert tokyo_cache.ttl("k") is None


def test_ttl_is_reported_in_the_database_time_zone(tokyo_cache: DatabaseCache):
    tokyo_cache.set("k", 1, timeout=300)
    ttl = tokyo_cache.ttl("k")
    assert ttl is not None
    assert 290 < ttl <= 300


def test_info_excludes_the_no_expiry_sentinel_east_of_utc(tokyo_cache: DatabaseCache):
    tokyo_cache.set("forever", 1, timeout=None)
    tokyo_cache.set("ticking", 1, timeout=600)
    keyspace = tokyo_cache.info()["keyspace"]["db0"]
    assert keyspace["keys"] == 2
    assert keyspace["expires"] == 1


def test_persist_on_a_key_without_a_ttl_returns_false(db_cache: DatabaseCache):
    db_cache.set("plain", 1, timeout=None)
    db_cache.rpush("list", 1)
    assert db_cache.persist("plain") is False
    assert db_cache.persist("list") is False
    assert db_cache.persist("absent") is False


def test_persist_on_a_key_with_a_ttl_returns_true(db_cache: DatabaseCache):
    db_cache.set("k", 1, timeout=300)
    assert db_cache.persist("k") is True
    assert db_cache.ttl("k") is None
    assert db_cache.persist("k") is False


def test_hdel_counts_a_repeated_field_once(db_cache: DatabaseCache):
    db_cache.hset("k", mapping={"a": 1, "b": 2})
    assert db_cache.hdel("k", "a", "a") == 1
    assert db_cache.hkeys("k") == ["b"]


def test_zrem_counts_a_repeated_member_once(db_cache: DatabaseCache):
    db_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert db_cache.zrem("k", "a", "a") == 1
    assert db_cache.zrange("k", 0, -1) == ["b"]


def test_zincrby_rejects_a_nan_sum(db_cache: DatabaseCache):
    db_cache.zadd("k", {"a": float("inf")})
    with pytest.raises(ValueError, match="NaN"):
        db_cache.zincrby("k", float("-inf"), "a")
    assert db_cache.zscore("k", "a") == float("inf")


def test_lpos_negative_count_rejected(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b", "a", "c", "a")
    with pytest.raises(ValueError, match="COUNT can't be negative"):
        db_cache.lpos("l", "a", count=-1)


def test_lpos_negative_maxlen_rejected(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "b", "a")
    with pytest.raises(ValueError, match="MAXLEN can't be negative"):
        db_cache.lpos("l", "a", maxlen=-1)


def test_spop_negative_count_rejected(db_cache: DatabaseCache):
    db_cache.sadd("s", "a", "b")
    with pytest.raises(ValueError, match="must be positive"):
        db_cache.spop("s", count=-1)
    assert db_cache.scard("s") == 2


def test_linsert_rejects_an_unknown_position(db_cache: DatabaseCache):
    db_cache.rpush("l", "a", "c")
    with pytest.raises(ValueError, match="syntax error"):
        db_cache.linsert("l", "SIDEWAYS", "c", "b")
    assert db_cache.lrange("l", 0, -1) == ["a", "c"]


def test_hset_odd_items_leaves_the_hash_alone(db_cache: DatabaseCache):
    db_cache.hset("h", "a", 1)
    # Same message as the RESP backends.
    with pytest.raises(ValueError, match="items must hold field/value pairs"):
        db_cache.hset("h", "b", 2, items=["c"])
    assert db_cache.hgetall("h") == {"a": 1}


@pytest.mark.parametrize(
    "flags",
    [{"nx": True, "xx": True}, {"gt": True, "lt": True}, {"nx": True, "gt": True}, {"nx": True, "lt": True}],
    ids=["nx+xx", "gt+lt", "nx+gt", "nx+lt"],
)
def test_zadd_rejects_the_flag_combinations_redis_py_rejects(db_cache: DatabaseCache, flags):
    db_cache.zadd("z", {"m": 1.0})
    with pytest.raises(ValueError, match="ZADD"):
        db_cache.zadd("z", {"m": 2.0, "n": 3.0}, **flags)
    assert db_cache.zrange("z", 0, -1, withscores=True) == [("m", 1.0)]


# SQLite does not abort the transaction on a failed statement, so the test asserts the savepoint round trip itself.
def test_info_failed_counts_roll_back_a_savepoint(db_cache: DatabaseCache):
    conn = connections["default"]
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE {conn.ops.quote_name(db_cache._get_table_name())}")
    with CaptureQueriesContext(conn) as ctx:
        info = db_cache.info()
    assert info["keyspace"]["db0"] == {"keys": 0, "expires": 0}
    statements = [q["sql"] for q in ctx.captured_queries]
    assert any(sql.startswith("SAVEPOINT") for sql in statements)
    assert any(sql.startswith("ROLLBACK TO SAVEPOINT") for sql in statements)


def _seed_twin_data(cache: DatabaseCache) -> None:
    cache.clear()
    cache.set("s", 5, timeout=3600)
    cache.rpush("l", "a", "b", "a", "c")
    cache.sadd("one", "a")
    cache.sadd("two", "a", "b")
    cache.hset("h", mapping={"f": 1, "g": 2.5})
    cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})


def _twin_state(cache: DatabaseCache) -> tuple[dict[tuple[int, str], object], dict[tuple[int, str], int | None]]:
    versions = (cache.version - 1, cache.version, cache.version + 1)
    stored = [(version, key) for version in versions for key in cache.keys(version=version)]
    values = {(version, key): cache._read(cache._internal_key(key, version=version)) for version, key in stored}
    return values, {(version, key): cache.ttl(key, version=version) for version, key in stored}


async def _call_async_twin(method, args, kwargs):
    call = method(*args, **kwargs)
    return [item async for item in call] if inspect.isasyncgen(call) else await call


_ASYNC_TWIN_CASES = [
    ("aset", ("s", 7, 100), {}),
    ("aset", ("new", 7), {"nx": True}),
    ("aget", ("s",), {}),
    ("aadd", ("new", 1), {}),
    ("atouch", ("s", 100), {}),
    ("adelete", ("l",), {}),
    ("aget_or_set", ("new", lambda: 3), {}),
    ("aset_many", ({"s": 1, "new": 2},), {}),
    ("adelete_many", (["s", "l", "missing"],), {}),
    ("aclear", (), {}),
    ("aclose", (), {}),
    ("ahas_key", ("l",), {}),
    ("aincr", ("s",), {}),
    ("adecr", ("s", 2), {}),
    ("aget_many", (["s", "l", "missing"],), {}),
    ("aincr_version", ("l",), {}),
    ("adecr_version", ("l",), {}),
    ("attl", ("l",), {}),
    ("atype", ("z",), {}),
    ("apersist", ("s",), {}),
    ("aexpire", ("l", 100), {}),
    ("akeys", ("*",), {}),
    ("ascan", (0,), {"count": 1, "key_type": KeyType.SET}),
    ("aiter_keys", ("*",), {}),
    ("adelete_pattern", ("*o*",), {}),
    ("alpush", ("l", "x", "y"), {}),
    ("arpush", ("l", "x"), {}),
    ("alpop", ("l",), {"count": 2}),
    ("arpop", ("l",), {}),
    ("alrange", ("l", 0, -1), {}),
    ("allen", ("l",), {}),
    ("alrem", ("l", 0, "a"), {}),
    ("altrim", ("l", 0, 1), {}),
    ("alindex", ("l", 1), {}),
    ("alset", ("l", 0, "z"), {}),
    ("alinsert", ("l", "BEFORE", "b", "q"), {}),
    ("alpos", ("l", "a"), {"count": 0}),
    ("asadd", ("one", "b", "c"), {}),
    ("asrem", ("two", "a", "zz"), {}),
    ("ascard", ("two",), {}),
    ("asismember", ("one", "a"), {}),
    ("asmembers", ("two",), {}),
    ("aspop", ("one",), {}),
    ("asrandmember", ("one", -3), {}),
    ("asmismember", ("two", "a", "x"), {}),
    ("asdiff", (["two", "one"],), {}),
    ("asinter", (["two", "one"],), {}),
    ("asunion", (["two", "one"],), {}),
    ("ahset", ("h", "n", 1), {"mapping": {"m": 2}}),
    ("ahdel", ("h", "f", "zz"), {}),
    ("ahget", ("h", "f"), {}),
    ("ahgetall", ("h",), {}),
    ("ahlen", ("h",), {}),
    ("ahkeys", ("h",), {}),
    ("ahvals", ("h",), {}),
    ("ahexists", ("h", "g"), {}),
    ("ahmget", ("h", "f", "zz"), {}),
    ("ahsetnx", ("h", "new", 1), {}),
    ("ahincrby", ("h", "f", 2), {}),
    ("ahincrbyfloat", ("h", "g", 0.5), {}),
    ("azadd", ("z", {"d": 4.0, "a": 0.5}), {"ch": True}),
    ("azcard", ("z",), {}),
    ("azscore", ("z", "b"), {}),
    ("azrank", ("z", "c"), {}),
    ("azrevrank", ("z", "c"), {}),
    ("azrange", ("z", 0, 1), {"withscores": True}),
    ("azrevrange", ("z", 0, 1), {}),
    ("azrangebyscore", ("z", 1, 2), {"withscores": True}),
    ("azrevrangebyscore", ("z", 3, 2), {}),
    ("azrem", ("z", "a", "zz"), {}),
    ("azincrby", ("z", 2.5, "a"), {}),
    ("azcount", ("z", 1, 2), {}),
    ("azpopmin", ("z",), {"count": 2}),
    ("azpopmax", ("z",), {}),
    ("azmscore", ("z", "a", "zz"), {}),
    ("azremrangebyrank", ("z", 0, 0), {}),
    ("azremrangebyscore", ("z", 2, 3), {}),
]


@pytest.mark.parametrize(("name", "args", "kwargs"), _ASYNC_TWIN_CASES, ids=[case[0] for case in _ASYNC_TWIN_CASES])
def test_async_twin_matches_sync(db_cache: DatabaseCache, name, args, kwargs):
    """``async_to_sync`` runs the ``sync_to_async`` query back on this thread, which owns the test transaction."""
    _seed_twin_data(db_cache)
    expected = getattr(db_cache, name.removeprefix("a"))(*args, **kwargs)
    if inspect.isgenerator(expected):
        expected = list(expected)
    expected_values, expected_ttls = _twin_state(db_cache)
    _seed_twin_data(db_cache)
    result = async_to_sync(_call_async_twin)(getattr(db_cache, name), args, kwargs)
    assert result == expected
    assert type(result) is type(expected)
    values, ttls = _twin_state(db_cache)
    assert values == expected_values
    assert ttls == pytest.approx(expected_ttls, abs=1)


def test_async_twin_cases_cover_every_async_method():
    owners = {
        name: next(klass for klass in DatabaseCache.__mro__ if name in vars(klass))
        for name in dir(DatabaseCache)
        if inspect.iscoroutinefunction(getattr(DatabaseCache, name))
        or inspect.isasyncgenfunction(getattr(DatabaseCache, name))
    }
    covered = {case[0] for case in _ASYNC_TWIN_CASES}
    assert {name for name, owner in owners.items() if owner is not BaseCachex} == covered
