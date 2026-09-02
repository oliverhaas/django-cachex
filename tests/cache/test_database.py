"""Tests for ``django_cachex.cache.database.DatabaseCache``.

Covers the parts of the cachex contract that are specific to the cache
table: atomic compound ops, SQL pattern translation and the typed scan. The
full per-op battery lives with the RESP-backend tests via the parametrized
fixtures.
"""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.core.management import call_command
from django.test import override_settings

from django_cachex.cache.base import BaseCachex
from django_cachex.cache.database import _MISSING, _List
from django_cachex.cache.database import DatabaseCache as DatabaseCacheClass
from django_cachex.exceptions import NotSupportedError, WrongTypeError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache.database import DatabaseCache


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
        yield cache  # type: ignore[misc]


class TestSetFlags:
    """DatabaseCache nx implemented via ``_base_set("add", ...)``; xx/get raise."""

    def test_nx_new_key_writes(self, db_cache: DatabaseCache):
        assert db_cache.set("k", "v", nx=True) is True
        assert db_cache.get("k") == "v"

    def test_nx_existing_key_no_write(self, db_cache: DatabaseCache):
        db_cache.set("k", "old")
        assert db_cache.set("k", "new", nx=True) is False
        assert db_cache.get("k") == "old"

    def test_no_flags_delegates_to_django(self, db_cache: DatabaseCache):
        assert db_cache.set("k", "v") is None
        assert db_cache.get("k") == "v"

    def test_xx_raises_not_supported(self, db_cache: DatabaseCache):
        with pytest.raises(NotSupportedError):
            db_cache.set("k", "v", xx=True)

    def test_get_raises_not_supported(self, db_cache: DatabaseCache):
        with pytest.raises(NotSupportedError):
            db_cache.set("k", "v", get=True)

    # Async tests are deliberately omitted: Django's DatabaseCache.aset
    # bridges through ``sync_to_async``, which on SQLite ``:memory:`` hits
    # "schema is locked" because the in-memory DB has a single connection.
    # The async path adds no logic beyond the bridge; the sync tests above
    # cover the cachex contract.


class TestWrongTypeNormalization:
    """``_coerce_*`` raises :class:`WrongTypeError`, not plain ``TypeError``.

    Cross-backend code that catches ``WrongTypeError`` must work against
    DatabaseCache too; raising plain ``TypeError`` previously broke that
    contract.
    """

    def test_lpush_on_string_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            db_cache.lpush("k", "x")

    def test_sadd_on_string_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            db_cache.sadd("k", "m")

    def test_hset_on_string_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            db_cache.hset("k", "f", "v")

    def test_zadd_on_string_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            db_cache.zadd("k", {"m": 1.0})

    def test_wrongtype_is_typeerror_subclass(self, db_cache: DatabaseCache):
        db_cache.set("k", "abc")
        # Existing call sites that catch the broader TypeError must still
        # work, since WrongTypeError is a TypeError subclass.
        with pytest.raises(TypeError):
            db_cache.lpush("k", "x")


class TestStringReads:
    """``get``/``get_many``/``incr`` treat a tagged collection as WRONGTYPE.

    Regression: the private ``_List``/``_Set``/``_Hash``/``_ZSet`` container
    leaked out of ``get()``, and ``incr`` on one raised a bare ``TypeError``
    from ``value + delta``.
    """

    def test_get_on_collection_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.rpush("lk", 1)
        with pytest.raises(WrongTypeError, match="'lk'"):
            db_cache.get("lk")

    def test_get_missing_key_returns_default(self, db_cache: DatabaseCache):
        assert db_cache.get("absent", "fallback") == "fallback"

    def test_get_many_skips_collection_keys(self, db_cache: DatabaseCache):
        # MGET reports a list/hash/set/zset key as nil, so RespCache omits it.
        db_cache.set("plain", 1)
        db_cache.rpush("lst", "a")
        db_cache.hset("hsh", "f", "v")
        assert db_cache.get_many(["plain", "lst", "hsh", "missing"]) == {"plain": 1}

    def test_incr_on_collection_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.rpush("lk", 1)
        with pytest.raises(WrongTypeError):
            db_cache.incr("lk")

    def test_wrongtype_message_uses_the_user_key(self, db_cache: DatabaseCache):
        db_cache.set("sk", "abc")
        with pytest.raises(WrongTypeError, match="'sk'") as exc_info:
            db_cache.lpush("sk", 1)
        assert ":1:sk" not in str(exc_info.value)


class TestTTLReporting:
    """A key with no expiry reports ``None``; a missing key reports ``-2``."""

    def test_persistent_key_reports_none(self, db_cache: DatabaseCache):
        db_cache.set("forever", 1, timeout=None)
        assert db_cache.ttl("forever") is None

    def test_missing_key_reports_minus_two(self, db_cache: DatabaseCache):
        assert db_cache.ttl("absent") == -2

    def test_expiring_key_reports_seconds(self, db_cache: DatabaseCache):
        db_cache.set("ticking", 1, timeout=3600)
        assert 3590 <= db_cache.ttl("ticking") <= 3600

    def test_compound_ops_leave_the_key_persistent(self, db_cache: DatabaseCache):
        db_cache.rpush("l", 1)
        db_cache.rpush("l", 2)
        assert db_cache.ttl("l") is None


class TestTypeDetection:
    """``type()`` reports the RESP type of the op that created the key;
    plain ``set()`` values are opaque and report ``STRING``."""

    def test_zset_reports_zset(self, db_cache: DatabaseCache):
        db_cache.zadd("zk", {"a": 1.0, "b": 2.0})
        assert db_cache.type("zk") == KeyType.ZSET

    def test_zset_with_string_members_reports_zset(self, db_cache: DatabaseCache):
        # String members structurally resemble a hash; the tag disambiguates.
        db_cache.zadd("zs", {"x": 1.0})
        assert db_cache.type("zs") == KeyType.ZSET

    def test_hash_reports_hash(self, db_cache: DatabaseCache):
        db_cache.hset("hk", "field", "value")
        assert db_cache.type("hk") == KeyType.HASH

    def test_list_reports_list(self, db_cache: DatabaseCache):
        db_cache.rpush("lk", "a")
        assert db_cache.type("lk") == KeyType.LIST

    def test_set_reports_set(self, db_cache: DatabaseCache):
        db_cache.sadd("sk", "m")
        assert db_cache.type("sk") == KeyType.SET

    @pytest.mark.parametrize(
        "value",
        [[1, 2, 3], {"a": "b"}, {1, 2, 3}, "plain", 42],
        ids=["list", "dict", "set", "str", "int"],
    )
    def test_plain_set_value_reports_string(self, db_cache: DatabaseCache, value):
        db_cache.set("k", value)
        assert db_cache.type("k") == KeyType.STRING


class TestTypeAliasing:
    """Compound ops reject keys created by a different RESP type (WRONGTYPE).

    Regression: hashes and sorted sets are both dicts on disk, so ``zadd``
    silently converted a hash and ``hget`` then returned a float."""

    def test_zadd_on_hash_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.hset("k", "a", "x")
        with pytest.raises(WrongTypeError):
            db_cache.zadd("k", {"b": 1.0})
        assert db_cache.type("k") == KeyType.HASH
        assert db_cache.hgetall("k") == {"a": "x"}

    def test_hget_on_zset_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.zadd("k", {"m": 1.0})
        with pytest.raises(WrongTypeError):
            db_cache.hget("k", "m")
        assert db_cache.type("k") == KeyType.ZSET

    def test_hset_on_zset_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.zadd("k", {"m": 1.0})
        with pytest.raises(WrongTypeError):
            db_cache.hset("k", "f", "v")

    def test_lpush_on_plain_list_value_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", [1, 2, 3])
        with pytest.raises(WrongTypeError):
            db_cache.lpush("k", "x")

    def test_sadd_on_plain_set_value_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", {1, 2, 3})
        with pytest.raises(WrongTypeError):
            db_cache.sadd("k", "m")

    def test_hset_on_plain_dict_value_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", {"a": "b"})
        with pytest.raises(WrongTypeError):
            db_cache.hset("k", "f", "v")

    def test_llen_on_plain_list_value_raises_wrongtype(self, db_cache: DatabaseCache):
        db_cache.set("k", [1, 2, 3])
        with pytest.raises(WrongTypeError):
            db_cache.llen("k")


class TestNoEmptyCollectionRows:
    """No-op compound writes must not create a permanent empty-collection row.

    Regression: the empty container was neither ``_DELETE`` nor ``_MISSING``,
    so it was INSERTed with ``expires = datetime.max``."""

    def test_zadd_xx_on_missing_key_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.zadd("z", {"m": 1.0}, xx=True) == 0
        assert db_cache.has_key("z") is False
        assert db_cache.type("z") is None

    def test_zadd_empty_mapping_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.zadd("z", {}) == 0
        assert db_cache.has_key("z") is False

    def test_sadd_no_members_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.sadd("s") == 0
        assert db_cache.has_key("s") is False

    def test_hset_no_fields_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.hset("h") == 0
        assert db_cache.has_key("h") is False

    def test_lpush_no_values_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.lpush("l") == 0
        assert db_cache.has_key("l") is False

    def test_rpush_no_values_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.rpush("l") == 0
        assert db_cache.has_key("l") is False

    def test_zadd_xx_on_existing_key_still_updates(self, db_cache: DatabaseCache):
        db_cache.zadd("z", {"m": 1.0})
        assert db_cache.zadd("z", {"m": 5.0}, xx=True) == 0
        assert db_cache.zscore("z", "m") == 5.0


class TestMissingSentinelNotPersisted:
    """A transform returning ``_MISSING`` must be a no-write, not an upsert.

    Regression: ``_atomic_compound`` pickled the module-level ``_MISSING``
    sentinel into the row, creating phantom rows for no-op calls on absent
    keys and destroying live rows for no-op calls on existing keys.
    """

    def test_lpop_missing_key_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.lpop("absent") is None
        assert db_cache.has_key("absent") is False
        assert db_cache.type("absent") is None

    def test_lrem_missing_key_creates_no_row(self, db_cache: DatabaseCache):
        assert db_cache.lrem("absent", 0, "x") == 0
        assert db_cache.has_key("absent") is False

    def test_lrem_no_match_preserves_list(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "a", "b")
        assert db_cache.lrem("l", 0, "z") == 0
        assert db_cache.lrange("l", 0, -1) == ["a", "b"]

    def test_hsetnx_existing_field_preserves_hash(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", "v")
        assert db_cache.hsetnx("h", "f", "other") is False
        assert db_cache.hgetall("h") == {"f": "v"}

    def test_hdel_missing_field_preserves_hash(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", "v")
        assert db_cache.hdel("h", "nope") == 0
        assert db_cache.hgetall("h") == {"f": "v"}

    def test_linsert_missing_pivot_preserves_list(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "a", "b")
        assert db_cache.linsert("l", "BEFORE", "nope", "x") == -1
        assert db_cache.lrange("l", 0, -1) == ["a", "b"]

    def test_zrem_missing_member_preserves_zset(self, db_cache: DatabaseCache):
        db_cache.zadd("z", {"m": 1.0})
        assert db_cache.zrem("z", "nope") == 0
        assert db_cache.zscore("z", "m") == 1.0


class TestPopCountZero:
    """``count=0`` pops nothing and returns an empty list, like Redis.

    Regression: ``rpop(key, count=0)`` sliced ``existing[-0:]``, popping the
    entire list and deleting the row.
    """

    def test_rpop_count_zero_returns_empty_and_keeps_list(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "a", "b", "c")
        assert db_cache.rpop("l", count=0) == []
        assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]

    def test_lpop_count_zero_returns_empty_and_keeps_list(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "a", "b", "c")
        assert db_cache.lpop("l", count=0) == []
        assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]


class TestRedisArgumentValidation:
    """Counts and ranks Redis rejects are rejected here, not silently reinterpreted."""

    @pytest.mark.parametrize("method", ["lpop", "rpop"])
    def test_pop_negative_count_rejected(self, db_cache: DatabaseCache, method: str):
        db_cache.rpush("l", "a", "b", "c")
        with pytest.raises(ValueError, match="must be positive"):
            getattr(db_cache, method)("l", -2)
        assert db_cache.lrange("l", 0, -1) == ["a", "b", "c"]

    @pytest.mark.parametrize("method", ["lpop", "rpop"])
    def test_pop_negative_count_rejected_on_missing_key(self, db_cache: DatabaseCache, method: str):
        with pytest.raises(ValueError, match="must be positive"):
            getattr(db_cache, method)("absent", -1)

    @pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
    def test_zpop_negative_count_rejected(self, db_cache: DatabaseCache, method: str):
        db_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
        with pytest.raises(ValueError, match="must be positive"):
            getattr(db_cache, method)("z", -2)
        assert db_cache.zcard("z") == 3

    def test_lpos_rank_zero_rejected(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "a")
        with pytest.raises(ValueError, match="RANK can't be zero"):
            db_cache.lpos("l", "a", rank=0)

    def test_lpos_negative_rank_scans_the_tail_within_maxlen(self, db_cache: DatabaseCache):
        db_cache.rpush("l", "b", "a", "c", "b")
        assert db_cache.lpos("l", "b", rank=-1, maxlen=2) == 3
        assert db_cache.lpos("l", "b", rank=-1, count=0) == [3, 0]

    def test_srandmember_negative_count_allows_repeats(self, db_cache: DatabaseCache):
        db_cache.sadd("s", "a")
        assert db_cache.srandmember("s", count=-3) == ["a", "a", "a"]
        assert db_cache.scard("s") == 1


class TestSetAlgebra:
    """``sdiff``/``sinter``/``sunion`` read all their keys in one query."""

    def test_sinter_across_three_keys(self, db_cache: DatabaseCache):
        db_cache.sadd("a", "x", "y", "z")
        db_cache.sadd("b", "y", "z")
        db_cache.sadd("c", "z")
        assert db_cache.sinter(["a", "b", "c"]) == {"z"}

    def test_missing_keys_read_empty(self, db_cache: DatabaseCache):
        db_cache.sadd("a", "x")
        assert db_cache.sdiff(["a", "absent"]) == {"x"}

    def test_wrongtype_names_the_offending_key(self, db_cache: DatabaseCache):
        db_cache.sadd("a", "x")
        db_cache.set("b", "plain")
        with pytest.raises(WrongTypeError, match="'b'"):
            db_cache.sunion(["a", "b"])


class TestInsertRaceRetriesTransform:
    """A lost insert race re-runs the transform against the winner's row."""

    def test_concurrent_set_during_transform(self, db_cache: DatabaseCache):
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

    def test_concurrent_compound_during_transform(self, db_cache: DatabaseCache):
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


class TestLikePatternEscaping:
    """``%`` and ``_`` stay literal in a pattern; ``\\`` escapes, as in Redis."""

    def test_underscore_in_pattern_is_literal(self, db_cache: DatabaseCache):
        db_cache.set("foo_bar", 1)
        db_cache.set("fooxbar", 1)
        assert db_cache.keys("foo_bar") == ["foo_bar"]

    def test_percent_in_pattern_is_literal(self, db_cache: DatabaseCache):
        db_cache.set("100%", 1)
        db_cache.set("100pc", 1)
        assert db_cache.keys("100%") == ["100%"]

    def test_backslash_escapes_the_next_character(self, db_cache: DatabaseCache):
        db_cache.set(r"a\b", 1)
        db_cache.set("ab", 1)
        # Redis globs read ``\x`` as a literal ``x``, so this names the key ``ab``.
        assert db_cache.keys(r"a\b") == ["ab"]
        assert db_cache.keys(r"a\\b") == [r"a\b"]

    def test_character_class_matches_one_member(self, db_cache: DatabaseCache):
        db_cache.set("ka", 1)
        db_cache.set("kb", 2)
        db_cache.set("kc", 3)
        assert db_cache.keys("k[ab]") == ["ka", "kb"]

    def test_negated_character_class(self, db_cache: DatabaseCache):
        db_cache.set("ka", 1)
        db_cache.set("kb", 2)
        assert db_cache.keys("k[^a]") == ["kb"]

    def test_glob_wildcards_still_translate(self, db_cache: DatabaseCache):
        db_cache.set("foo_bar", 1)
        db_cache.set("fooxbar", 1)
        db_cache.set("other", 1)
        assert sorted(db_cache.keys("foo*")) == ["foo_bar", "fooxbar"]
        assert sorted(db_cache.keys("foo?bar")) == ["foo_bar", "fooxbar"]

    def test_delete_pattern_with_literal_underscore(self, db_cache: DatabaseCache):
        db_cache.set("foo_bar", 1)
        db_cache.set("fooxbar", 1)
        assert db_cache.delete_pattern("foo_bar") == 1
        assert db_cache.get("fooxbar") == 1


class TestNumericCoercionErrors:
    """Increment ops reject values real Redis rejects instead of coercing."""

    def test_hincrby_non_integer_field_raises(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", "abc")
        with pytest.raises(ValueError, match="not an integer"):
            db_cache.hincrby("h", "f", 1)
        assert db_cache.hget("h", "f") == "abc"

    def test_hincrby_float_field_raises(self, db_cache: DatabaseCache):
        # Redis rejects HINCRBY on a float value; int() would truncate it.
        db_cache.hset("h", "f", 3.5)
        with pytest.raises(ValueError, match="not an integer"):
            db_cache.hincrby("h", "f", 1)
        assert db_cache.hget("h", "f") == 3.5

    def test_hincrby_int_field_increments(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", 5)
        assert db_cache.hincrby("h", "f", 2) == 7

    def test_hincrbyfloat_non_numeric_field_raises(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", "abc")
        with pytest.raises(ValueError, match="not a float"):
            db_cache.hincrbyfloat("h", "f", 1.0)
        assert db_cache.hget("h", "f") == "abc"

    def test_hincrbyfloat_int_field_increments(self, db_cache: DatabaseCache):
        db_cache.hset("h", "f", 2)
        assert db_cache.hincrbyfloat("h", "f", 0.5) == 2.5

    def test_zadd_non_numeric_score_raises(self, db_cache: DatabaseCache):
        with pytest.raises(ValueError, match="not a valid float"):
            db_cache.zadd("z", {"m": "abc"})
        assert db_cache.zcard("z") == 0

    def test_zadd_numeric_string_score_coerced(self, db_cache: DatabaseCache):
        # Redis parses numeric strings as scores.
        assert db_cache.zadd("z", {"m": "1.5"}) == 1
        assert db_cache.zscore("z", "m") == 1.5

    def test_zincrby_non_numeric_amount_raises(self, db_cache: DatabaseCache):
        db_cache.zadd("z", {"m": 1.0})
        with pytest.raises(ValueError, match="not a valid float"):
            db_cache.zincrby("z", "abc", "m")
        assert db_cache.zscore("z", "m") == 1.0


class TestCompoundInsertCulling:
    """Compound-structure creates honor MAX_ENTRIES via the same cull check
    as plain ``set()``; all compound creates share the ``_atomic_compound``
    insert path."""

    @pytest.fixture
    def small_db_cache(self, db) -> Iterator[DatabaseCache]:
        call_command("createcachetable", "django_cachex_test_cache")
        with override_settings(CACHES=SMALL_DATABASE_CACHES):
            cache = caches["db_small"]
            cache.clear()
            yield cache  # type: ignore[misc]

    def test_compound_inserts_cull_when_over_max_entries(self, small_db_cache: DatabaseCache):
        for i in range(10):
            small_db_cache.sadd(f"k{i}", "m")
        # MAX_ENTRIES=4 with CULL_FREQUENCY=2 halves the table whenever an
        # insert finds it over the limit, so growth stays bounded.
        assert len(small_db_cache.keys("*")) <= 5


class TestZSetScoreRanges:
    """``LIMIT`` windows and score bounds follow Redis, not raw Python slicing."""

    @pytest.fixture
    def zset_cache(self, db_cache: DatabaseCache) -> DatabaseCache:
        db_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        return db_cache

    def test_negative_num_reaches_the_end(self, zset_cache: DatabaseCache):
        # Regression: the idiomatic ``LIMIT 0 -1`` sliced ``[0:-1]`` and
        # silently dropped the last member.
        assert zset_cache.zrangebyscore("z", "-inf", "+inf", start=0, num=-1) == ["a", "b", "c", "d"]

    def test_positive_num_windows(self, zset_cache: DatabaseCache):
        assert zset_cache.zrangebyscore("z", "-inf", "+inf", start=1, num=2) == ["b", "c"]

    def test_infinite_bounds_parse(self, zset_cache: DatabaseCache):
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
    def test_exclusive_bound_raises_not_supported(self, zset_cache: DatabaseCache, method, args):
        # Regression: ``float("(1")`` raised a bare ValueError instead of
        # telling the caller the bound style is unsupported.
        with pytest.raises(NotSupportedError):
            getattr(zset_cache, method)(*args)


class TestKeyPrefixWildcards:
    """Glob translation applies to the user pattern only, never to KEY_PREFIX."""

    def test_wildcard_in_key_prefix_is_literal(self, db):
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


class TestInfoKeyspace:
    """``keyspace.db0.expires`` counts keys that carry a TTL, like Redis INFO."""

    def test_no_expiry_keys_excluded_from_expires(self, db_cache: DatabaseCache):
        db_cache.set("forever", 1, timeout=None)
        db_cache.set("ticking", 1, timeout=600)
        keyspace = db_cache.info()["keyspace"]["db0"]
        assert keyspace["keys"] == 2
        assert keyspace["expires"] == 1


class TestSremNoOpDoesNotRewrite:
    """``srem`` short-circuits when nothing matched, like ``hdel``/``zrem``."""

    def test_srem_missing_member_preserves_set(self, db_cache: DatabaseCache):
        db_cache.sadd("s", "a")
        assert db_cache.srem("s", "nope") == 0
        assert db_cache.smembers("s") == {"a"}

    def test_srem_last_member_deletes_key(self, db_cache: DatabaseCache):
        db_cache.sadd("s", "a")
        assert db_cache.srem("s", "a") == 1
        assert db_cache.has_key("s") is False


class TestDeletePattern:
    """``delete_pattern`` batches its DELETEs and honors ``itersize``."""

    def test_deletes_all_matches(self, db_cache: DatabaseCache):
        for i in range(7):
            db_cache.set(f"p:{i}", i)
        db_cache.set("other", 1)
        assert db_cache.delete_pattern("p:*") == 7
        assert db_cache.keys("*") == ["other"]

    def test_itersize_chunks_do_not_change_the_result(self, db_cache: DatabaseCache):
        for i in range(7):
            db_cache.set(f"p:{i}", i)
        assert db_cache.delete_pattern("p:*", itersize=2) == 7
        assert db_cache.keys("*") == []


class TestScanSurface:
    """``scan`` resolves ``key_type`` in the listing query; ``ascan`` mirrors it."""

    @pytest.fixture
    def typed_cache(self, db_cache: DatabaseCache) -> DatabaseCache:
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
    def test_scan_filters_by_key_type(self, typed_cache: DatabaseCache, key_type, expected):
        assert typed_cache.scan(pattern="*", key_type=key_type) == (0, [expected])

    def test_scan_key_type_the_table_cannot_hold_matches_nothing(self, typed_cache: DatabaseCache):
        assert typed_cache.scan(pattern="*", key_type=KeyType.STREAM) == (0, [])

    def test_scan_without_key_type_returns_everything(self, db_cache: DatabaseCache):
        db_cache.set("plain", 1)
        db_cache.rpush("alist", "a")
        _, keys = db_cache.scan(pattern="*")
        assert sorted(keys) == ["alist", "plain"]

    def test_scan_paginates(self, db_cache: DatabaseCache):
        for i in range(5):
            db_cache.set(f"k{i}", i)
        cursor, page = db_cache.scan(count=2)
        assert page == ["k0", "k1"]
        cursor, page = db_cache.scan(cursor=cursor, count=2)
        assert page == ["k2", "k3"]
        cursor, page = db_cache.scan(cursor=cursor, count=2)
        assert (cursor, page) == (0, ["k4"])

    def test_scan_combines_pattern_and_type(self, typed_cache: DatabaseCache):
        typed_cache.rpush("blist", "b")
        assert typed_cache.scan(pattern="a*", key_type=KeyType.LIST) == (0, ["alist"])

    def test_ascan_is_overridden(self):
        # Regression: ``ascan`` was left at the BaseCachex default and raised
        # NotSupportedError even though the sync ``scan`` works.
        assert DatabaseCacheClass.ascan is not BaseCachex.ascan
