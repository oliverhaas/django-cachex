"""Tests for ``django_cachex.cache.database.DatabaseCache``.

Limited coverage focused on the cachex-contract surface that
``test_base.py`` defers ("TBD"). The full per-op battery still lives with
the RESP-backend tests via the parametrized fixtures.
"""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.core.management import call_command
from django.test import override_settings

from django_cachex.cache.database import _MISSING
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
        # Standard set returns None on success
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
    """``_coerce_*`` raises :class:`WrongTypeError`, not plain ``TypeError`` (B4).

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


class TestTypeDetection:
    """``type()`` tells a sorted set apart from a hash even though both are
    stored as dicts (regression: zsets were misreported as ``HASH``)."""

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
            return [*existing, "v"], "ret"

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
            return [*existing, "b"], len(existing) + 1

        assert db_cache._atomic_compound(internal_key, transform) == 2
        assert db_cache.lrange("racy", 0, -1) == ["a", "b"]


class TestLikePatternEscaping:
    """Literal ``%``, ``_``, and ``\\`` in keys and patterns match literally."""

    def test_underscore_in_pattern_is_literal(self, db_cache: DatabaseCache):
        db_cache.set("foo_bar", 1)
        db_cache.set("fooxbar", 1)
        assert db_cache.keys("foo_bar") == ["foo_bar"]

    def test_percent_in_pattern_is_literal(self, db_cache: DatabaseCache):
        db_cache.set("100%", 1)
        db_cache.set("100pc", 1)
        assert db_cache.keys("100%") == ["100%"]

    def test_backslash_in_pattern_is_literal(self, db_cache: DatabaseCache):
        db_cache.set(r"a\b", 1)
        db_cache.set("axb", 1)
        assert db_cache.keys(r"a\b") == [r"a\b"]

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
