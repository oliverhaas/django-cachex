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
