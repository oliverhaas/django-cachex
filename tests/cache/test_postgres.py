"""Tests for the PostgreSQL cache backend.

Uses a PostgreSQL testcontainer, runs Django migrations to create the
UNLOGGED tables, then exercises the full cache API through Django's
cache framework.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, cast

import pytest
from django.test import override_settings
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import KeyValueCache

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_PG_IMAGE = "postgres:17"
_PG_DB = "cachex_test"
_PG_USER = "cachex"
_PG_PASS = "cachex"
_DB_ALIAS = "pg_cache"


@pytest.fixture(scope="session")
def pg_container(django_db_blocker):
    """Start a PostgreSQL 17 container and run migrations (session-scoped)."""
    from django.core.management import call_command
    from django.db import connections

    container = DockerContainer(_PG_IMAGE)
    container.with_env("POSTGRES_DB", _PG_DB)
    container.with_env("POSTGRES_USER", _PG_USER)
    container.with_env("POSTGRES_PASSWORD", _PG_PASS)
    container.with_exposed_ports(5432)
    container.start()
    wait_for_logs(container, "database system is ready to accept connections", timeout=30)
    time.sleep(1)
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(5432))

    # Register the database alias so Django can connect
    connections.settings[_DB_ALIAS] = {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _PG_DB,
        "USER": _PG_USER,
        "PASSWORD": _PG_PASS,
        "HOST": host,
        "PORT": str(port),
        "ATOMIC_REQUESTS": False,
        "AUTOCOMMIT": True,
        "CONN_MAX_AGE": 0,
        "CONN_HEALTH_CHECKS": False,
        "OPTIONS": {},
        "TIME_ZONE": None,
        "TEST": {
            "CHARSET": None,
            "COLLATION": None,
            "MIGRATE": True,
            "MIRROR": None,
            "NAME": None,
        },
    }

    # Unblock DB access for migration, then retry until PostgreSQL is ready
    with django_db_blocker.unblock():
        for attempt in range(10):
            try:
                call_command(
                    "migrate",
                    "cachex_postgres",
                    database=_DB_ALIAS,
                    verbosity=0,
                )
                break
            except Exception:
                if attempt == 9:
                    raise
                time.sleep(0.5)

    yield {"host": host, "port": port}

    with django_db_blocker.unblock():
        try:
            connections[_DB_ALIAS].close()
            delattr(connections._connections, _DB_ALIAS)
        except AttributeError:
            pass
    connections.settings.pop(_DB_ALIAS, None)
    container.stop()


@pytest.fixture
def cache(pg_container, django_db_blocker) -> Iterator[KeyValueCache]:
    """Provide a fresh PostgreSQLCache instance, cleared before and after each test."""
    cache_settings = {
        "default": {
            "BACKEND": "django_cachex.cache.PostgreSQLCache",
            "LOCATION": "cachex",
            "OPTIONS": {
                "DATABASE": _DB_ALIAS,
            },
        },
    }

    # Use django_db_blocker to allow database access without @pytest.mark.django_db.
    # The mark doesn't work because it resolves databases="__all__" before our
    # pg_cache alias is registered, then blocks access to it.
    with django_db_blocker.unblock(), override_settings(CACHES=cache_settings):
        from django.core.cache import cache as default_cache

        pg_cache = cast("KeyValueCache", default_cache)
        pg_cache.clear()
        yield pg_cache
        pg_cache.clear()


# ---------------------------------------------------------------------------
# Core Cache Operations
# ---------------------------------------------------------------------------


class TestCoreOperations:
    def test_set_and_get(self, cache: KeyValueCache):
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing(self, cache: KeyValueCache):
        assert cache.get("nonexistent") is None

    def test_set_overwrite(self, cache: KeyValueCache):
        cache.set("k", "v1")
        cache.set("k", "v2")
        assert cache.get("k") == "v2"

    def test_add_only_if_missing(self, cache: KeyValueCache):
        assert cache.add("add_key", "first") is True
        assert cache.add("add_key", "second") is False
        assert cache.get("add_key") == "first"

    def test_delete(self, cache: KeyValueCache):
        cache.set("del_me", 1)
        assert cache.delete("del_me") is True
        assert cache.get("del_me") is None
        assert cache.delete("del_me") is False

    def test_has_key(self, cache: KeyValueCache):
        cache.set("exists", "yes")
        assert cache.has_key("exists") is True
        assert cache.has_key("nope") is False

    def test_get_many(self, cache: KeyValueCache):
        cache.set("a", 1)
        cache.set("b", 2)
        result = cache.get_many(["a", "b", "c"])
        assert result == {"a": 1, "b": 2}

    def test_set_many(self, cache: KeyValueCache):
        cache.set_many({"x": 10, "y": 20})
        assert cache.get("x") == 10
        assert cache.get("y") == 20

    def test_delete_many(self, cache: KeyValueCache):
        cache.set_many({"p": 1, "q": 2, "r": 3})
        deleted = cache.delete_many(["p", "q"])
        assert deleted == 2
        assert cache.get("p") is None
        assert cache.get("r") == 3

    def test_incr_decr(self, cache: KeyValueCache):
        cache.set("counter", 10)
        assert cache.incr("counter") == 11
        assert cache.incr("counter", 5) == 16
        assert cache.decr("counter", 3) == 13

    def test_incr_nonexistent_raises(self, cache: KeyValueCache):
        with pytest.raises(ValueError):
            cache.incr("missing_counter")

    def test_clear(self, cache: KeyValueCache):
        cache.set("a", 1)
        cache.set("b", 2)
        assert cache.clear() is True
        assert cache.get("a") is None


class TestDataTypes:
    def test_integer(self, cache: KeyValueCache):
        cache.set("int_val", 42)
        assert cache.get("int_val") == 42

    def test_float(self, cache: KeyValueCache):
        cache.set("float_val", 3.14)
        assert cache.get("float_val") == 3.14

    def test_string(self, cache: KeyValueCache):
        cache.set("str_val", "hello world")
        assert cache.get("str_val") == "hello world"

    def test_dict(self, cache: KeyValueCache):
        cache.set("dict_val", {"a": 1, "b": [2, 3]})
        assert cache.get("dict_val") == {"a": 1, "b": [2, 3]}

    def test_list(self, cache: KeyValueCache):
        cache.set("list_val", [1, "two", 3.0])
        assert cache.get("list_val") == [1, "two", 3.0]

    def test_bool(self, cache: KeyValueCache):
        cache.set("bool_val", True)
        assert cache.get("bool_val") is True

    def test_none(self, cache: KeyValueCache):
        cache.set("none_val", None)
        # None as value should round-trip (pickle handles it)
        assert cache.get("none_val") is None

    def test_unicode(self, cache: KeyValueCache):
        cache.set("unicode", "你好世界 🌍")
        assert cache.get("unicode") == "你好世界 🌍"


class TestSetFlags:
    def test_set_nx(self, cache: KeyValueCache):
        assert cache.set("nx_key", "first", nx=True) is True
        assert cache.set("nx_key", "second", nx=True) is False
        assert cache.get("nx_key") == "first"

    def test_set_xx(self, cache: KeyValueCache):
        assert cache.set("xx_key", "val", xx=True) is False
        cache.set("xx_key", "exists")
        assert cache.set("xx_key", "updated", xx=True) is True
        assert cache.get("xx_key") == "updated"

    def test_set_get(self, cache: KeyValueCache):
        cache.set("get_key", "old")
        old = cache.set("get_key", "new", get=True)
        assert old == "old"
        assert cache.get("get_key") == "new"

    def test_set_get_missing(self, cache: KeyValueCache):
        result = cache.set("get_missing", "val", get=True)
        assert result is None


# ---------------------------------------------------------------------------
# TTL Operations
# ---------------------------------------------------------------------------


class TestTTL:
    def test_set_with_timeout(self, cache: KeyValueCache):
        cache.set("ttl_key", "value", timeout=60)
        ttl = cache.ttl("ttl_key")
        assert ttl is not None
        assert 0 < ttl <= 60

    def test_ttl_no_expiry(self, cache: KeyValueCache):
        cache.set("no_ttl", "value", timeout=None)
        assert cache.ttl("no_ttl") is None

    def test_ttl_missing_key(self, cache: KeyValueCache):
        assert cache.ttl("gone") == -2

    def test_pttl(self, cache: KeyValueCache):
        cache.set("pttl_key", "value", timeout=60)
        pttl = cache.pttl("pttl_key")
        assert pttl is not None
        assert 0 < pttl <= 60000

    def test_persist(self, cache: KeyValueCache):
        cache.set("persist_key", "value", timeout=60)
        assert cache.persist("persist_key") is True
        assert cache.ttl("persist_key") is None

    def test_expire(self, cache: KeyValueCache):
        cache.set("exp_key", "value", timeout=None)
        assert cache.expire("exp_key", 30) is True
        ttl = cache.ttl("exp_key")
        assert ttl is not None
        assert 0 < ttl <= 30

    def test_touch(self, cache: KeyValueCache):
        cache.set("touch_key", "value", timeout=10)
        assert cache.touch("touch_key", timeout=60) is True
        ttl = cache.ttl("touch_key")
        assert ttl is not None
        assert ttl > 10

    def test_expiretime(self, cache: KeyValueCache):
        cache.set("etime", "val", timeout=600)
        etime = cache.expiretime("etime")
        assert etime is not None
        assert etime > 0


# ---------------------------------------------------------------------------
# Key Operations
# ---------------------------------------------------------------------------


class TestKeyOps:
    def test_keys_pattern(self, cache: KeyValueCache):
        cache.set("user:1", "a")
        cache.set("user:2", "b")
        cache.set("order:1", "c")
        result = cache.keys("*user*")
        assert sorted(result) == sorted([k for k in result if "user" in k])

    def test_type_string(self, cache: KeyValueCache):
        cache.set("str_type", "hello")
        assert cache.type("str_type") is not None

    def test_type_missing(self, cache: KeyValueCache):
        assert cache.type("missing_type") is None

    def test_delete_pattern(self, cache: KeyValueCache):
        cache.set("dp:1", "a")
        cache.set("dp:2", "b")
        cache.set("other", "c")
        deleted = cache.delete_pattern("dp:*")
        assert deleted == 2
        assert cache.get("other") == "c"

    def test_rename(self, cache: KeyValueCache):
        cache.set("src", "val")
        cache.rename("src", "dst")
        assert cache.get("src") is None
        assert cache.get("dst") == "val"

    def test_rename_missing_raises(self, cache: KeyValueCache):
        with pytest.raises(ValueError):
            cache.rename("no_such_key", "dst")

    def test_scan(self, cache: KeyValueCache):
        cache.set("scan:a", 1)
        cache.set("scan:b", 2)
        _, keys = cache.scan(pattern="scan:*")
        assert set(keys) == {"scan:a", "scan:b"}


# ---------------------------------------------------------------------------
# Hash Operations
# ---------------------------------------------------------------------------


class TestHashes:
    def test_hset_and_hget(self, cache: KeyValueCache):
        cache.hset("h1", "field1", "value1")
        assert cache.hget("h1", "field1") == "value1"
        assert cache.hget("h1", "missing") is None

    def test_hset_mapping(self, cache: KeyValueCache):
        cache.hset("h2", mapping={"a": 1, "b": 2, "c": 3})
        assert cache.hlen("h2") == 3
        assert cache.hget("h2", "b") == 2

    def test_hgetall(self, cache: KeyValueCache):
        cache.hset("h3", mapping={"x": "hello", "y": 42})
        result = cache.hgetall("h3")
        assert result == {"x": "hello", "y": 42}

    def test_hmget(self, cache: KeyValueCache):
        cache.hset("h4", mapping={"a": 1, "b": 2})
        assert cache.hmget("h4", "a", "b", "c") == [1, 2, None]

    def test_hdel(self, cache: KeyValueCache):
        cache.hset("h5", mapping={"a": 1, "b": 2, "c": 3})
        assert cache.hdel("h5", "b") == 1
        assert cache.hlen("h5") == 2

    def test_hexists(self, cache: KeyValueCache):
        cache.hset("h6", "field", "val")
        assert cache.hexists("h6", "field") is True
        assert cache.hexists("h6", "nope") is False

    def test_hkeys_hvals(self, cache: KeyValueCache):
        cache.hset("h7", mapping={"a": 1, "b": 2})
        assert sorted(cache.hkeys("h7")) == ["a", "b"]
        assert sorted(cache.hvals("h7")) == [1, 2]

    def test_hsetnx(self, cache: KeyValueCache):
        assert cache.hsetnx("h8", "f", "first") is True
        assert cache.hsetnx("h8", "f", "second") is False
        assert cache.hget("h8", "f") == "first"

    def test_hincrby(self, cache: KeyValueCache):
        cache.hset("h9", "count", 10)
        assert cache.hincrby("h9", "count", 5) == 15

    def test_hincrbyfloat(self, cache: KeyValueCache):
        cache.hset("h10", "score", 1.5)
        result = cache.hincrbyfloat("h10", "score", 0.5)
        assert abs(result - 2.0) < 0.01


# ---------------------------------------------------------------------------
# List Operations
# ---------------------------------------------------------------------------


class TestLists:
    def test_lpush_rpush_lrange(self, cache: KeyValueCache):
        cache.rpush("L1", "a", "b", "c")
        assert cache.lrange("L1", 0, -1) == ["a", "b", "c"]
        cache.lpush("L1", "z")
        assert cache.lrange("L1", 0, -1) == ["z", "a", "b", "c"]

    def test_lpop_rpop(self, cache: KeyValueCache):
        cache.rpush("L2", 1, 2, 3)
        assert cache.lpop("L2") == 1
        assert cache.rpop("L2") == 3
        assert cache.lrange("L2", 0, -1) == [2]

    def test_lpop_count(self, cache: KeyValueCache):
        cache.rpush("L3", "a", "b", "c", "d")
        result = cache.lpop("L3", count=2)
        assert result == ["a", "b"]
        assert cache.llen("L3") == 2

    def test_rpop_count(self, cache: KeyValueCache):
        cache.rpush("L4", "a", "b", "c", "d")
        result = cache.rpop("L4", count=2)
        assert result == ["d", "c"]

    def test_llen(self, cache: KeyValueCache):
        assert cache.llen("empty_list") == 0
        cache.rpush("L5", "a", "b")
        assert cache.llen("L5") == 2

    def test_lindex(self, cache: KeyValueCache):
        cache.rpush("L6", "a", "b", "c")
        assert cache.lindex("L6", 0) == "a"
        assert cache.lindex("L6", -1) == "c"
        assert cache.lindex("L6", 99) is None

    def test_lset(self, cache: KeyValueCache):
        cache.rpush("L7", "a", "b", "c")
        cache.lset("L7", 1, "B")
        assert cache.lrange("L7", 0, -1) == ["a", "B", "c"]

    def test_lrem(self, cache: KeyValueCache):
        cache.rpush("L8", "a", "b", "a", "c", "a")
        removed = cache.lrem("L8", 2, "a")
        assert removed == 2
        assert cache.llen("L8") == 3

    def test_ltrim(self, cache: KeyValueCache):
        cache.rpush("L9", "a", "b", "c", "d", "e")
        cache.ltrim("L9", 1, 3)
        assert cache.lrange("L9", 0, -1) == ["b", "c", "d"]

    def test_lmove(self, cache: KeyValueCache):
        cache.rpush("src_list", "a", "b", "c")
        result = cache.lmove("src_list", "dst_list", "LEFT", "RIGHT")
        assert result == "a"
        assert cache.lrange("dst_list", 0, -1) == ["a"]
        assert cache.lrange("src_list", 0, -1) == ["b", "c"]

    def test_linsert(self, cache: KeyValueCache):
        cache.rpush("L10", "a", "c")
        length = cache.linsert("L10", "BEFORE", "c", "b")
        assert length == 3
        assert cache.lrange("L10", 0, -1) == ["a", "b", "c"]

    def test_lpop_empty(self, cache: KeyValueCache):
        assert cache.lpop("empty") is None
        assert cache.lpop("empty", count=3) == []

    def test_lpos(self, cache: KeyValueCache):
        cache.rpush("L11", "a", "b", "c", "b", "d")
        assert cache.lpos("L11", "b") == 1
        assert cache.lpos("L11", "z") is None


# ---------------------------------------------------------------------------
# Set Operations
# ---------------------------------------------------------------------------


class TestSets:
    def test_sadd_smembers(self, cache: KeyValueCache):
        cache.sadd("S1", "a", "b", "c")
        assert cache.smembers("S1") == {"a", "b", "c"}

    def test_scard(self, cache: KeyValueCache):
        cache.sadd("S2", "x", "y")
        assert cache.scard("S2") == 2

    def test_sismember(self, cache: KeyValueCache):
        cache.sadd("S3", "a", "b")
        assert cache.sismember("S3", "a") is True
        assert cache.sismember("S3", "z") is False

    def test_smismember(self, cache: KeyValueCache):
        cache.sadd("S4", "a", "b")
        assert cache.smismember("S4", "a", "c") == [True, False]

    def test_srem(self, cache: KeyValueCache):
        cache.sadd("S5", "a", "b", "c")
        assert cache.srem("S5", "b") == 1
        assert cache.smembers("S5") == {"a", "c"}

    def test_spop(self, cache: KeyValueCache):
        cache.sadd("S6", "only")
        result = cache.spop("S6")
        assert result == "only"
        assert cache.scard("S6") == 0

    def test_srandmember(self, cache: KeyValueCache):
        cache.sadd("S7", "a", "b", "c")
        result = cache.srandmember("S7")
        assert result in {"a", "b", "c"}

    def test_sdiff(self, cache: KeyValueCache):
        cache.sadd("SA", "a", "b", "c")
        cache.sadd("SB", "b", "c", "d")
        assert cache.sdiff(["SA", "SB"]) == {"a"}

    def test_sinter(self, cache: KeyValueCache):
        cache.sadd("SC", "a", "b", "c")
        cache.sadd("SD", "b", "c", "d")
        assert cache.sinter(["SC", "SD"]) == {"b", "c"}

    def test_sunion(self, cache: KeyValueCache):
        cache.sadd("SE", "a", "b")
        cache.sadd("SF", "b", "c")
        assert cache.sunion(["SE", "SF"]) == {"a", "b", "c"}

    def test_smove(self, cache: KeyValueCache):
        cache.sadd("SG", "a", "b")
        cache.sadd("SH", "c")
        assert cache.smove("SG", "SH", "a") is True
        assert cache.smembers("SG") == {"b"}
        assert cache.smembers("SH") == {"a", "c"}

    def test_sdiffstore(self, cache: KeyValueCache):
        cache.sadd("SI", "a", "b", "c")
        cache.sadd("SJ", "b")
        count = cache.sdiffstore("SRESULT", ["SI", "SJ"])
        assert count == 2
        assert cache.smembers("SRESULT") == {"a", "c"}


# ---------------------------------------------------------------------------
# Sorted Set Operations
# ---------------------------------------------------------------------------


class TestSortedSets:
    def test_zadd_zrange(self, cache: KeyValueCache):
        cache.zadd("Z1", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrange("Z1", 0, -1) == ["a", "b", "c"]

    def test_zadd_withscores(self, cache: KeyValueCache):
        cache.zadd("Z2", {"x": 10.0, "y": 20.0})
        result = cache.zrange("Z2", 0, -1, withscores=True)
        assert result == [("x", 10.0), ("y", 20.0)]

    def test_zcard(self, cache: KeyValueCache):
        cache.zadd("Z3", {"a": 1.0, "b": 2.0})
        assert cache.zcard("Z3") == 2

    def test_zscore(self, cache: KeyValueCache):
        cache.zadd("Z4", {"member": 42.5})
        assert cache.zscore("Z4", "member") == 42.5
        assert cache.zscore("Z4", "missing") is None

    def test_zrank_zrevrank(self, cache: KeyValueCache):
        cache.zadd("Z5", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrank("Z5", "b") == 1
        assert cache.zrevrank("Z5", "b") == 1

    def test_zincrby(self, cache: KeyValueCache):
        cache.zadd("Z6", {"m": 5.0})
        assert cache.zincrby("Z6", 3.0, "m") == 8.0

    def test_zrem(self, cache: KeyValueCache):
        cache.zadd("Z7", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrem("Z7", "b") == 1
        assert cache.zcard("Z7") == 2

    def test_zpopmin(self, cache: KeyValueCache):
        cache.zadd("Z8", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zpopmin("Z8")
        assert result == [("a", 1.0)]
        assert cache.zcard("Z8") == 2

    def test_zpopmax(self, cache: KeyValueCache):
        cache.zadd("Z9", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zpopmax("Z9")
        assert result == [("c", 3.0)]

    def test_zcount(self, cache: KeyValueCache):
        cache.zadd("Z10", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert cache.zcount("Z10", 2, 3) == 2

    def test_zrangebyscore(self, cache: KeyValueCache):
        cache.zadd("Z11", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zrangebyscore("Z11", 1, 2)
        assert result == ["a", "b"]

    def test_zrevrange(self, cache: KeyValueCache):
        cache.zadd("Z12", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrevrange("Z12", 0, 1) == ["c", "b"]

    def test_zremrangebyscore(self, cache: KeyValueCache):
        cache.zadd("Z13", {"a": 1.0, "b": 2.0, "c": 3.0})
        removed = cache.zremrangebyscore("Z13", 1, 2)
        assert removed == 2
        assert cache.zrange("Z13", 0, -1) == ["c"]

    def test_zremrangebyrank(self, cache: KeyValueCache):
        cache.zadd("Z14", {"a": 1.0, "b": 2.0, "c": 3.0})
        removed = cache.zremrangebyrank("Z14", 0, 1)
        assert removed == 2
        assert cache.zrange("Z14", 0, -1) == ["c"]

    def test_zadd_nx(self, cache: KeyValueCache):
        cache.zadd("Z15", {"a": 1.0})
        cache.zadd("Z15", {"a": 5.0}, nx=True)
        assert cache.zscore("Z15", "a") == 1.0

    def test_zadd_xx(self, cache: KeyValueCache):
        cache.zadd("Z16", {"a": 1.0})
        count = cache.zadd("Z16", {"a": 5.0, "b": 2.0}, xx=True)
        assert cache.zscore("Z16", "a") == 5.0
        assert cache.zscore("Z16", "b") is None

    def test_zmscore(self, cache: KeyValueCache):
        cache.zadd("Z17", {"a": 1.0, "b": 2.0})
        assert cache.zmscore("Z17", "a", "b", "c") == [1.0, 2.0, None]


# ---------------------------------------------------------------------------
# Not Supported Operations
# ---------------------------------------------------------------------------


class TestNotSupported:
    def test_eval_script_raises(self, cache: KeyValueCache):
        with pytest.raises(NotSupportedError):
            cache.eval_script("return 1", keys=["k"])

    def test_streams_raise(self, cache: KeyValueCache):
        with pytest.raises(NotSupportedError):
            cache.xadd("stream", {"field": "value"})

    def test_blocking_ops_raise(self, cache: KeyValueCache):
        with pytest.raises(NotSupportedError):
            cache.blpop("key", timeout=1)


# ---------------------------------------------------------------------------
# Type Transitions
# ---------------------------------------------------------------------------


class TestTypeTransitions:
    def test_string_to_hash(self, cache: KeyValueCache):
        """SET a key as string, then HSET — should work (type transitions)."""
        cache.set("tkey", "string_value")
        cache.hset("tkey", "field", "hash_value")
        assert cache.hget("tkey", "field") == "hash_value"
        assert cache.get("tkey") is None

    def test_hash_to_string(self, cache: KeyValueCache):
        cache.hset("tkey2", "f", "v")
        cache.set("tkey2", "now_a_string")
        assert cache.get("tkey2") == "now_a_string"
        assert cache.hget("tkey2", "f") is None

    def test_list_to_set(self, cache: KeyValueCache):
        cache.rpush("tkey3", "a", "b")
        cache.sadd("tkey3", "x", "y")
        assert cache.smembers("tkey3") == {"x", "y"}
        assert cache.llen("tkey3") == 0
