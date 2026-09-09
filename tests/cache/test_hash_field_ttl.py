"""Tests for hash field expiration: the HEXPIRE family, HSETEX and HGETEX."""

import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def _version(raw: object) -> tuple[int, ...]:
    return tuple(int(part) for part in str(raw).split(".")[:2] if part.isdigit())


def field_ttl_support(cache: RespCache) -> tuple[bool, bool]:
    """Return ``(hexpire family supported, hsetex/hgetex supported)`` for the connected server.

    The cluster container runs Redis 7.0, so each test either skips or asserts
    the NotSupportedError gate based on INFO server.
    """
    info = cache.info("server")
    if "redis_version" not in info:
        info = next(iter(info.values()))
    valkey = info.get("valkey_version")
    if valkey:
        supported = _version(valkey) >= (9, 0)
        return supported, supported
    redis = _version(info.get("redis_version", "0"))
    return redis >= (7, 4), redis >= (8, 0)


def assert_seconds(actual: int | None, expected: int) -> None:
    """Assert a whole-second TTL, allowing the second a slow round trip can shave off."""
    assert actual in (expected - 1, expected)


@pytest.fixture
def field_ttl_cache(cache: RespCache) -> RespCache:
    if not field_ttl_support(cache)[0]:
        pytest.skip("server lacks HEXPIRE (Redis 7.4+ / Valkey 9.0+)")
    return cache


@pytest.fixture
def setex_cache(cache: RespCache) -> RespCache:
    if not field_ttl_support(cache)[1]:
        pytest.skip("server lacks HSETEX/HGETEX (Redis 8.0+ / Valkey 9.0+)")
    return cache


class TestHexpire:
    def test_sets_ttl_on_named_fields_only(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"a": 1, "b": 2})

        assert field_ttl_cache.hexpire("h", 60, "a") == [1]

        ttl_a, ttl_b = field_ttl_cache.httl("h", "a", "b")
        assert_seconds(ttl_a, 60)
        assert ttl_b is None
        assert field_ttl_cache.hget("h", "a") == 1

    def test_reports_a_code_per_field(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)

        assert field_ttl_cache.hexpire("h", 60, "a", "missing") == [1, -2]
        assert field_ttl_cache.hexpire("nokey", 60, "a") == [-2]

    def test_zero_timeout_deletes_the_field(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"a": 1, "b": 2})

        assert field_ttl_cache.hexpire("h", 0, "a") == [2]
        assert field_ttl_cache.hexists("h", "a") is False
        assert field_ttl_cache.hget("h", "b") == 2

    def test_accepts_a_timedelta(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)

        assert field_ttl_cache.hexpire("h", timedelta(minutes=2), "a") == [1]
        assert_seconds(field_ttl_cache.httl("h", "a")[0], 120)

    def test_conditions(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"with_ttl": 1, "no_ttl": 2})
        field_ttl_cache.hexpire("h", 100, "with_ttl")

        assert field_ttl_cache.hexpire("h", 50, "with_ttl", "no_ttl", nx=True) == [0, 1]
        field_ttl_cache.hpersist("h", "no_ttl")
        assert field_ttl_cache.hexpire("h", 50, "with_ttl", "no_ttl", xx=True) == [1, 0]
        assert field_ttl_cache.hexpire("h", 10, "with_ttl", gt=True) == [0]
        assert field_ttl_cache.hexpire("h", 10, "with_ttl", lt=True) == [1]
        assert_seconds(field_ttl_cache.httl("h", "with_ttl")[0], 10)

    def test_no_fields_is_a_no_op(self, field_ttl_cache: RespCache):
        assert field_ttl_cache.hexpire("h", 10) == []
        assert field_ttl_cache.httl("h") == []
        assert field_ttl_cache.hpersist("h") == []

    def test_respects_version(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1, version=2)

        assert field_ttl_cache.hexpire("h", 60, "a", version=2) == [1]
        assert_seconds(field_ttl_cache.httl("h", "a", version=2)[0], 60)
        assert field_ttl_cache.httl("h", "a") == [-2]

    def test_field_expires_and_hash_survives(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"short": 1, "long": 2})

        assert field_ttl_cache.hpexpire("h", 200, "short") == [1]
        time.sleep(0.5)

        assert field_ttl_cache.hget("h", "short") is None
        assert field_ttl_cache.hexists("h", "short") is False
        assert field_ttl_cache.hgetall("h") == {"long": 2}

    def test_hset_overwrite_drops_the_ttl(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)
        field_ttl_cache.hexpire("h", 60, "a")

        field_ttl_cache.hset("h", "a", 2)

        assert field_ttl_cache.httl("h", "a") == [None]


class TestHexpireat:
    def test_datetime_deadline(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)
        when = datetime.now(UTC) + timedelta(hours=1)

        assert field_ttl_cache.hexpireat("h", when, "a") == [1]
        assert field_ttl_cache.hexpiretime("h", "a") == [int(when.timestamp())]

    def test_unix_deadline(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)
        when = int(time.time()) + 3600

        assert field_ttl_cache.hexpireat("h", when, "a") == [1]
        assert field_ttl_cache.hexpiretime("h", "a") == [when]
        assert_seconds(field_ttl_cache.httl("h", "a")[0], 3600)

    def test_millisecond_deadline(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)
        when = datetime.now(UTC) + timedelta(hours=1)

        assert field_ttl_cache.hpexpireat("h", when, "a") == [1]
        # Redis rounds a sub-second deadline up in HEXPIRETIME, Valkey to nearest.
        assert field_ttl_cache.hexpiretime("h", "a")[0] in {int(when.timestamp()), int(when.timestamp()) + 1}
        assert field_ttl_cache.hpexpireat("h", int(when.timestamp() * 1000), "a") == [1]

    def test_past_deadline_deletes_the_field(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)

        assert field_ttl_cache.hexpireat("h", datetime.now(UTC) - timedelta(seconds=1), "a") == [2]
        assert field_ttl_cache.hexists("h", "a") is False


class TestHttl:
    def test_reports_none_for_no_expiry_and_minus_two_for_missing(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)

        assert field_ttl_cache.httl("h", "a", "missing") == [None, -2]
        assert field_ttl_cache.hpttl("h", "a", "missing") == [None, -2]
        assert field_ttl_cache.hexpiretime("h", "a", "missing") == [None, -2]
        assert field_ttl_cache.httl("nokey", "a") == [-2]

    def test_hpttl_reports_milliseconds(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", "a", 1)
        field_ttl_cache.hexpire("h", 60, "a")

        (pttl,) = field_ttl_cache.hpttl("h", "a")
        assert pttl is not None
        assert 59_000 < pttl <= 60_000


class TestHpersist:
    def test_codes_and_effect(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"a": 1, "b": 2})
        field_ttl_cache.hexpire("h", 60, "a")

        assert field_ttl_cache.hpersist("h", "a", "b", "missing") == [1, -1, -2]
        assert field_ttl_cache.httl("h", "a") == [None]


class TestHsetex:
    def test_sets_value_and_ttl_in_one_call(self, setex_cache: RespCache):
        assert setex_cache.hsetex("h", "a", {"nested": [1, 2]}, timeout=60) is True

        assert setex_cache.hget("h", "a") == {"nested": [1, 2]}
        assert_seconds(setex_cache.httl("h", "a")[0], 60)

    def test_mapping_and_items_forms(self, setex_cache: RespCache):
        assert setex_cache.hsetex("h", mapping={"a": 1, "b": "two"}, timeout=60) is True
        assert setex_cache.hsetex("h", items=["c", 3.5, "d", None], timeout=60) is True

        assert setex_cache.hgetall("h") == {"a": 1, "b": "two", "c": 3.5, "d": None}
        assert all(ttl is not None for ttl in setex_cache.httl("h", "a", "b", "c", "d"))

    def test_default_timeout_is_the_backend_default(self, setex_cache: RespCache):
        setex_cache.hsetex("h", "a", 1)

        assert_seconds(setex_cache.httl("h", "a")[0], setex_cache.default_timeout)

    def test_none_timeout_drops_the_ttl(self, setex_cache: RespCache):
        setex_cache.hsetex("h", "a", 1, timeout=60)

        setex_cache.hsetex("h", "a", 2, timeout=None)

        assert setex_cache.hget("h", "a") == 2
        assert setex_cache.httl("h", "a") == [None]

    def test_zero_timeout_expires_at_once(self, setex_cache: RespCache):
        setex_cache.hset("h", "keep", 1)

        assert setex_cache.hsetex("h", "a", 1, timeout=0) is True

        assert setex_cache.hexists("h", "a") is False
        assert setex_cache.hget("h", "keep") == 1

    def test_keepttl_keeps_the_existing_ttl(self, setex_cache: RespCache):
        setex_cache.hsetex("h", "a", 1, timeout=60)

        assert setex_cache.hsetex("h", "a", 2, timeout=5, keepttl=True) is True

        assert setex_cache.hget("h", "a") == 2
        assert_seconds(setex_cache.httl("h", "a")[0], 60)

    def test_fnx_and_fxx(self, setex_cache: RespCache):
        setex_cache.hset("h", "existing", 1)

        assert setex_cache.hsetex("h", mapping={"existing": 2, "new": 3}, timeout=60, fnx=True) is False
        assert setex_cache.hexists("h", "new") is False
        assert setex_cache.hsetex("h", "new", 3, timeout=60, fnx=True) is True

        assert setex_cache.hsetex("h", mapping={"existing": 5, "other": 6}, timeout=60, fxx=True) is False
        assert setex_cache.hget("h", "existing") == 1
        assert setex_cache.hsetex("h", "existing", 5, timeout=60, fxx=True) is True
        assert setex_cache.hget("h", "existing") == 5

    def test_requires_a_field(self, setex_cache: RespCache):
        with pytest.raises(ValueError, match="at least one field"):
            setex_cache.hsetex("h", timeout=60)
        with pytest.raises(ValueError, match="pairs"):
            setex_cache.hsetex("h", items=["a"], timeout=60)

    def test_respects_version(self, setex_cache: RespCache):
        setex_cache.hsetex("h", "a", 1, timeout=60, version=2)

        assert setex_cache.hget("h", "a", version=2) == 1
        assert setex_cache.hget("h", "a") is None


class TestHgetex:
    def test_reads_values_and_leaves_ttl_alone_by_default(self, setex_cache: RespCache):
        setex_cache.hsetex("h", mapping={"a": {"x": 1}, "b": "two"}, timeout=60)

        assert setex_cache.hgetex("h", "a", "b", "missing") == [{"x": 1}, "two", None]
        assert_seconds(setex_cache.httl("h", "a")[0], 60)
        assert setex_cache.hgetex("nokey", "a") == [None]
        assert setex_cache.hgetex("h") == []

    def test_timeout_sets_a_new_ttl(self, setex_cache: RespCache):
        setex_cache.hset("h", "a", 1)

        assert setex_cache.hgetex("h", "a", timeout=30) == [1]
        assert_seconds(setex_cache.httl("h", "a")[0], 30)

    def test_persist_removes_the_ttl(self, setex_cache: RespCache):
        setex_cache.hsetex("h", "a", 1, timeout=60)

        assert setex_cache.hgetex("h", "a", persist=True) == [1]
        assert setex_cache.httl("h", "a") == [None]

    def test_zero_timeout_reads_then_deletes(self, setex_cache: RespCache):
        setex_cache.hset("h", mapping={"a": 1, "b": 2})

        assert setex_cache.hgetex("h", "a", timeout=0) == [1]
        assert setex_cache.hexists("h", "a") is False
        assert setex_cache.hget("h", "b") == 2


class TestAsync:
    @pytest.mark.asyncio
    async def test_expire_family(self, field_ttl_cache: RespCache):
        await field_ttl_cache.ahset("h", mapping={"a": 1, "b": 2})

        assert await field_ttl_cache.ahexpire("h", 60, "a", "missing") == [1, -2]
        ttl_a, ttl_b = await field_ttl_cache.ahttl("h", "a", "b")
        assert_seconds(ttl_a, 60)
        assert ttl_b is None
        assert await field_ttl_cache.ahpexpire("h", timedelta(minutes=1), "b") == [1]
        (pttl,) = await field_ttl_cache.ahpttl("h", "b")
        assert pttl is not None
        assert 59_000 < pttl <= 60_000

        when = datetime.now(UTC) + timedelta(hours=1)
        assert await field_ttl_cache.ahexpireat("h", when, "a") == [1]
        assert await field_ttl_cache.ahpexpireat("h", when, "b") == [1]
        expire_a, expire_b = await field_ttl_cache.ahexpiretime("h", "a", "b")
        assert expire_a == int(when.timestamp())
        assert expire_b in {int(when.timestamp()), int(when.timestamp()) + 1}

        assert await field_ttl_cache.ahpersist("h", "a", "b", "missing") == [1, 1, -2]
        assert await field_ttl_cache.ahttl("h", "a", "b") == [None, None]
        assert await field_ttl_cache.ahexpire("h", 10) == []

    @pytest.mark.asyncio
    async def test_hsetex_and_hgetex(self, setex_cache: RespCache):
        assert await setex_cache.ahsetex("h", "a", {"x": 1}, timeout=60) is True
        assert await setex_cache.ahsetex("h", mapping={"b": 2}, timeout=None) is True
        assert await setex_cache.ahsetex("h", "a", 9, timeout=60, fnx=True) is False

        assert await setex_cache.ahgetex("h", "a", "b", "missing") == [{"x": 1}, 2, None]
        assert_seconds((await setex_cache.ahttl("h", "a"))[0], 60)
        assert await setex_cache.ahgetex("h", "a", persist=True) == [{"x": 1}]
        assert await setex_cache.ahgetex("h", "b", timeout=30) == [2]
        assert await setex_cache.ahttl("h", "a") == [None]
        assert_seconds((await setex_cache.ahttl("h", "b"))[0], 30)
        assert await setex_cache.ahgetex("h") == []


class TestPipeline:
    def test_expire_family(self, field_ttl_cache: RespCache):
        field_ttl_cache.hset("h", mapping={"a": 1, "b": 2})
        when = datetime.now(UTC) + timedelta(hours=1)

        pipe = field_ttl_cache.pipeline()
        pipe.hexpire("h", 60, "a", "missing")
        pipe.hpexpire("h", timedelta(minutes=1), "b")
        pipe.httl("h", "a", "b", "missing")
        pipe.hpttl("h", "a")
        pipe.hexpireat("h", when, "a")
        pipe.hpexpireat("h", when, "b")
        pipe.hexpiretime("h", "a", "missing")
        pipe.hpersist("h", "a", "b", "missing")
        pipe.httl("h", "a", "b")
        pipe.hexpire("h", 0, "b")
        results = pipe.execute()

        assert results[0] == [1, -2]
        assert results[1] == [1]
        assert_seconds(results[2][0], 60)
        assert_seconds(results[2][1], 60)
        assert results[2][2] == -2
        assert 59_000 < results[3][0] <= 60_000
        assert results[4] == [1]
        assert results[5] == [1]
        assert results[6] == [int(when.timestamp()), -2]
        assert results[7] == [1, 1, -2]
        assert results[8] == [None, None]
        assert results[9] == [2]
        assert field_ttl_cache.hgetall("h") == {"a": 1}

    def test_hsetex_and_hgetex(self, setex_cache: RespCache):
        pipe = setex_cache.pipeline()
        pipe.hsetex("h", "a", {"x": 1}, timeout=60)
        pipe.hsetex("h", mapping={"b": 2}, timeout=None)
        pipe.hsetex("h", "a", 9, timeout=60, fnx=True)
        pipe.hgetex("h", "a", "b", "missing")
        pipe.hgetex("h", "a", persist=True)
        pipe.hgetex("h", "b", timeout=30)
        pipe.httl("h", "a", "b")
        pipe.hsetex("h", "a", 3, timeout=60, keepttl=True)
        pipe.httl("h", "a")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is True
        assert results[2] is False
        assert results[3] == [{"x": 1}, 2, None]
        assert results[4] == [{"x": 1}]
        assert results[5] == [2]
        assert results[6][0] is None
        assert_seconds(results[6][1], 30)
        assert results[7] is True
        assert results[8] == [None]
        assert setex_cache.hget("h", "a") == 3

    @pytest.mark.asyncio
    async def test_async_pipeline(self, field_ttl_cache: RespCache):
        await field_ttl_cache.ahset("h", "a", 1)

        pipe = await field_ttl_cache.apipeline()
        pipe.hexpire("h", 60, "a")
        pipe.httl("h", "a", "missing")
        pipe.hpersist("h", "a")
        results = await pipe.execute()

        assert results[0] == [1]
        assert_seconds(results[1][0], 60)
        assert results[1][1] == -2
        assert results[2] == [1]


class TestUnsupportedServer:
    """Older servers answer with an unknown-command error; that must surface as NotSupportedError."""

    def test_hexpire_family_raises_not_supported(self, cache: RespCache):
        if field_ttl_support(cache)[0]:
            pytest.skip("server supports hash field expiration")
        cache.hset("h", "a", 1)

        with pytest.raises(NotSupportedError) as excinfo:
            cache.hexpire("h", 60, "a")
        assert excinfo.value.operation == "hexpire"
        assert "Redis 7.4+ or Valkey 9.0+" in str(excinfo.value)

        with pytest.raises(NotSupportedError):
            cache.httl("h", "a")
        with pytest.raises(NotSupportedError):
            cache.hpersist("h", "a")

    def test_hsetex_and_hgetex_raise_not_supported(self, cache: RespCache):
        if field_ttl_support(cache)[1]:
            pytest.skip("server supports HSETEX/HGETEX")

        with pytest.raises(NotSupportedError) as excinfo:
            cache.hsetex("h", "a", 1, timeout=60)
        assert excinfo.value.operation == "hsetex"
        assert "Redis 8.0+ or Valkey 9.0+" in str(excinfo.value)

        with pytest.raises(NotSupportedError):
            cache.hgetex("h", "a")

    def test_pipeline_raises_on_execute(self, cache: RespCache):
        if field_ttl_support(cache)[0]:
            pytest.skip("server supports hash field expiration")

        pipe = cache.pipeline()
        pipe.hset("h", "a", 1)
        pipe.hexpire("h", 60, "a")
        with pytest.raises(NotSupportedError):
            pipe.execute()

    @pytest.mark.asyncio
    async def test_async_raises_not_supported(self, cache: RespCache):
        if field_ttl_support(cache)[0]:
            pytest.skip("server supports hash field expiration")
        await cache.ahset("h", "a", 1)

        with pytest.raises(NotSupportedError) as excinfo:
            await cache.ahexpire("h", 60, "a")
        assert excinfo.value.operation == "hexpire"
