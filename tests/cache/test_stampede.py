"""Tests for cache stampede prevention via XFetch algorithm (TTL-based)."""

import logging
import random
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_cachex.exceptions import NotSupportedError, WrongTypeError
from django_cachex.stampede import StampedeConfig, make_stampede_config, should_recompute, should_recompute_remaining
from tests.cache.support import make_cache
from tests.fixtures.cache import server_version, skip_below_server

if TYPE_CHECKING:
    from django_cachex.cache import RespCache

# =============================================================================
# Unit tests for stampede module (no Redis needed)
# =============================================================================


def test_should_recompute_remaining_no_time_left_always_recomputes():
    config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
    assert should_recompute_remaining(0.0, config) is True
    assert should_recompute_remaining(-1.5, config) is True


def test_should_recompute_remaining_zero_delta_never_rolls():
    config = StampedeConfig(buffer=60, delta=0)
    assert not any(should_recompute_remaining(0.001, config) for _ in range(1000))


def test_should_recompute_remaining_ample_time_never_recomputes():
    config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
    assert not any(should_recompute_remaining(290.0, config) for _ in range(1000))


def test_fresh_value_no_recompute():
    config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
    # TTL 350 leaves 350 - 60 = 290s of logical lifetime.
    triggers = sum(1 for _ in range(1000) if should_recompute(350, config))
    assert triggers == 0


def test_expired_always_recomputes():
    config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
    # TTL 50 leaves 50 - 60 = -10s, so it always triggers.
    assert should_recompute(50, config) is True
    assert should_recompute(60, config) is True
    assert should_recompute(0, config) is True


def test_near_expiry_likely_triggers():
    """With very little logical time remaining and large delta, triggers often."""
    config = StampedeConfig(buffer=60, delta=10.0, beta=1.0)
    # TTL 61 leaves 1s against a delta of 10s, so it triggers most of the time.
    triggers = sum(1 for _ in range(100) if should_recompute(61, config))
    assert triggers > 50


def test_higher_beta_triggers_more():
    low_beta = StampedeConfig(buffer=60, delta=2.0, beta=0.5)
    high_beta = StampedeConfig(buffer=60, delta=2.0, beta=5.0)
    # TTL 65 leaves 5s.
    low = sum(1 for _ in range(1000) if should_recompute(65, low_beta))
    high = sum(1 for _ in range(1000) if should_recompute(65, high_beta))
    assert high > low


def test_zero_delta_never_triggers_early():
    """With delta=0, only triggers when logically expired."""
    config = StampedeConfig(buffer=60, delta=0.0, beta=1.0)
    # TTL 65 leaves 5s, but delta=0 switches the probabilistic trigger off.
    triggers = sum(1 for _ in range(1000) if should_recompute(65, config))
    assert triggers == 0
    # But when logically expired, still triggers
    assert should_recompute(60, config) is True


def test_zero_beta_and_delta_are_accepted():
    # Both spell "no probabilistic trigger", which should_recompute honors.
    assert StampedeConfig(beta=0, delta=0).delta == 0


# A float or string ``buffer`` reaches the driver's ``ex`` intact and turns every timed ``set`` into a ``DataError``.
@pytest.mark.parametrize("buffer", [60.0, "60", None, True])
def test_non_int_buffer_rejected(buffer):
    with pytest.raises(TypeError, match="buffer must be an int"):
        StampedeConfig(buffer=buffer)


def test_negative_buffer_rejected():
    with pytest.raises(ValueError, match="buffer must not be negative"):
        StampedeConfig(buffer=-10)


@pytest.mark.parametrize("field", ["beta", "delta"])
def test_non_numeric_beta_and_delta_rejected(field):
    with pytest.raises(TypeError, match=f"{field} must be a number"):
        StampedeConfig(**{field: "1.0"})


@pytest.mark.parametrize("field", ["beta", "delta"])
@pytest.mark.parametrize("value", [-1.0, float("inf"), float("nan")])
def test_out_of_range_beta_and_delta_rejected(field, value):
    with pytest.raises(ValueError, match=f"{field} must be a finite number"):
        StampedeConfig(**{field: value})


def test_options_dict_with_a_bad_value_fails_at_configuration_time():
    with pytest.raises(TypeError, match="buffer must be an int"):
        make_stampede_config({"buffer": 30.0})


def test_none_and_false_disable_prevention():
    assert make_stampede_config(None) is None
    assert make_stampede_config(False) is None


def test_true_uses_the_defaults():
    assert make_stampede_config(True) == StampedeConfig()


def test_empty_dict_disables_prevention():
    # An empty dict is falsy, so it reads as "off", not "defaults".
    assert make_stampede_config({}) is None


def test_dict_sets_every_field():
    config = make_stampede_config({"buffer": 30, "beta": 2.0, "delta": 0.5})
    assert config == StampedeConfig(buffer=30, beta=2.0, delta=0.5)


def test_partial_dict_keeps_the_defaults():
    assert make_stampede_config({"buffer": 15}) == StampedeConfig(buffer=15, beta=1.0, delta=1.0)


def test_unknown_keys_are_dropped_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="django_cachex.stampede"):
        config = make_stampede_config({"buffer": 30, "bufer": 99})
    assert config == StampedeConfig(buffer=30)
    assert "bufer" in caplog.text


def test_a_ready_config_is_used_as_is():
    config = StampedeConfig(buffer=5)
    assert make_stampede_config(config) is config


@pytest.mark.parametrize("option", ["False", "true", "", 1, 0, 60.0, [60], ("buffer", 60)])
def test_make_stampede_config_other_types_are_rejected(option):
    with pytest.raises(ImproperlyConfigured, match="stampede_prevention"):
        make_stampede_config(option)


def test_dict_option_becomes_the_adapter_policy():
    cache = make_cache(stampede_prevention={"buffer": 30, "beta": 2.0, "delta": 0.5})
    assert cache.adapter.resolve_stampede(None) == StampedeConfig(buffer=30, beta=2.0, delta=0.5)


def test_true_option_becomes_the_default_policy():
    assert make_cache(stampede_prevention=True).adapter.resolve_stampede(None) == StampedeConfig()


def test_absent_option_leaves_prevention_off():
    assert make_cache().adapter.resolve_stampede(None) is None


def test_string_option_is_rejected_when_the_adapter_is_built():
    with pytest.raises(ImproperlyConfigured, match="stampede_prevention"):
        make_cache(stampede_prevention="False").adapter  # noqa: B018


# =============================================================================
# Integration tests (require Redis)
# =============================================================================


def test_set_and_get(stampede_cache: RespCache):
    stampede_cache.set("sp_basic", "hello", timeout=300)
    assert stampede_cache.get("sp_basic") == "hello"


def test_get_missing_key(stampede_cache: RespCache):
    stampede_cache.delete("sp_missing")
    assert stampede_cache.get("sp_missing") is None


def test_get_with_default(stampede_cache: RespCache):
    stampede_cache.delete("sp_default")
    assert stampede_cache.get("sp_default", "fallback") == "fallback"


def test_delete(stampede_cache: RespCache):
    stampede_cache.set("sp_del", "val", timeout=300)
    assert stampede_cache.delete("sp_del") is True
    assert stampede_cache.get("sp_del") is None


def test_add_new_key(stampede_cache: RespCache):
    stampede_cache.delete("sp_add")
    assert stampede_cache.add("sp_add", "first", timeout=300) is True
    assert stampede_cache.get("sp_add") == "first"


def test_add_existing_key(stampede_cache: RespCache):
    stampede_cache.set("sp_add_exists", "original", timeout=300)
    assert stampede_cache.add("sp_add_exists", "new", timeout=300) is False
    assert stampede_cache.get("sp_add_exists") == "original"


def test_incr(stampede_cache: RespCache):
    stampede_cache.set("sp_incr", 10, timeout=300)
    result = stampede_cache.incr("sp_incr")
    assert result == 11


def test_decr(stampede_cache: RespCache):
    stampede_cache.set("sp_decr", 10, timeout=300)
    result = stampede_cache.decr("sp_decr")
    assert result == 9


def test_stored_ttl_includes_buffer(stampede_cache: RespCache):
    stampede_cache.set("sp_ttl", "val", timeout=300)
    # The raw stored TTL is between 300 and 360 (300 + 60 buffer).
    ttl = stampede_cache.ttl("sp_ttl", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


def test_reported_ttl_strips_buffer(stampede_cache: RespCache):
    """ttl() reports the timeout the caller passed, not the buffered one."""
    stampede_cache.set("sp_ttl_logical", "val", timeout=300)
    ttl = stampede_cache.ttl("sp_ttl_logical")
    assert ttl is not None
    assert 290 < ttl <= 300


def test_reported_pttl_strips_buffer(stampede_cache: RespCache):
    stampede_cache.set("sp_pttl_logical", "val", timeout=300)
    pttl = stampede_cache.pttl("sp_pttl_logical")
    assert pttl is not None
    assert 290_000 < pttl <= 300_000


def test_reported_expiretime_strips_buffer(stampede_cache: RespCache):
    skip_below_server(stampede_cache, redis=(7, 0), feature="EXPIRETIME")
    stampede_cache.set("sp_et_logical", "val", timeout=300)
    logical = stampede_cache.expiretime("sp_et_logical")
    raw = stampede_cache.expiretime("sp_et_logical", stampede_prevention=False)
    assert logical is not None
    assert raw is not None
    assert raw - logical == 60


def test_ttl_sentinels_pass_through(stampede_cache: RespCache):
    """-2 (missing key) must not have the buffer subtracted from it."""
    stampede_cache.delete("sp_ttl_missing")
    assert stampede_cache.ttl("sp_ttl_missing") == -2
    assert stampede_cache.pttl("sp_ttl_missing") == -2


def test_no_timeout_no_buffer(stampede_cache: RespCache):
    stampede_cache.set("sp_persist", "val", timeout=None)
    ttl = stampede_cache.ttl("sp_persist")
    assert ttl is None  # No expiry


# ``set(nx=/xx=/get=)`` goes through ``set_with_flags``, which must buffer the TTL like plain ``set()``.
@pytest.mark.parametrize("flags", [{"nx": True}, {"xx": True}, {"get": True}], ids=["nx", "xx", "get"])
def test_set_flags_stored_ttl_includes_buffer(stampede_cache: RespCache, flags: dict[str, bool]):
    if not flags.get("nx"):
        stampede_cache.set("sp_flags", "old", timeout=300)
    stampede_cache.set("sp_flags", "val", timeout=300, **flags)

    raw = stampede_cache.ttl("sp_flags", stampede_prevention=False)
    assert raw is not None
    assert 300 < raw <= 360
    assert stampede_cache.get("sp_flags") == "val"


def test_get_flag_returns_the_old_value(stampede_cache: RespCache):
    stampede_cache.set("sp_flags_get", "old", timeout=300)
    assert stampede_cache.set("sp_flags_get", "new", timeout=300, get=True) == "old"


@pytest.mark.asyncio
@pytest.mark.parametrize("flags", [{"nx": True}, {"xx": True}, {"get": True}], ids=["nx", "xx", "get"])
async def test_aset_flags_stored_ttl_includes_buffer(stampede_cache: RespCache, flags: dict[str, bool]):
    if not flags.get("nx"):
        await stampede_cache.aset("asp_flags", "old", timeout=300)
    await stampede_cache.aset("asp_flags", "val", timeout=300, **flags)

    raw = await stampede_cache.attl("asp_flags", stampede_prevention=False)
    assert raw is not None
    assert 300 < raw <= 360
    assert await stampede_cache.aget("asp_flags") == "val"


# Regression: ``touch()`` stored the raw timeout, so a touched key lost its buffer and was eligible for recompute.
def test_touch_reapplies_buffer(stampede_cache: RespCache):
    stampede_cache.set("sp_touch", "val", timeout=300)
    stampede_cache.expire("sp_touch", 50, stampede_prevention=False)

    assert stampede_cache.touch("sp_touch", timeout=300) is True
    ttl = stampede_cache.ttl("sp_touch", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360  # 300 + 60 buffer, same as set()
    assert stampede_cache.get("sp_touch") == "val"


def test_touch_short_timeout_stays_logically_alive(stampede_cache: RespCache):
    stampede_cache.set("sp_touch_short", "val", timeout=300)

    # timeout=50 is below the 60s buffer; without the buffer the key
    # would be logically expired the moment it was touched.
    assert stampede_cache.touch("sp_touch_short", timeout=50) is True
    ttl = stampede_cache.ttl("sp_touch_short", stampede_prevention=False)
    assert ttl is not None
    assert 50 < ttl <= 110
    assert stampede_cache.get("sp_touch_short") == "val"


def test_touch_none_makes_persistent(stampede_cache: RespCache):
    stampede_cache.set("sp_touch_none", "val", timeout=300)
    assert stampede_cache.touch("sp_touch_none", timeout=None) is True
    assert stampede_cache.ttl("sp_touch_none") is None


@pytest.mark.asyncio
async def test_atouch_reapplies_buffer(stampede_cache: RespCache):
    await stampede_cache.aset("asp_touch", "val", timeout=300)
    await stampede_cache.aexpire("asp_touch", 50, stampede_prevention=False)

    assert await stampede_cache.atouch("asp_touch", timeout=300) is True
    ttl = await stampede_cache.attl("asp_touch", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360
    assert await stampede_cache.aget("asp_touch") == "val"


# Regression: ``expire(k, 30)`` passed the raw timeout, below the 60s buffer, so every later ``get()`` returned None.
def test_expire_keeps_value_readable(stampede_cache: RespCache):
    stampede_cache.set("sp_exp_read", "val", timeout=300)
    assert stampede_cache.expire("sp_exp_read", 30) is True
    assert stampede_cache.get("sp_exp_read") == "val"
    assert stampede_cache.get("sp_exp_read") == "val"


def test_expire_reports_the_timeout_it_was_given(stampede_cache: RespCache):
    stampede_cache.set("sp_exp_ttl", "val", timeout=300)
    stampede_cache.expire("sp_exp_ttl", 120)
    assert stampede_cache.ttl("sp_exp_ttl") == pytest.approx(120, abs=2)
    assert stampede_cache.ttl("sp_exp_ttl", stampede_prevention=False) == pytest.approx(180, abs=2)


def test_expire_timedelta_gets_the_buffer(stampede_cache: RespCache):
    stampede_cache.set("sp_exp_td", "val", timeout=300)
    stampede_cache.expire("sp_exp_td", timedelta(seconds=120))
    assert stampede_cache.get("sp_exp_td") == "val"
    assert stampede_cache.ttl("sp_exp_td", stampede_prevention=False) == pytest.approx(180, abs=2)


def test_expire_non_positive_timeout_still_deletes(stampede_cache: RespCache):
    """The buffer must not resurrect a key the caller asked to drop."""
    stampede_cache.set("sp_exp_zero", "val", timeout=300)
    assert stampede_cache.expire("sp_exp_zero", 0) is True
    assert stampede_cache.has_key("sp_exp_zero") is False


def test_pexpire_keeps_value_readable(stampede_cache: RespCache):
    stampede_cache.set("sp_pexp", "val", timeout=300)
    assert stampede_cache.pexpire("sp_pexp", 30_000) is True
    assert stampede_cache.get("sp_pexp") == "val"
    assert stampede_cache.pttl("sp_pexp") == pytest.approx(30_000, abs=2000)


def test_expireat_keeps_value_readable(stampede_cache: RespCache):
    skip_below_server(stampede_cache, redis=(7, 0), feature="EXPIRETIME")
    stampede_cache.set("sp_expat", "val", timeout=300)
    when = int(time.time()) + 30
    assert stampede_cache.expireat("sp_expat", when) is True
    assert stampede_cache.get("sp_expat") == "val"
    assert stampede_cache.expiretime("sp_expat") == pytest.approx(when, abs=2)


def test_expireat_datetime_keeps_value_readable(stampede_cache: RespCache):
    stampede_cache.set("sp_expat_dt", "val", timeout=300)
    when = datetime.now(tz=UTC) + timedelta(seconds=30)
    assert stampede_cache.expireat("sp_expat_dt", when) is True
    assert stampede_cache.get("sp_expat_dt") == "val"


def test_pexpireat_keeps_value_readable(stampede_cache: RespCache):
    stampede_cache.set("sp_pexpat", "val", timeout=300)
    assert stampede_cache.pexpireat("sp_pexpat", int(time.time() * 1000) + 30_000) is True
    assert stampede_cache.get("sp_pexpat") == "val"


def test_expireat_in_the_past_still_deletes(stampede_cache: RespCache):
    stampede_cache.set("sp_expat_past", "val", timeout=300)
    assert stampede_cache.expireat("sp_expat_past", int(time.time()) - 30) is True
    assert stampede_cache.has_key("sp_expat_past") is False


def test_expireat_datetime_in_the_past_still_deletes(stampede_cache: RespCache):
    stampede_cache.set("sp_expat_past_dt", "val", timeout=300)
    when = datetime.now(tz=UTC) - timedelta(seconds=30)
    assert stampede_cache.expireat("sp_expat_past_dt", when) is True
    assert stampede_cache.has_key("sp_expat_past_dt") is False


def test_pexpireat_in_the_past_still_deletes(stampede_cache: RespCache):
    stampede_cache.set("sp_pexpat_past", "val", timeout=300)
    assert stampede_cache.pexpireat("sp_pexpat_past", int(time.time() * 1000) - 30_000) is True
    assert stampede_cache.has_key("sp_pexpat_past") is False


@pytest.mark.asyncio
async def test_aexpire_keeps_value_readable(stampede_cache: RespCache):
    await stampede_cache.aset("asp_exp_read", "val", timeout=300)
    assert await stampede_cache.aexpire("asp_exp_read", 30) is True
    assert await stampede_cache.aget("asp_exp_read") == "val"
    assert await stampede_cache.attl("asp_exp_read") == pytest.approx(30, abs=2)


@pytest.mark.asyncio
async def test_apexpire_and_apexpireat_keep_value_readable(stampede_cache: RespCache):
    await stampede_cache.aset("asp_pexp", "val", timeout=300)
    assert await stampede_cache.apexpire("asp_pexp", 30_000) is True
    assert await stampede_cache.aget("asp_pexp") == "val"

    await stampede_cache.aset("asp_pexpat", "val", timeout=300)
    assert await stampede_cache.apexpireat("asp_pexpat", int(time.time() * 1000) + 30_000) is True
    assert await stampede_cache.aget("asp_pexpat") == "val"


@pytest.mark.asyncio
async def test_aexpireat_keeps_value_readable(stampede_cache: RespCache):
    skip_below_server(stampede_cache, redis=(7, 0), feature="EXPIRETIME")
    await stampede_cache.aset("asp_expat", "val", timeout=300)
    when = int(time.time()) + 30
    assert await stampede_cache.aexpireat("asp_expat", when) is True
    assert await stampede_cache.aget("asp_expat") == "val"
    assert await stampede_cache.aexpiretime("asp_expat") == pytest.approx(when, abs=2)


def test_expire_and_ttl_are_untouched_without_stampede(cache: RespCache):
    cache.set("nosp_exp", "val", timeout=300)
    assert cache.expire("nosp_exp", 120) is True
    assert cache.ttl("nosp_exp") == pytest.approx(120, abs=2)
    assert cache.pttl("nosp_exp") == pytest.approx(120_000, abs=2000)


def test_expireat_and_expiretime_are_untouched_without_stampede(cache: RespCache):
    skip_below_server(cache, redis=(7, 0), feature="EXPIRETIME")
    cache.set("nosp_expat", "val", timeout=300)
    when = int(time.time()) + 120
    assert cache.expireat("nosp_expat", when) is True
    assert cache.expiretime("nosp_expat") == when


def test_get_many(stampede_cache: RespCache):
    stampede_cache.set("sp_m1", "v1", timeout=300)
    stampede_cache.set("sp_m2", "v2", timeout=300)
    stampede_cache.delete("sp_m3")

    result = stampede_cache.get_many(["sp_m1", "sp_m2", "sp_m3"])
    assert "v1" in result.values()
    assert "v2" in result.values()
    assert len(result) == 2


def test_get_many_filters_expired(stampede_cache: RespCache):
    stampede_cache.set("sp_gm_exp", "val", timeout=300)
    stampede_cache.expire("sp_gm_exp", 50, stampede_prevention=False)

    result = stampede_cache.get_many(["sp_gm_exp"])
    assert len(result) == 0


def test_set_many(stampede_cache: RespCache):
    stampede_cache.set_many({"sp_sm1": "a", "sp_sm2": "b"}, timeout=300)
    assert stampede_cache.get("sp_sm1") == "a"
    assert stampede_cache.get("sp_sm2") == "b"


def test_set_many_ttl_includes_buffer(stampede_cache: RespCache):
    stampede_cache.set_many({"sp_sm_ttl": "val"}, timeout=300)
    ttl = stampede_cache.ttl("sp_sm_ttl", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


# ``stampede_prevention=False`` keeps ``expire()`` from adding the buffer, so a raw TTL of 50 reads as expired.
def test_returns_none_after_logical_expiry(stampede_cache: RespCache):
    """After logical expiry, get() returns None even though key is still in Redis."""
    stampede_cache.set("sp_expire", "val", timeout=300)
    assert stampede_cache.get("sp_expire") == "val"

    stampede_cache.expire("sp_expire", 50, stampede_prevention=False)
    assert stampede_cache.get("sp_expire") is None


def test_recompute_stores_with_buffer(stampede_cache: RespCache):
    """After recomputation, the new value should have buffered TTL."""
    stampede_cache.set("sp_recomp", "initial", timeout=300)
    stampede_cache.expire("sp_recomp", 50, stampede_prevention=False)

    # Recompute
    stampede_cache.set("sp_recomp", "recomputed", timeout=300)

    assert stampede_cache.get("sp_recomp") == "recomputed"
    ttl = stampede_cache.ttl("sp_recomp", stampede_prevention=False)
    assert ttl is not None
    assert ttl > 300


def test_get_and_add_agree_on_a_key_in_its_last_half_second(stampede_cache: RespCache):
    stampede_cache.set("sp_last_ms", "stale", timeout=300)
    stampede_cache.pexpire("sp_last_ms", 400, stampede_prevention=False)

    assert stampede_cache.get("sp_last_ms") is None
    assert stampede_cache.get_many(["sp_last_ms"]) == {}
    assert stampede_cache.add("sp_last_ms", "fresh", timeout=300) is True
    assert stampede_cache.get("sp_last_ms") == "fresh"


@pytest.mark.asyncio
async def test_aget_and_aadd_agree_on_a_key_in_its_last_half_second(stampede_cache: RespCache):
    await stampede_cache.aset("asp_last_ms", "stale", timeout=300)
    await stampede_cache.apexpire("asp_last_ms", 400, stampede_prevention=False)

    assert await stampede_cache.aget("asp_last_ms") is None
    assert await stampede_cache.aget_many(["asp_last_ms"]) == {}
    assert await stampede_cache.aadd("asp_last_ms", "fresh", timeout=300) is True
    assert await stampede_cache.aget("asp_last_ms") == "fresh"


def test_pipeline_set_get(stampede_cache: RespCache):
    with stampede_cache.pipeline() as pipe:
        pipe.set("sp_pipe1", "value1", timeout=300)
        pipe.set("sp_pipe2", "value2", timeout=300)
        pipe.get("sp_pipe1")
        pipe.get("sp_pipe2")
        results = pipe.execute()

    # SET results (True for success), then GET results
    assert results[2] == "value1"
    assert results[3] == "value2"


def test_pipeline_serves_stale_data(stampede_cache: RespCache):
    """Pipeline should serve stale data (not return None) during buffer window."""
    stampede_cache.set("sp_pipe_stale", "stale_val", timeout=300)
    stampede_cache.expire("sp_pipe_stale", 50, stampede_prevention=False)

    with stampede_cache.pipeline() as pipe:
        pipe.get("sp_pipe_stale")
        results = pipe.execute()

    assert results[0] == "stale_val"


def test_false_skips_ttl_check(stampede_cache: RespCache):
    """stampede_prevention=False on get() should return value even if logically expired."""
    stampede_cache.set("sp_ovr_get", "val", timeout=300)
    stampede_cache.expire("sp_ovr_get", 50, stampede_prevention=False)  # logically expired

    # Default behavior: returns None (logically expired)
    assert stampede_cache.get("sp_ovr_get") is None
    assert stampede_cache.get("sp_ovr_get", stampede_prevention=False) == "val"


def test_false_skips_buffer_on_set(stampede_cache: RespCache):
    """stampede_prevention=False on set() should not add buffer to TTL."""
    stampede_cache.set("sp_ovr_set", "val", timeout=300, stampede_prevention=False)
    ttl = stampede_cache.ttl("sp_ovr_set", stampede_prevention=False)
    assert ttl is not None
    assert 290 < ttl <= 300  # No buffer added


def test_false_skips_buffer_on_touch(stampede_cache: RespCache):
    """stampede_prevention=False on touch() should not add buffer to TTL."""
    stampede_cache.set("sp_ovr_touch", "val", timeout=300)
    assert stampede_cache.touch("sp_ovr_touch", timeout=200, stampede_prevention=False) is True
    ttl = stampede_cache.ttl("sp_ovr_touch", stampede_prevention=False)
    assert ttl is not None
    assert 190 < ttl <= 200  # No buffer added


def test_false_skips_buffer_on_expire(stampede_cache: RespCache):
    """stampede_prevention=False on expire() should set the raw TTL."""
    stampede_cache.set("sp_ovr_expire", "val", timeout=300)
    assert stampede_cache.expire("sp_ovr_expire", 200, stampede_prevention=False) is True
    ttl = stampede_cache.ttl("sp_ovr_expire", stampede_prevention=False)
    assert ttl is not None
    assert 190 < ttl <= 200


def test_false_on_get_many(stampede_cache: RespCache):
    """stampede_prevention=False on get_many() should return logically expired values."""
    stampede_cache.set("sp_ovr_gm", "val", timeout=300)
    stampede_cache.expire("sp_ovr_gm", 50, stampede_prevention=False)  # logically expired

    # Default behavior: filtered out (logically expired)
    assert len(stampede_cache.get_many(["sp_ovr_gm"])) == 0
    # With stampede_prevention=False, value is returned despite logical expiry
    result = stampede_cache.get_many(["sp_ovr_gm"], stampede_prevention=False)
    assert result.get("sp_ovr_gm") == "val"


def test_true_on_non_stampede_cache(cache: RespCache):
    """stampede_prevention=True on a cache without global stampede should still add buffer."""
    cache.set("sp_ovr_force", "val", timeout=300, stampede_prevention=True)
    ttl = cache.ttl("sp_ovr_force")
    assert ttl is not None
    assert ttl > 300  # Buffer was added


def test_false_skips_buffer_on_add(stampede_cache: RespCache):
    stampede_cache.delete("sp_ovr_add")
    assert stampede_cache.add("sp_ovr_add", "val", timeout=300, stampede_prevention=False) is True
    ttl = stampede_cache.ttl("sp_ovr_add", stampede_prevention=False)
    assert ttl is not None
    assert 290 < ttl <= 300  # No buffer added


def test_true_on_add_adds_buffer_without_instance_config(cache: RespCache):
    cache.delete("sp_ovr_add_force")
    assert cache.add("sp_ovr_add_force", "val", timeout=300, stampede_prevention=True) is True
    ttl = cache.ttl("sp_ovr_add_force")
    assert ttl is not None
    assert ttl > 300  # Buffer was added


def test_false_skips_buffer_on_set_many(stampede_cache: RespCache):
    stampede_cache.set_many({"sp_ovr_sm": "val"}, timeout=300, stampede_prevention=False)
    ttl = stampede_cache.ttl("sp_ovr_sm", stampede_prevention=False)
    assert ttl is not None
    assert 290 < ttl <= 300  # No buffer added


def test_true_on_set_many_adds_buffer_without_instance_config(cache: RespCache):
    cache.set_many({"sp_ovr_sm_force": "val"}, timeout=300, stampede_prevention=True)
    ttl = cache.ttl("sp_ovr_sm_force")
    assert ttl is not None
    assert ttl > 300  # Buffer was added


def test_false_on_get_or_set_serves_the_logically_expired_value(stampede_cache: RespCache):
    stampede_cache.set("sp_ovr_gos", "stale", timeout=300)
    stampede_cache.expire("sp_ovr_gos", 50, stampede_prevention=False)  # logically expired

    result = stampede_cache.get_or_set("sp_ovr_gos", lambda: "fresh", timeout=300, stampede_prevention=False)
    assert result == "stale"


def test_config_override_buffer(cache: RespCache):
    """``stampede_prevention=StampedeConfig(...)`` should force the supplied policy."""
    # Non-stampede cache with per-call override: buffer=120
    cache.set("sp_ovr_cfg", "val", timeout=300, stampede_prevention=StampedeConfig(buffer=120))
    ttl = cache.ttl("sp_ovr_cfg")
    assert ttl is not None
    assert 300 < ttl <= 420  # 300 + 120 buffer


def test_config_override_replaces_instance(stampede_cache: RespCache):
    """``StampedeConfig`` override replaces the instance config wholesale."""
    # Instance has buffer=60; explicit override supplies the full policy.
    stampede_cache.set(
        "sp_ovr_replace",
        "val",
        timeout=300,
        stampede_prevention=StampedeConfig(buffer=90, delta=5.0),
    )
    ttl = stampede_cache.ttl("sp_ovr_replace", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 390  # 300 + 90 buffer from override


# =============================================================================
# Edge-case tests for the XFetch algorithm
# =============================================================================


@pytest.mark.parametrize(("value", "recomputes"), [(0.0, False), (1.0 - 2**-53, True)], ids=["lowest", "highest"])
def test_extreme_random_values_do_not_crash(mocker, value, recomputes):
    rng = random.Random()
    mocker.patch.object(rng, "random", return_value=value)
    mocker.patch("django_cachex.stampede.random", rng)
    assert should_recompute(65, StampedeConfig(buffer=60, delta=1.0, beta=1.0)) is recomputes


def test_expovariate_edge_via_mock(mocker):
    config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)

    # A large expovariate value makes the threshold very negative, so it always triggers.
    mocker.patch("django_cachex.stampede.random.expovariate", return_value=1000.0)
    assert should_recompute(65, config) is True

    # A tiny expovariate value keeps the threshold near 0, so a fresh key never triggers.
    mocker.patch("django_cachex.stampede.random.expovariate", return_value=0.001)
    assert should_recompute(350, config) is False


def test_get_or_set_overwrites_stale_key(stampede_cache: RespCache):
    """get_or_set must use set() (not add/NX) when stampede triggers, so
    the recomputed value actually replaces the stale one."""
    stampede_cache.set("sp_gos_overwrite", "stale", timeout=300)
    stampede_cache.expire("sp_gos_overwrite", 50, stampede_prevention=False)

    result = stampede_cache.get_or_set(
        "sp_gos_overwrite",
        lambda: "fresh",
        timeout=300,
    )
    assert result == "fresh"
    assert stampede_cache.get("sp_gos_overwrite") == "fresh"


def test_get_many_filters_logically_expired_consistently(stampede_cache: RespCache):
    stampede_cache.set("sp_gmc_str", "hello", timeout=300)
    stampede_cache.set("sp_gmc_int", 42, timeout=300)
    stampede_cache.expire("sp_gmc_str", 50, stampede_prevention=False)
    stampede_cache.expire("sp_gmc_int", 50, stampede_prevention=False)

    # Both get() and get_many() should treat them as logically expired
    assert stampede_cache.get("sp_gmc_str") is None
    assert stampede_cache.get("sp_gmc_int") is None
    result = stampede_cache.get_many(["sp_gmc_str", "sp_gmc_int"])
    assert len(result) == 0


def test_get_many_preserves_fresh_values(stampede_cache: RespCache):
    stampede_cache.set("sp_gmc_fresh1", "val", timeout=300)
    stampede_cache.set("sp_gmc_fresh2", 99, timeout=300)

    result = stampede_cache.get_many(["sp_gmc_fresh1", "sp_gmc_fresh2"])
    assert result["sp_gmc_fresh1"] == "val"
    assert result["sp_gmc_fresh2"] == 99


@pytest.mark.asyncio
async def test_aset_and_aget(stampede_cache: RespCache):
    await stampede_cache.aset("asp_basic", "hello", timeout=300)
    assert await stampede_cache.aget("asp_basic") == "hello"


@pytest.mark.asyncio
async def test_aget_missing_key(stampede_cache: RespCache):
    await stampede_cache.adelete("asp_missing")
    assert await stampede_cache.aget("asp_missing") is None


@pytest.mark.asyncio
async def test_adelete(stampede_cache: RespCache):
    await stampede_cache.aset("asp_del", "val", timeout=300)
    assert await stampede_cache.adelete("asp_del") is True
    assert await stampede_cache.aget("asp_del") is None


@pytest.mark.asyncio
async def test_aget_many(stampede_cache: RespCache):
    await stampede_cache.aset("asp_m1", "v1", timeout=300)
    await stampede_cache.aset("asp_m2", "v2", timeout=300)
    await stampede_cache.adelete("asp_m3")

    result = await stampede_cache.aget_many(["asp_m1", "asp_m2", "asp_m3"])
    assert "v1" in result.values()
    assert "v2" in result.values()
    assert len(result) == 2


@pytest.mark.asyncio
async def test_aget_many_filters_expired(stampede_cache: RespCache):
    await stampede_cache.aset("asp_gm_exp", "val", timeout=300)
    await stampede_cache.aexpire("asp_gm_exp", 50, stampede_prevention=False)

    result = await stampede_cache.aget_many(["asp_gm_exp"])
    assert len(result) == 0


@pytest.mark.asyncio
async def test_aset_many(stampede_cache: RespCache):
    await stampede_cache.aset_many({"asp_sm1": "a", "asp_sm2": "b"}, timeout=300)
    assert await stampede_cache.aget("asp_sm1") == "a"
    assert await stampede_cache.aget("asp_sm2") == "b"


@pytest.mark.asyncio
async def test_areturns_none_after_logical_expiry(stampede_cache: RespCache):
    await stampede_cache.aset("asp_expire", "val", timeout=300)
    assert await stampede_cache.aget("asp_expire") == "val"

    await stampede_cache.aexpire("asp_expire", 50, stampede_prevention=False)
    assert await stampede_cache.aget("asp_expire") is None


@pytest.mark.asyncio
async def test_aget_or_set_overwrites_stale_key(stampede_cache: RespCache):
    await stampede_cache.aset("asp_gos_overwrite", "stale", timeout=300)
    await stampede_cache.aexpire("asp_gos_overwrite", 50, stampede_prevention=False)

    result = await stampede_cache.aget_or_set(
        "asp_gos_overwrite",
        lambda: "fresh_async",
        timeout=300,
    )
    assert result == "fresh_async"
    assert await stampede_cache.aget("asp_gos_overwrite") == "fresh_async"


def test_get_or_set_false_skips_the_buffer_on_a_miss(stampede_cache: RespCache):
    stampede_cache.delete("sp_gos_off")
    assert stampede_cache.get_or_set("sp_gos_off", "val", timeout=300, stampede_prevention=False) == "val"
    ttl = stampede_cache.ttl("sp_gos_off", stampede_prevention=False)
    assert ttl is not None
    assert 290 < ttl <= 300


@pytest.mark.asyncio
async def test_aget_or_set_false_skips_the_buffer_on_a_miss(stampede_cache: RespCache):
    await stampede_cache.adelete("asp_gos_off")
    assert await stampede_cache.aget_or_set("asp_gos_off", "val", timeout=300, stampede_prevention=False) == "val"
    ttl = await stampede_cache.attl("asp_gos_off", stampede_prevention=False)
    assert ttl is not None
    assert 290 < ttl <= 300


def test_add_overwrites_a_logically_expired_key(stampede_cache: RespCache):
    stampede_cache.set("sp_add_stale", "stale", timeout=300)
    stampede_cache.expire("sp_add_stale", 50, stampede_prevention=False)

    assert stampede_cache.add("sp_add_stale", "fresh", timeout=300) is True
    assert stampede_cache.get("sp_add_stale") == "fresh"
    ttl = stampede_cache.ttl("sp_add_stale", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


def test_add_missing_key_stores_the_buffered_ttl(stampede_cache: RespCache):
    stampede_cache.delete("sp_add_missing")
    assert stampede_cache.add("sp_add_missing", "val", timeout=300) is True
    ttl = stampede_cache.ttl("sp_add_missing", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


def test_add_keeps_a_persistent_key(stampede_cache: RespCache):
    stampede_cache.set("sp_add_forever", "original", timeout=None)
    assert stampede_cache.add("sp_add_forever", "new", timeout=300) is False
    assert stampede_cache.get("sp_add_forever") == "original"


def test_add_timeout_none_replaces_a_logically_expired_key_with_a_persistent_one(stampede_cache: RespCache):
    stampede_cache.set("sp_add_none", "stale", timeout=300)
    stampede_cache.expire("sp_add_none", 50, stampede_prevention=False)

    assert stampede_cache.add("sp_add_none", "fresh", timeout=None) is True
    assert stampede_cache.get("sp_add_none") == "fresh"
    assert stampede_cache.ttl("sp_add_none", stampede_prevention=False) is None


def test_add_timeout_zero_drops_a_logically_expired_key(stampede_cache: RespCache):
    stampede_cache.set("sp_add_zero", "stale", timeout=300)
    stampede_cache.expire("sp_add_zero", 50, stampede_prevention=False)

    assert stampede_cache.add("sp_add_zero", "fresh", timeout=0) is True
    assert stampede_cache.keys("sp_add_zero") == []


def test_add_timeout_zero_keeps_a_fresh_key(stampede_cache: RespCache):
    stampede_cache.set("sp_add_zero_fresh", "original", timeout=300)
    assert stampede_cache.add("sp_add_zero_fresh", "new", timeout=0) is False
    assert stampede_cache.get("sp_add_zero_fresh") == "original"


def test_add_without_stampede_keeps_a_short_ttl_key(cache: RespCache):
    cache.set("sp_add_plain", "original", timeout=300)
    cache.expire("sp_add_plain", 50)

    assert cache.add("sp_add_plain", "new", timeout=300) is False
    assert cache.get("sp_add_plain") == "original"


@pytest.mark.asyncio
async def test_aadd_overwrites_a_logically_expired_key(stampede_cache: RespCache):
    await stampede_cache.aset("asp_add_stale", "stale", timeout=300)
    await stampede_cache.aexpire("asp_add_stale", 50, stampede_prevention=False)

    assert await stampede_cache.aadd("asp_add_stale", "fresh", timeout=300) is True
    assert await stampede_cache.aget("asp_add_stale") == "fresh"
    ttl = await stampede_cache.attl("asp_add_stale", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


@pytest.mark.asyncio
async def test_aadd_keeps_fresh_and_persistent_keys(stampede_cache: RespCache):
    await stampede_cache.aset("asp_add_fresh", "original", timeout=300)
    await stampede_cache.aset("asp_add_forever", "original", timeout=None)

    assert await stampede_cache.aadd("asp_add_fresh", "new", timeout=300) is False
    assert await stampede_cache.aadd("asp_add_forever", "new", timeout=300) is False
    assert await stampede_cache.aget("asp_add_fresh") == "original"
    assert await stampede_cache.aget("asp_add_forever") == "original"


def test_set_nx_refills_a_logically_expired_key(stampede_cache: RespCache):
    stampede_cache.set("sp_nx_stale", "stale", timeout=300)
    stampede_cache.expire("sp_nx_stale", 50, stampede_prevention=False)
    assert stampede_cache.get("sp_nx_stale") is None

    assert stampede_cache.set("sp_nx_stale", "fresh", timeout=300, nx=True) is True
    assert stampede_cache.set("sp_nx_stale", "late", timeout=300, nx=True) is False
    assert stampede_cache.get("sp_nx_stale") == "fresh"
    ttl = stampede_cache.ttl("sp_nx_stale", stampede_prevention=False)
    assert ttl is not None
    assert 300 < ttl <= 360


@pytest.mark.asyncio
async def test_aset_nx_refills_a_logically_expired_key(stampede_cache: RespCache):
    await stampede_cache.aset("asp_nx_stale", "stale", timeout=300)
    await stampede_cache.aexpire("asp_nx_stale", 50, stampede_prevention=False)

    assert await stampede_cache.aset("asp_nx_stale", "fresh", timeout=300, nx=True) is True
    assert await stampede_cache.aset("asp_nx_stale", "late", timeout=300, nx=True) is False
    assert await stampede_cache.aget("asp_nx_stale") == "fresh"


_FLAGS_ON_A_LOGICALLY_EXPIRED_KEY = pytest.mark.parametrize(
    ("flags", "reply", "stored", "raw_ttl"),
    [
        ({"nx": True, "get": True}, None, "fresh", 360),
        ({"xx": True}, False, "stale", 50),
        ({"xx": True, "get": True}, None, "stale", 50),
        ({"get": True}, None, "fresh", 360),
    ],
    ids=["nx-get", "xx", "xx-get", "get"],
)


@_FLAGS_ON_A_LOGICALLY_EXPIRED_KEY
def test_flagged_set_counts_a_logically_expired_key_as_absent(
    stampede_cache: RespCache,
    flags: dict[str, bool],
    reply: bool | None,
    stored: str,
    raw_ttl: int,
):
    if flags.get("nx"):
        skip_below_server(stampede_cache, redis=(7, 0), feature="SET NX GET")
    stampede_cache.set("sp_flags_stale", "stale", timeout=300)
    stampede_cache.expire("sp_flags_stale", 50, stampede_prevention=False)

    assert stampede_cache.set("sp_flags_stale", "fresh", timeout=300, **flags) is reply
    assert stampede_cache.get("sp_flags_stale", stampede_prevention=False) == stored
    ttl = stampede_cache.ttl("sp_flags_stale", stampede_prevention=False)
    assert ttl is not None
    assert raw_ttl - 10 < ttl <= raw_ttl


@pytest.mark.asyncio
@_FLAGS_ON_A_LOGICALLY_EXPIRED_KEY
async def test_flagged_aset_counts_a_logically_expired_key_as_absent(
    stampede_cache: RespCache,
    flags: dict[str, bool],
    reply: bool | None,
    stored: str,
    raw_ttl: int,
):
    if flags.get("nx"):
        skip_below_server(stampede_cache, redis=(7, 0), feature="SET NX GET")
    await stampede_cache.aset("asp_flags_stale", "stale", timeout=300)
    await stampede_cache.aexpire("asp_flags_stale", 50, stampede_prevention=False)

    assert await stampede_cache.aset("asp_flags_stale", "fresh", timeout=300, **flags) is reply
    assert await stampede_cache.aget("asp_flags_stale", stampede_prevention=False) == stored
    ttl = await stampede_cache.attl("asp_flags_stale", stampede_prevention=False)
    assert ttl is not None
    assert raw_ttl - 10 < ttl <= raw_ttl


@pytest.mark.parametrize(
    "flags",
    [{"get": True}, {"xx": True, "get": True}, {"nx": True, "get": True}],
    ids=["get", "xx-get", "nx-get"],
)
def test_set_get_on_a_logically_expired_list_raises_wrongtype(stampede_cache: RespCache, flags: dict[str, bool]):
    if flags.get("nx"):
        skip_below_server(stampede_cache, redis=(7, 0), feature="SET NX GET")
    stampede_cache.rpush("sp_list_stale", "x")
    stampede_cache.expire("sp_list_stale", 50, stampede_prevention=False)

    with pytest.raises(WrongTypeError):
        stampede_cache.set("sp_list_stale", "v", timeout=300, **flags)
    assert stampede_cache.lrange("sp_list_stale", 0, -1) == ["x"]


def test_set_nx_get_on_a_logically_expired_key_raises_not_supported_before_redis_7(stampede_cache: RespCache):
    server, version = server_version(stampede_cache)
    if server == "valkey" or version >= (7, 0):
        pytest.skip("the server accepts SET NX GET")
    stampede_cache.set("sp_nx_get_old_server", "stale", timeout=300)
    stampede_cache.expire("sp_nx_get_old_server", 50, stampede_prevention=False)

    with pytest.raises(NotSupportedError):
        stampede_cache.set("sp_nx_get_old_server", "fresh", timeout=300, nx=True, get=True)
    assert stampede_cache.get("sp_nx_get_old_server", stampede_prevention=False) == "stale"
