"""The ORM cache's store on every RESP adapter and topology.

tests/orm runs the ORM cache on redis-py only. Here its store runs on each driver, standalone, Sentinel and
Cluster, whose replies come back in the driver's own types.
"""

import time
from typing import TYPE_CHECKING

import pytest
from django.core.cache import DEFAULT_CACHE_ALIAS, caches

from django_cachex.orm.store import BYPASS, RespStore, _LocalResults, get_store
from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from django_cachex.cache import RespCache

DB = "default"
# Several tables; the database alias, their hash tag, keeps their keys in one cluster slot.
TABLES = ("shop_order", "shop_line")
ENTRY = "orm:{default}:q:query"
GENERATION = "orm:{default}:g:shop_order"
LEASE = "orm:{default}:l:shop_order"


@pytest.fixture
def store(cache: RespCache) -> RespStore:
    store = get_store(DEFAULT_CACHE_ALIAS)
    assert isinstance(store, RespStore)
    return store


def test_miss_store_hit(cache: RespCache, store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert not miss.hit
    assert miss.token is not None

    assert store.store(DB, "query", TABLES, miss.token, [(1, "a"), (2, "b")], 60)
    hit = store.lookup(DB, "query", TABLES)
    assert hit.hit
    assert hit.value == [(1, "a"), (2, "b")]
    ttl = cache.ttl(ENTRY)
    assert ttl is not None
    assert 0 < ttl <= 60


def test_store_without_timeout(cache: RespCache, store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert store.store(DB, "query", TABLES, miss.token, [(1, "a")], None)
    assert store.lookup(DB, "query", TABLES).value == [(1, "a")]
    assert cache.ttl(ENTRY) is None


def test_generations(store: RespStore):
    generations = store.generations(DB, TABLES)
    assert generations is not None
    assert len(generations) == len(TABLES)
    assert all(generation.isdigit() for generation in generations)
    assert store.generations(DB, TABLES) == generations
    assert store.lookup(DB, "query", TABLES).token == ":".join(generations).encode()


def test_lease(store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    before = store.generations(DB, TABLES)

    store.begin_write(DB, TABLES[:1], "writer", 60)
    assert store.lookup(DB, "query", TABLES) is BYPASS
    assert store.generations(DB, TABLES) is None
    assert not store.store(DB, "query", TABLES, miss.token, "stale", 60)

    store.end_write(DB, TABLES[:1], "writer")
    after = store.generations(DB, TABLES)
    assert before is not None
    assert after is not None
    assert int(after[0]) == int(before[0]) + 2
    assert after[1] == before[1]
    assert not store.store(DB, "query", TABLES, miss.token, "stale", 60)
    fresh = store.lookup(DB, "query", TABLES)
    assert store.store(DB, "query", TABLES, fresh.token, "fresh", 60)
    assert store.lookup(DB, "query", TABLES).value == "fresh"


def test_lookup_drops_an_expired_lease(cache: RespCache, store: RespStore):
    store.begin_write(DB, TABLES[:1], "writer", 0.001)
    time.sleep(0.05)

    assert store.lookup(DB, "query", TABLES).token is not None
    assert cache.ttl(LEASE) == -2


def test_leases_of_two_writers(store: RespStore):
    store.begin_write(DB, TABLES, "first", 60)
    store.begin_write(DB, TABLES[1:], "second", 60)
    store.end_write(DB, TABLES, "first")
    assert store.generations(DB, TABLES) is None
    store.end_write(DB, TABLES[1:], "second")
    assert store.generations(DB, TABLES) is not None


def test_keys_without_expiry(cache: RespCache, store: RespStore):
    # volatile-* eviction takes only results: generation and lease keys never expire.
    assert store.generations(DB, TABLES) is not None
    store.begin_write(DB, TABLES[:1], "writer", 60)
    assert cache.ttl(GENERATION) is None
    assert cache.ttl(LEASE) is None
    store.end_write(DB, TABLES[:1], "writer")
    assert cache.ttl(LEASE) == -2


def test_bump(store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert store.store(DB, "query", TABLES, miss.token, "value", 60)
    before = store.generations(DB, TABLES)

    store.bump(DB, TABLES[1:])
    after = store.generations(DB, TABLES)
    assert before is not None
    assert after is not None
    assert after[0] == before[0]
    assert int(after[1]) == int(before[1]) + 1
    assert not store.lookup(DB, "query", TABLES).hit


def test_local_copy(cache: RespCache, store: RespStore):
    local = RespStore(caches[DEFAULT_CACHE_ALIAS], _LocalResults(max_entries=10))
    miss = local.lookup(DB, "query", TABLES)
    assert local.store(DB, "query", TABLES, miss.token, {"a": 1}, 60)

    # The server's payload changes but not its generations, so the local copy is served.
    cache.eval_script(
        "return redis.call('HSET', KEYS[1], 'v', ARGV[1])",
        keys=[ENTRY],
        args=["not a pickle"],
        pre_hook=keys_only_pre,
    )
    hit = local.lookup(DB, "query", TABLES)
    assert hit.hit
    assert hit.value == {"a": 1}

    # Without the local copy, the broken payload is fetched and counts as a miss.
    remote = store.lookup(DB, "query", TABLES)
    assert not remote.hit
    assert remote.token == miss.token


def test_outdated_local_copy_gives_way_to_the_server_copy(cache: RespCache):
    first = RespStore(caches[DEFAULT_CACHE_ALIAS], _LocalResults(max_entries=10))
    second = RespStore(caches[DEFAULT_CACHE_ALIAS], _LocalResults(max_entries=10))
    assert first.store(DB, "query", TABLES, first.lookup(DB, "query", TABLES).token, "old", 60)
    first.bump(DB, TABLES)
    assert second.store(DB, "query", TABLES, second.lookup(DB, "query", TABLES).token, "new", 60)

    assert first.lookup(DB, "query", TABLES).value == "new"


@pytest.mark.parametrize("local", [False, True], ids=["server_copy", "local_copy"])
def test_hit_runs_no_script_outside_a_cluster(cache: RespCache, topology: str, local: bool, mocker):
    store = RespStore(caches[DEFAULT_CACHE_ALIAS], _LocalResults(max_entries=10) if local else None)
    assert store.store(DB, "query", TABLES, store.lookup(DB, "query", TABLES).token, "value", 60)
    scripts = mocker.spy(store.cache.adapter, "eval")

    assert store.lookup(DB, "query", TABLES).value == "value"
    assert scripts.call_count == (1 if topology == "cluster" else 0)


def test_local_copies_keyed_like_the_server(cache: RespCache, mocker):
    real = caches[DEFAULT_CACHE_ALIAS]
    local = RespStore(real, _LocalResults(max_entries=10))
    for tenant in ("a", "b"):
        # A key prefix per tenant, whose separate generations happen to be equal.
        mocker.patch.object(real, "key_prefix", tenant)
        cache.eval_script(
            "for _, key in ipairs(KEYS) do redis.call('SET', key, '1000') end",
            keys=[f"orm:{{{DB}}}:g:{table}" for table in TABLES],
            pre_hook=keys_only_pre,
        )
        miss = local.lookup(DB, "query", TABLES)
        assert local.store(DB, "query", TABLES, miss.token, tenant, 60)

    mocker.patch.object(real, "key_prefix", "a")
    assert local.lookup(DB, "query", TABLES).value == "a"
