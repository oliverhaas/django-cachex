"""The ORM cache's store on every RESP adapter and topology.

tests/orm runs the ORM cache on redis-py only. Here its store runs on each driver, standalone, Sentinel and
Cluster, whose replies come back in the driver's own types.
"""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import DEFAULT_CACHE_ALIAS

from django_cachex.orm.store import RespStore, get_store

if TYPE_CHECKING:
    from django_cachex.cache import RespCache

DB = "default"
# Several tables; the database alias, their hash tag, keeps their keys in one cluster slot.
TABLES = ("shop_order", "shop_line")
ENTRY = "orm:{default}:q:query"
GENERATION = "orm:{default}:g:shop_order"


@pytest.fixture
def store(cache: RespCache) -> RespStore:
    store = get_store(DEFAULT_CACHE_ALIAS)
    assert isinstance(store, RespStore)
    return store


def test_miss_store_hit(cache: RespCache, store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert not miss.hit
    assert miss.token is not None

    assert store.store(DB, "query", miss.token, [(1, "a"), (2, "b")], 60)
    hit = store.lookup(DB, "query", TABLES)
    assert hit.hit
    assert hit.value == [(1, "a"), (2, "b")]
    ttl = cache.ttl(ENTRY)
    assert ttl is not None
    assert 0 < ttl <= 60


def test_store_without_timeout(cache: RespCache, store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert store.store(DB, "query", miss.token, [(1, "a")], None)
    assert store.lookup(DB, "query", TABLES).value == [(1, "a")]
    assert cache.ttl(ENTRY) is None


def test_generations(store: RespStore):
    generations = store.generations(DB, TABLES)
    assert generations is not None
    assert len(generations) == len(TABLES)
    assert all(generation.isdigit() for generation in generations)
    assert store.generations(DB, TABLES) == generations
    assert store.lookup(DB, "query", TABLES).token == ":".join(generations)


def test_generations_without_expiry(cache: RespCache, store: RespStore):
    # volatile-* eviction takes only results: generation keys never expire.
    assert store.generations(DB, TABLES) is not None
    assert cache.ttl(GENERATION) is None
    store.bump(DB, TABLES)
    assert cache.ttl(GENERATION) is None


def test_bump(store: RespStore):
    miss = store.lookup(DB, "query", TABLES)
    assert store.store(DB, "query", miss.token, "value", 60)
    before = store.generations(DB, TABLES)

    store.bump(DB, TABLES[1:])
    after = store.generations(DB, TABLES)
    assert before is not None
    assert after is not None
    assert after[0] == before[0]
    assert after[1] != before[1]
    assert not store.lookup(DB, "query", TABLES).hit


def test_result_of_an_older_version_is_replaced(cache: RespCache, store: RespStore):
    # Earlier versions stored each result in a hash, which a lookup reads as missing.
    cache.hset(ENTRY, "g", "1:2")
    miss = store.lookup(DB, "query", TABLES)
    assert not miss.hit
    assert store.store(DB, "query", miss.token, "value", 60)
    assert store.lookup(DB, "query", TABLES).value == "value"
