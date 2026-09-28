"""The ORM cache's Lua scripts on every RESP adapter and topology."""

# tests/orm runs the ORM cache on redis-py only. Here its store runs on each
# driver, standalone, Sentinel and Cluster, whose script replies come back in
# the driver's own types.

from typing import TYPE_CHECKING

from django.core.cache import DEFAULT_CACHE_ALIAS, caches

from django_cachex.orm.store import BYPASS, RespStore, _LocalResults, get_store
from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from django_cachex.cache import RespCache

DB = "default"
# Two tables, so the scripts touch several generation and lease keys. The
# database alias is their hash tag, which keeps them in one cluster slot.
TABLES = ("shop_order", "shop_line")
ENTRY = "orm:{default}:q:query"


def _store() -> RespStore:
    store = get_store(DEFAULT_CACHE_ALIAS)
    assert isinstance(store, RespStore)
    return store


class TestOrmStore:
    def test_miss_store_hit(self, cache: RespCache):
        store = _store()
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

    def test_integer_result(self, cache: RespCache):
        # encode() passes ints through, so a count() result is sent as digits.
        store = _store()
        miss = store.lookup(DB, "query", TABLES)
        assert store.store(DB, "query", TABLES, miss.token, 42, None)
        assert store.lookup(DB, "query", TABLES).value == 42
        assert cache.ttl(ENTRY) is None

    def test_generations(self, cache: RespCache):
        store = _store()
        generations = store.generations(DB, TABLES)
        assert generations is not None
        assert len(generations) == len(TABLES)
        assert all(generation.isdigit() for generation in generations)
        assert store.generations(DB, TABLES) == generations
        # A lookup reads the same generations.
        assert store.lookup(DB, "query", TABLES).token == ":".join(generations).encode()

    def test_lease(self, cache: RespCache):
        store = _store()
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
        # A result read before the write no longer stores.
        assert not store.store(DB, "query", TABLES, miss.token, "stale", 60)
        fresh = store.lookup(DB, "query", TABLES)
        assert store.store(DB, "query", TABLES, fresh.token, "fresh", 60)
        assert store.lookup(DB, "query", TABLES).value == "fresh"

    def test_leases_of_two_writers(self, cache: RespCache):
        store = _store()
        store.begin_write(DB, TABLES, "first", 60)
        store.begin_write(DB, TABLES[1:], "second", 60)
        store.end_write(DB, TABLES, "first")
        # The second writer still holds its lease on the second table.
        assert store.generations(DB, TABLES) is None
        store.end_write(DB, TABLES[1:], "second")
        assert store.generations(DB, TABLES) is not None

    def test_bump(self, cache: RespCache):
        store = _store()
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

    def test_local_copy(self, cache: RespCache):
        store = RespStore(caches[DEFAULT_CACHE_ALIAS], _LocalResults(max_entries=10))
        miss = store.lookup(DB, "query", TABLES)
        assert store.store(DB, "query", TABLES, miss.token, {"a": 1}, 60)

        # Replace the server's payload but not its generations: a lookup that
        # sends the generations of the local copy is answered without the
        # payload and serves the local copy.
        cache.eval_script(
            "return redis.call('HSET', KEYS[1], 'v', ARGV[1])",
            keys=[ENTRY],
            args=["not a pickle"],
            pre_hook=keys_only_pre,
        )
        hit = store.lookup(DB, "query", TABLES)
        assert hit.hit
        assert hit.value == {"a": 1}

        # Without the local copy the payload is fetched, fails to decode and
        # counts as a miss, so the query runs and stores its result again.
        remote = _store().lookup(DB, "query", TABLES)
        assert not remote.hit
        assert remote.token == miss.token
