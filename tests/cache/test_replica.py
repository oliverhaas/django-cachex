"""Integration tests for master-replica Redis setup.

Tests the multi-server read replica functionality where:
- Writes go to the first server (master)
- Reads distribute to replica servers
"""

import asyncio
import time
from typing import TYPE_CHECKING, Any

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import ReplicaSetContainerInfo


@pytest.fixture
def replica_urls(replica_containers: ReplicaSetContainerInfo) -> list[str]:
    """Every server URL, master first: that order is what selects the write pool."""
    return [
        f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0",
        *(
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ),
    ]


@pytest.fixture
def replica_cache(replica_urls: list[str]) -> Iterator[RespCache]:
    caches_config = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": replica_urls,
        },
    }
    with override_settings(CACHES=caches_config):
        yield caches["default"]


def wait_for_replication(
    cache,
    expected: dict[str, Any],
    *,
    max_attempts: int = 50,
    sleep_interval: float = 0.1,
) -> dict[str, Any]:
    """Poll ``get_many`` until every key carries its expected value.

    Polling in small steps rather than sleeping once for the worst case
    keeps the tests fast while tolerating replication lag.

    Returns:
        The last ``get_many`` result, so the caller can assert on it.
    """
    keys = list(expected.keys())
    result: dict[str, Any] = {}
    for _ in range(max_attempts):
        result = cache.get_many(keys)
        if result == expected:
            return result
        time.sleep(sleep_interval)
    return result


class TestReplicaSetup:
    """Tests for master-replica Redis setup."""

    def test_replica_containers_start(self, replica_containers: ReplicaSetContainerInfo):
        assert replica_containers.master_host
        assert replica_containers.master_port > 0
        assert len(replica_containers.replica_hosts) == 2
        assert len(replica_containers.replica_ports) == 2
        assert all(port > 0 for port in replica_containers.replica_ports)

    def test_write_to_master_read_from_replica(self, replica_cache: RespCache):
        replica_cache.set("replica_test_key", "test_value", timeout=60)

        result = wait_for_replication(replica_cache, {"replica_test_key": "test_value"})
        assert result == {"replica_test_key": "test_value"}, "Replication timed out"

        replica_cache.delete("replica_test_key")

    def test_pool_selection_write_uses_master(self, replica_cache: RespCache):
        for _ in range(10):
            assert replica_cache.adapter._get_connection_pool_index(write=True) == 0

    def test_pool_selection_read_uses_replicas(self, replica_cache: RespCache, replica_urls: list[str]):
        read_indices = set()
        for _ in range(50):
            read_idx = replica_cache.adapter._get_connection_pool_index(write=False)
            assert 0 < read_idx < len(replica_urls), (
                f"Read index {read_idx} should be in replica range [1, {len(replica_urls)})"
            )
            read_indices.add(read_idx)

        # Fifty uniform picks hit every replica; a stuck picker would not.
        expected_indices = set(range(1, len(replica_urls)))
        assert read_indices == expected_indices, (
            f"Expected reads to distribute across {expected_indices}, got {read_indices}"
        )

    def test_one_pool_per_server_read_from(self, replica_cache: RespCache, replica_urls: list[str]):
        """Reads spread over the replicas, and each one that is used gets its own pool."""
        replica_cache.set("pool_test", "value", timeout=60)
        assert wait_for_replication(replica_cache, {"pool_test": "value"}) == {"pool_test": "value"}

        for _ in range(20):
            replica_cache.get("pool_test")

        pools = replica_cache.adapter._pools
        assert set(pools) == set(range(len(replica_urls))), (
            f"Expected a pool per server after 20 reads, got indices {sorted(pools)}"
        )
        assert len({id(pool) for pool in pools.values()}) == len(replica_urls)

        replica_cache.delete("pool_test")

    def test_servers_list_configuration(self, replica_cache: RespCache, replica_urls: list[str]):
        assert replica_cache.adapter._servers == replica_urls
        assert len(replica_cache.adapter._servers) == 3


class TestReplicaDataIntegrity:
    """Tests for data integrity across master-replica setup."""

    def test_set_get_many_with_replicas(self, replica_cache: RespCache):
        data = {f"many_key_{i}": f"value_{i}" for i in range(10)}
        replica_cache.set_many(data, timeout=60)

        # get_many may route through a different pool than get.
        assert wait_for_replication(replica_cache, data) == data

        replica_cache.delete_many(list(data.keys()))

    def test_incr_decr_with_replicas(self, replica_cache: RespCache):
        """incr and decr always go to the master, and the result replicates."""
        replica_cache.set("counter", 10, timeout=60)
        assert wait_for_replication(replica_cache, {"counter": 10}) == {"counter": 10}

        assert replica_cache.incr("counter", 5) == 15
        assert wait_for_replication(replica_cache, {"counter": 15}) == {"counter": 15}

        assert replica_cache.decr("counter", 3) == 12

        replica_cache.delete("counter")

    def test_delete_propagates_to_replicas(self, replica_cache: RespCache):
        replica_cache.set("delete_test", "value", timeout=60)
        assert wait_for_replication(replica_cache, {"delete_test": "value"}) == {"delete_test": "value"}

        replica_cache.delete("delete_test")

        # A miss reads back as None, which wait_for_replication cannot express.
        for _ in range(50):
            if replica_cache.get("delete_test") is None:
                break
            time.sleep(0.1)
        assert replica_cache.get("delete_test") is None


@pytest.mark.asyncio
class TestReplicaAsync:
    """Async tests for master-replica setup."""

    async def test_async_write_read_with_replicas(self, replica_cache: RespCache):
        await replica_cache.aset("async_replica_test", "async_value", timeout=60)

        replicated = False
        for _ in range(50):
            if await replica_cache.aget("async_replica_test") == "async_value":
                replicated = True
                break
            await asyncio.sleep(0.1)

        assert replicated, "Async replication timed out"

        await replica_cache.adelete("async_replica_test")
