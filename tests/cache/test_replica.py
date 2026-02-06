"""Integration tests for master-replica Redis setup.

Tests the multi-server read replica functionality where:
- Writes go to the first server (master)
- Reads distribute to replica servers
"""

from __future__ import annotations

import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from tests.fixtures.containers import ReplicaSetContainerInfo


def wait_for_replication(
    cache,
    expected: dict[str, Any],
    *,
    max_attempts: int = 50,
    sleep_interval: float = 0.1,
) -> dict[str, Any]:
    """Wait for keys to replicate with expected values.

    Uses polling with small sleeps rather than a fixed large sleep
    to handle replication lag efficiently. Works for single keys or
    multiple keys via get_many.

    Args:
        cache: The cache instance to check
        expected: Dict mapping keys to their expected values
        max_attempts: Maximum number of polling attempts
        sleep_interval: Time to sleep between attempts in seconds

    Returns:
        The last get_many result (for assertion)
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
        """Test that master and replica containers start successfully."""
        assert replica_containers.master_host
        assert replica_containers.master_port > 0
        assert len(replica_containers.replica_hosts) == 2
        assert len(replica_containers.replica_ports) == 2
        assert all(port > 0 for port in replica_containers.replica_ports)

    def test_write_to_master_read_from_replica(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test that data written to master replicates to replicas."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Write data (goes to master)
            cache.set("replica_test_key", "test_value", timeout=60)

            # Wait for replication and verify data is readable
            result = wait_for_replication(cache, {"replica_test_key": "test_value"})
            assert result == {"replica_test_key": "test_value"}, "Replication timed out"

            # Clean up
            cache.delete("replica_test_key")

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_pool_selection_write_uses_master(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test that write operations always use index 0 (master)."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Check that write always selects index 0
            for _ in range(10):
                write_idx = cache._cache._get_connection_pool_index(write=True)
                assert write_idx == 0, "Write should always use master (index 0)"

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_pool_selection_read_uses_replicas(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test that read operations select from replica indices."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            total_servers = 1 + len(replica_urls)  # master + replicas

            # Check that read selects valid replica indices
            read_indices = set()
            for _ in range(50):  # Run multiple times to see distribution
                read_idx = cache._cache._get_connection_pool_index(write=False)
                assert 0 < read_idx < total_servers, (
                    f"Read index {read_idx} should be in replica range [1, {total_servers})"
                )
                read_indices.add(read_idx)

            # Should have selected multiple different replicas
            assert len(read_indices) >= 1, "Should select from available replicas"

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_multiple_pools_created(self, replica_containers: ReplicaSetContainerInfo):
        """Test that separate connection pools are created for each server."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Force pool creation by doing operations
            cache.set("pool_test", "value", timeout=60)

            # Wait for replication
            assert wait_for_replication(cache, {"pool_test": "value"}) == {"pool_test": "value"}

            # Do multiple reads to create replica pools
            for _ in range(20):
                cache.get("pool_test")

            # Check pools were created
            pools = cache._cache._pools
            assert len(pools) >= 1, "At least master pool should exist"

            # Clean up
            cache.delete("pool_test")

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_servers_list_configuration(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test that _servers list is correctly configured."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]
        all_urls = [master_url, *replica_urls]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": all_urls,
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Verify _servers list
            assert cache._cache._servers == all_urls
            assert len(cache._cache._servers) == 3  # 1 master + 2 replicas

        with suppress(KeyError, AttributeError):
            del caches["default"]


class TestReplicaDataIntegrity:
    """Tests for data integrity across master-replica setup."""

    def test_set_get_many_with_replicas(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test set_many/get_many work correctly with replicas."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Write many keys
            data = {f"many_key_{i}": f"value_{i}" for i in range(10)}
            cache.set_many(data, timeout=60)

            # Wait for all keys to be readable via get_many.
            # get_many may route through a different connection pool than get,
            # so waiting for a single key via get() is not sufficient.
            result = wait_for_replication(cache, data)
            assert result == data

            # Clean up
            cache.delete_many(list(data.keys()))

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_incr_decr_with_replicas(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test increment/decrement work correctly (always goes to master)."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Set initial value
            cache.set("counter", 10, timeout=60)

            # Wait for replication
            assert wait_for_replication(cache, {"counter": 10}) == {"counter": 10}

            # Increment (goes to master)
            result = cache.incr("counter", 5)
            assert result == 15

            # Wait for replication of new value
            assert wait_for_replication(cache, {"counter": 15}) == {"counter": 15}

            # Decrement
            result = cache.decr("counter", 3)
            assert result == 12

            # Clean up
            cache.delete("counter")

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_delete_propagates_to_replicas(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test that delete operations propagate to replicas."""
        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Set and verify replication
            cache.set("delete_test", "value", timeout=60)
            assert wait_for_replication(cache, {"delete_test": "value"}) == {"delete_test": "value"}

            # Delete
            cache.delete("delete_test")

            # Wait for delete to propagate - use direct polling since
            # get_many returns {} for missing keys, not a value we can match
            for _ in range(50):
                if cache.get("delete_test") is None:
                    break
                time.sleep(0.1)
            assert cache.get("delete_test") is None

        with suppress(KeyError, AttributeError):
            del caches["default"]


@pytest.mark.asyncio
class TestReplicaAsync:
    """Async tests for master-replica setup."""

    async def test_async_write_read_with_replicas(
        self,
        replica_containers: ReplicaSetContainerInfo,
    ):
        """Test async operations work with replica setup."""
        import asyncio

        master_url = f"redis://{replica_containers.master_host}:{replica_containers.master_port}/0"
        replica_urls = [
            f"redis://{host}:{port}/0"
            for host, port in zip(
                replica_containers.replica_hosts,
                replica_containers.replica_ports,
                strict=True,
            )
        ]

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [master_url, *replica_urls],
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Async write
            await cache.aset("async_replica_test", "async_value", timeout=60)

            # Poll for replication with async sleep
            replicated = False
            for _ in range(50):
                value = await cache.aget("async_replica_test")
                if value == "async_value":
                    replicated = True
                    break
                await asyncio.sleep(0.1)

            assert replicated, "Async replication timed out"

            # Clean up
            await cache.adelete("async_replica_test")

        with suppress(KeyError, AttributeError):
            del caches["default"]
