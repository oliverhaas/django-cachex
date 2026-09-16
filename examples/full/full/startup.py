"""
Full example project startup data population.

Populate every configured cache with sample data.

This runs on every Django startup because:
- LocMemCache doesn't persist between restarts
- Django's auto-reload restarts the server frequently during development

The full example wires up the backends listed in settings.py, so we populate
each with appropriate sample data including Redis-specific data types.

The logic is intentionally verbose so developers can see what's happening.
"""

# ruff: noqa: T201, BLE001
# T201: print statements are intentional for visibility
# BLE001: seed data is best-effort; any failure must not block startup

from typing import Any

from django.conf import settings
from django.core.cache import caches

from django_cachex.cache.base import BaseCachex

# Minimum number of keys expected in each cache
MIN_KEYS = 5


def get_basic_sample_data() -> dict:
    """Basic sample data suitable for any cache backend."""
    return {
        # User data
        "user:1": {"name": "Alice", "email": "alice@example.com", "role": "admin"},
        "user:2": {"name": "Bob", "email": "bob@example.com", "role": "user"},
        # Session data
        "session:abc123": {"user_id": 1, "active": True},
        # Application data
        "greeting": "Hello from this cache!",
        "counter": 42,
        "config:version": "1.0.0",
    }


def supports(cache: Any, operation: str) -> bool:
    """Whether this backend really implements ``operation``.

    ``hasattr`` cannot answer that: :class:`BaseCachex` declares every
    extension operation and raises ``NotSupportedError`` from it, so the
    attribute exists on backends that do not support the command.
    """
    implementation = getattr(type(cache), operation, None)
    return implementation is not None and implementation is not getattr(BaseCachex, operation, None)


def populate_redis_data_types(cache: Any, cache_alias: str) -> int:
    """
    Populate the collection types (list, set, hash, zset).

    Skips any operation the backend does not implement.

    Returns:
        Number of keys successfully created.
    """
    created = 0

    # List
    if supports(cache, "rpush"):
        try:
            cache.rpush("mylist", "item1", "item2", "item3")
            created += 1
        except Exception as e:
            print(f"    [{cache_alias}] Warning: Failed to create list: {e}")

    # Set
    if supports(cache, "sadd"):
        try:
            cache.sadd("myset", "member1", "member2", "member3")
            created += 1
        except Exception as e:
            print(f"    [{cache_alias}] Warning: Failed to create set: {e}")

    # Hash
    if supports(cache, "hset"):
        try:
            cache.hset("myhash", "field1", "value1")
            cache.hset("myhash", "field2", "value2")
            created += 1
        except Exception as e:
            print(f"    [{cache_alias}] Warning: Failed to create hash: {e}")

    # Sorted Set
    if supports(cache, "zadd"):
        try:
            cache.zadd("myzset", {"one": 1.0, "two": 2.0, "three": 3.0})
            created += 1
        except Exception as e:
            print(f"    [{cache_alias}] Warning: Failed to create zset: {e}")

    return created


_SKIP_ALIASES: dict[str, str] = {
    "celery": "populated by Celery via ./run.sh send-tasks",
}


def populate_cache_if_needed(cache_alias: str) -> None:
    """
    Check if cache has enough keys, populate if not.

    Args:
        cache_alias: The cache alias from Django's CACHES setting.
    """
    cache_config = settings.CACHES.get(cache_alias, {})
    backend = cache_config.get("BACKEND", "")

    if cache_alias in _SKIP_ALIASES:
        print(f"  [{cache_alias}] Skipping ({_SKIP_ALIASES[cache_alias]})")
        return

    # Skip dummy cache - it doesn't store anything
    if "DummyCache" in backend:
        print(f"  [{cache_alias}] Skipping DummyCache (doesn't store data)")
        return

    cache = caches[cache_alias]

    # Check current key count. DatabaseCache cannot count keys without DB
    # queries, which warns during startup, so it gets its own branch.
    try:
        if "DatabaseCache" in backend:
            # For database cache, check if we can get a known key
            # If not found, assume it needs population
            count = 0 if cache.get("config:version") is None else MIN_KEYS
        elif supports(cache, "keys"):
            # Count keys via keys() if available
            count = len(list(cache.keys("*")))  # type: ignore[operator]
        else:
            count = 0
    except Exception:
        count = 0

    if count >= MIN_KEYS:
        print(f"  [{cache_alias}] Already has {count} keys, skipping population")
        return

    print(f"  [{cache_alias}] Found {count} keys, populating with sample data...")

    # Populate with basic sample data
    sample_data = get_basic_sample_data()
    populated = 0

    for key, value in sample_data.items():
        try:
            cache.set(key, value, timeout=3600)
            populated += 1
        except Exception as e:
            print(f"    [{cache_alias}] Warning: Failed to set {key}: {e}")

    if supports(cache, "rpush"):
        populated += populate_redis_data_types(cache, cache_alias)

    print(f"  [{cache_alias}] Added {populated} sample keys/structures")


def ensure_sample_data() -> None:
    """
    Ensure all configured caches have sample data.

    This is called on Django startup via AppConfig.ready().
    """
    print("\n" + "=" * 60)
    print("CACHE STARTUP: Checking sample data (Full Example)")
    print("=" * 60)

    # Process caches in a specific order for readability
    # Standalone first, then cluster, sentinel, then Django builtins
    cache_order = [
        "default",
        "redis",
        "tracking",
        "celery",
        "cluster",
        "sentinel",
        "locmem",
        "database",
        "file",
        "dummy",
    ]
    other_caches = [c for c in settings.CACHES if c not in cache_order]

    for cache_alias in cache_order + other_caches:
        if cache_alias in settings.CACHES:
            try:
                populate_cache_if_needed(cache_alias)
            except Exception as e:
                print(f"  [{cache_alias}] Error during population: {e}")

    print("=" * 60)
    print("CACHE STARTUP: Complete")
    print("=" * 60 + "\n")
