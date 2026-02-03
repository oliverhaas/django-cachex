"""
Example project startup data population.

This module ensures the cache has sample data for demonstration purposes.
It runs on every Django startup because:
- LocMemCache doesn't persist between restarts
- Django's auto-reload restarts the server frequently during development

The logic is intentionally verbose so developers can see what's happening.
"""

# ruff: noqa: T201, BLE001
# T201: print statements are intentional for visibility
# BLE001: broad exception catching is intentional for robustness

from django.conf import settings
from django.core.cache import caches

# Minimum number of keys expected in each cache
MIN_KEYS = 10


def get_sample_data() -> dict:
    """Sample data to populate caches with."""
    return {
        # User data
        "user:1": {"name": "Alice", "email": "alice@example.com", "role": "admin"},
        "user:2": {"name": "Bob", "email": "bob@example.com", "role": "user"},
        "user:3": {"name": "Charlie", "email": "charlie@example.com", "role": "user"},
        # Session data
        "session:abc123": {"user_id": 1, "active": True, "created": "2024-01-01"},
        "session:def456": {"user_id": 2, "active": True, "created": "2024-01-02"},
        # Application data
        "greeting": "Hello, World!",
        "counter": 42,
        "tags": ["python", "django", "cachex"],
        "config:version": "1.0.0",
        "config:features": {"dark_mode": True, "notifications": True},
        # Additional keys to ensure we have enough
        "cache:stats": {"hits": 100, "misses": 10},
        "rate_limit:user:1": 50,
    }


def populate_cache_if_needed(cache_alias: str) -> None:
    """
    Check if cache has enough keys, populate if not.

    Args:
        cache_alias: The cache alias from Django's CACHES setting.
    """
    cache_config = settings.CACHES.get(cache_alias, {})
    backend = cache_config.get("BACKEND", "")

    # Skip dummy cache - it doesn't store anything
    if "DummyCache" in backend:
        print(f"  [{cache_alias}] Skipping DummyCache (doesn't store data)")
        return

    cache = caches[cache_alias]

    # Check current key count
    # Note: Not all backends support key counting, so we wrap in try/except
    # DatabaseCache cannot count keys without DB queries during startup
    try:
        if "DatabaseCache" in backend:
            # For database cache, check if we can get a known key
            count = 0 if cache.get("config:version") is None else MIN_KEYS
        else:
            from django_cachex.admin.service import CacheService

            service = CacheService(cache_alias, cache)
            if hasattr(service, "key_count"):
                count = service.key_count()  # type: ignore[operator]
            else:
                # Fallback: try counting via keys() if available
                count = len(list(cache.keys("*"))) if hasattr(cache, "keys") else 0  # type: ignore[operator]
    except Exception:
        # If we can't count keys, assume we need to populate
        count = 0

    if count >= MIN_KEYS:
        print(f"  [{cache_alias}] Already has {count} keys, skipping population")
        return

    print(f"  [{cache_alias}] Found {count} keys, populating with sample data...")

    # Populate with sample data
    sample_data = get_sample_data()
    populated = 0

    for key, value in sample_data.items():
        try:
            cache.set(key, value, timeout=3600)  # 1 hour timeout
            populated += 1
        except Exception as e:
            print(f"    Warning: Failed to set {key}: {e}")

    print(f"  [{cache_alias}] Added {populated} sample keys")


def ensure_sample_data() -> None:
    """
    Ensure all configured caches have sample data.

    This is called on Django startup via AppConfig.ready().
    """
    print("\n" + "=" * 60)
    print("CACHE STARTUP: Checking sample data")
    print("=" * 60)

    for cache_alias in settings.CACHES:
        populate_cache_if_needed(cache_alias)

    print("=" * 60)
    print("CACHE STARTUP: Complete")
    print("=" * 60 + "\n")
