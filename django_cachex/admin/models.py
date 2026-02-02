from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

from django.conf import settings
from django.db import models

if TYPE_CHECKING:
    from .service import CacheService


class Cache(models.Model):
    """
    Fake model representing a Django cache instance.

    This model is not managed by Django (no migrations, no database table).
    It provides a hook for Django admin to display cache instances.
    """

    # Fake primary key - corresponds to cache name
    name = models.CharField(max_length=255, primary_key=True)

    class Meta:
        managed = False
        app_label = "django_cachex"
        verbose_name = "Cache"
        verbose_name_plural = "Caches"

    def __str__(self) -> str:
        return self.name or "Cache"

    @classmethod
    def get_all(cls) -> list[Cache]:
        """Get all configured caches as Cache instances."""
        caches = []
        for name in settings.CACHES:
            cache = cls()
            cache.name = name
            caches.append(cache)
        return caches

    @classmethod
    def get_by_name(cls, name: str) -> Cache | None:
        """Get a Cache instance by name."""
        if name in settings.CACHES:
            cache = cls()
            cache.name = name
            return cache
        return None

    @property
    def config(self) -> dict:
        """Get the cache configuration dict."""
        return dict(settings.CACHES.get(self.name, {}))

    @property
    def backend(self) -> str:
        """Get the backend class path."""
        return str(self.config.get("BACKEND", "Unknown"))

    @property
    def backend_short(self) -> str:
        """Get the short backend class name."""
        backend = self.backend
        return backend.rsplit(".", 1)[-1] if "." in backend else backend

    @property
    def location(self) -> str:
        """Get the cache location."""
        loc = self.config.get("LOCATION", "")
        if isinstance(loc, list):
            return ", ".join(str(item) for item in loc)
        return str(loc) if loc else ""

    @property
    def service(self) -> CacheService:
        """Get the CacheService for this cache."""
        from .service import get_cache_service

        return get_cache_service(self.name)

    @property
    def support_level(self) -> str:
        """Determine the support level for this cache backend.

        Returns:
            - "cachex": Full support (django-cachex backends)
            - "wrapped": Django core builtin backends (wrapped for almost full support)
            - "limited": Custom/unknown backends with limited support
        """
        backend = self.backend

        # django-cachex backends - full support
        if backend.startswith("django_cachex."):
            return "cachex"

        # Django core builtin backends - wrapped support
        django_builtins = {
            "django.core.cache.backends.locmem.LocMemCache",
            "django.core.cache.backends.db.DatabaseCache",
            "django.core.cache.backends.filebased.FileBasedCache",
            "django.core.cache.backends.dummy.DummyCache",
            "django.core.cache.backends.memcached.PyMemcacheCache",
            "django.core.cache.backends.memcached.PyLibMCCache",
            "django.core.cache.backends.memcached.MemcachedCache",
        }
        if backend in django_builtins:
            return "wrapped"

        # Unknown/custom backends
        return "limited"

    @property
    def pk(self) -> str:
        """Primary key is the cache name."""
        return self.name

    @pk.setter
    def pk(self, value: str):
        """Set primary key (cache name)."""
        self.name = value


class Key(models.Model):
    """
    Fake model representing a cache key.

    This model is not managed by Django (no migrations, no database table).
    Keys are identified by a composite of (cache_name, key_name).
    """

    # Composite identifier encoded as: cache_name:url_encoded_key_name
    id = models.CharField(max_length=2048, primary_key=True)

    # Denormalized fields for display
    cache_name = models.CharField(max_length=255)
    key_name = models.CharField(max_length=2048)

    class Meta:
        managed = False
        app_label = "django_cachex"
        verbose_name = "Key"
        verbose_name_plural = "Keys"

    def __str__(self) -> str:
        return self.key_name or "Key"

    @classmethod
    def make_pk(cls, cache_name: str, key_name: str) -> str:
        """Create a URL-safe primary key from cache name and key name.

        Format: cache_name:url_encoded_key_name
        """
        return f"{cache_name}:{quote(key_name, safe='')}"

    @classmethod
    def parse_pk(cls, pk: str) -> tuple[str, str]:
        """Parse a primary key into (cache_name, key_name)."""
        parts = pk.split(":", 1)
        if len(parts) == 2:
            return parts[0], unquote(parts[1])
        return "", pk

    @classmethod
    def from_cache_key(cls, cache_name: str, key_name: str) -> Key:
        """Create a Key instance from cache name and key name."""
        key = cls()
        key.id = cls.make_pk(cache_name, key_name)
        key.cache_name = cache_name
        key.key_name = key_name
        return key

    @property
    def pk(self) -> str:
        """Primary key is the encoded cache:key composite."""
        return self.id

    @pk.setter
    def pk(self, value: str):
        """Set primary key and update cache_name/key_name."""
        self.id = value
        self.cache_name, self.key_name = self.parse_pk(value)

    @property
    def cache(self) -> Cache | None:
        """Get the parent Cache object."""
        return Cache.get_by_name(self.cache_name)

    @property
    def service(self) -> CacheService:
        """Get the CacheService for this key's cache."""
        from .service import get_cache_service

        return get_cache_service(self.cache_name)
