# django-cachex

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## What's in the box

A drop-in replacement for Django's built-in Redis cache, plus:

- One package for both Valkey and Redis, default and Sentinel and Cluster.
- Sync and async support sharing one configuration: every cache exposes both `get()` and `aget()`, backed by the same alias.
- Hash, list, set, sorted set, and stream operations on the cache object.
- TTL and pattern helpers (`ttl()`, `expire()`, `keys()`, `delete_pattern()`).
- Distributed locks: `cache.lock()`.
- Weighted semaphores: `cache.semaphore()` for budget-based concurrency gating (counting and weighted, in-process and distributed).
- Lua scripting with automatic key prefixing and value encoding/decoding.
- Pluggable serializers (Pickle, JSON, MsgPack, ormsgpack, orjson) and compressors (Zlib, Gzip, LZ4, LZMA, Zstandard), each with fallback chains for safe migrations.
- Cache stampede prevention (TTL-based XFetch).
- Two composite backends: `StreamCache` (cross-pod stream-synchronized in-memory cache) and `TieredCache` (L1/L2 with TTL propagation).
- Django `LocMemCache` and `DatabaseCache` extensions with the hash, list, set and sorted set ops (no streams) and admin support.
- Optional `valkey-glide` adapter: Valkey's official Rust-cored client, exposed as `ValkeyGlideCache`. Experimental.
- Django admin UI for browsing keys, inspecting values, editing, and flushing.

## Requirements

- Python 3.14+ (free-threaded supported)
- Django 6.0+
- valkey-py 6.1+ (redis-py 6.0+ also supported)
- Valkey 7.2+ or Redis 6.0+ on the server (the admin's compare-and-swap
  edits use `SET ... KEEPTTL`, which lands in Redis 6.0)

## Quick Start

Install with pip:

```console
pip install django-cachex[valkey-py]
```

Configure as cache backend:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    }
}
```

Enable the admin interface (optional):

```python
INSTALLED_APPS = [
    # ...
    "django_cachex.admin",  # cache admin interface
]
```

## Acknowledgments

This project was inspired by [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors is derived from django-redis, licensed under BSD-3-Clause. The admin functionality was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard).

The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip), so the numbers are directly comparable.

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
