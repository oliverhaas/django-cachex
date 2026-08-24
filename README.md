# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.

## Installation

```console
pip install django-cachex[valkey-py]
```

## Quick Start

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    }
}
```

## What's in the box

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
- Django admin UI for browsing keys, inspecting values, editing, and flushing. See below.

## Cache Admin

Add `django_cachex.admin` to your `INSTALLED_APPS` to enable the cache admin interface:

```python
INSTALLED_APPS = [
    # ...
    "django_cachex.admin",
]
```

Browse all configured caches, search and filter keys by type, and manage values directly:

![Cache list](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-cache-list.png)
![Key list](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-key-list.png)
![Key detail](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-key-detail.png)

Features:
- Browse all configured cache backends (Valkey, Redis, LocMemCache, DatabaseCache, and more)
- Search keys with wildcard patterns (`user:*`, `*:session`)
- Filter by key type (string, list, set, hash, zset, stream)
- View and edit values with type-specific operations
- Inspect and modify TTL
- View server info and memory statistics
- Flush caches

## Documentation

Full documentation at [oliverhaas.github.io/django-cachex](https://oliverhaas.github.io/django-cachex/)

## Requirements

- Python 3.14+ (free-threaded supported)
- Django 6.0+
- valkey-py 6.1+ or redis-py 6.0+
- Valkey 7.0+ or Redis 6.0+ on the server (the admin's compare-and-swap
  edits use `SET ... KEEPTTL`, which lands in Redis 6.0)

The `valkey-glide` adapter is optional and experimental: interfaces and
behavior may still change, and it has seen less production testing than
the redis-py/valkey-py paths. Install with the `valkey-glide` extra
(`pip install django-cachex[valkey-glide]`) to enable
`ValkeyGlideCache`; it pulls in `valkey-glide-sync` and `valkey-glide`,
the official Rust-cored Valkey client. cp314 GIL only; no free-threaded
wheels yet. Cluster is supported via `ValkeyGlideClusterCache`; Sentinel
is not currently exposed (`valkey-glide` itself does not ship a Sentinel
client).

## Acknowledgments

This project started from [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some serializer and compressor utility code is derived from django-redis, licensed under BSD-3-Clause. The admin UI was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard).

The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip), so the numbers are directly comparable.

I also want to mention [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel), which I never really used, but are newer and interesting efforts of similar goals as this package has.

## License

MIT
