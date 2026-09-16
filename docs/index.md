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
- `TrackingCache`, a local read cache over a Redis/Valkey alias, invalidated by the server's `CLIENT TRACKING` or bounded by a local TTL.
- Django `LocMemCache` and `DatabaseCache` extensions with the hash, list, set and sorted set ops, `ttl()`/`expire()`/`persist()`, key patterns, and admin support (no streams, locks, pipelines or Lua).
- Optional `valkey-glide` adapter: Valkey's official Rust-cored client, exposed as `ValkeyGlideCache`. Experimental.
- Django admin UI for browsing keys, inspecting values, editing, and flushing.

## Requirements

- Python 3.14+. The free-threaded build (3.14t) is supported; note that
  `hiredis` and `libvalkey` are C extensions without free-threading support,
  so importing either on 3.14t re-enables the GIL with a `RuntimeWarning`.
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 6.0 to 8.x (`redis>=6,<9`)
- Valkey 7.2+ or Redis 6.2+ on the server. `set(get=True)` and the
  immediate-expiry conditional writes (`add()` and `set(nx=/xx=/get=)` with
  `timeout=0`) send `SET ... GET` and `SET ... PXAT`, both Redis 6.2 commands;
  `set(nx=True, get=True)` needs Redis 7.0+
- Hash field expiration needs Valkey 9.0+ or Redis 7.4+, and `hsetex`/`hgetex`
  Redis 8.0+; older servers raise `NotSupportedError` for those methods

The `valkey-glide` adapter is optional and experimental: interfaces and
behavior may still change, and it has seen less production testing than
the redis-py/valkey-py paths. Install with the `valkey-glide` extra
(`pip install django-cachex[valkey-glide]`) to enable
`ValkeyGlideCache`; it pulls in `valkey-glide-sync` and `valkey-glide`
(2.5 to 2.x), the official Rust-cored Valkey client. cp314 GIL only; no
free-threaded wheels yet. Cluster is supported via
`ValkeyGlideClusterCache`; Sentinel is not currently exposed
(`valkey-glide` itself does not ship a Sentinel client).

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

This project started from [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some serializer and compressor utility code is derived from django-redis, licensed under BSD-3-Clause. The admin UI was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard).

The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip), so the numbers are directly comparable.

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
