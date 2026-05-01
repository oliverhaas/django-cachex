# django-cachex

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## What's in the box

A drop-in replacement for Django's built-in Redis cache, plus:

- One package for both Valkey and Redis, default and Sentinel and Cluster.
- Sync and async support sharing one configuration. The async cache works from sync code too.
- Hash, list, set, sorted set, and stream operations on the cache object.
- TTL and pattern helpers (`ttl()`, `expire()`, `keys()`, `delete_pattern()`).
- Distributed locks: `cache.lock()`.
- Lua scripting with automatic key prefixing and value encoding/decoding.
- Pluggable serializers (Pickle, JSON, MsgPack, ormsgpack, orjson) and compressors (Zlib, Gzip, LZ4, LZMA, Zstandard), each with fallback chains for safe migrations.
- Cache stampede prevention (TTL-based XFetch).
- Two composite backends: `SyncCache` (cross-pod stream-synchronized in-memory cache) and `TieredCache` (L1/L2 with TTL propagation).
- Django `LocMemCache` and `DatabaseCache` extensions with the same data-structure ops and admin support.
- Optional Rust I/O driver (PyO3 + tokio + redis-rs) behind the same `KeyValueCache` API. Free-threaded CPython (3.14t) supported.
- Django admin UI for browsing keys, inspecting values, editing, and flushing.

## Requirements

- Python 3.14+ (free-threaded supported)
- Django 6.0+
- valkey-py 6.1+ or redis-py 6.0+

## Quick Start

Install with pip:

```console
pip install django-cachex[valkey]
```

Configure as cache backend:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",       # or redis://...
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

The Rust I/O driver and async bridge are heavily inspired by, and in places directly adapted from, [django-vcache](https://gitlab.com/glitchtip/django-vcache) (MIT, by David Burke / GlitchTip). The fork-safe tokio runtime, the `RustAwaitable` deferred-loop-binding pattern, and the multiplexed-connection design all originate there.

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
