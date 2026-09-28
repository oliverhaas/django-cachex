# django-cachex

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## What's in the box

A drop-in replacement for Django's built-in Redis cache, plus:

- One package for Valkey and Redis, standalone, Sentinel and Cluster.
- Sync and async methods on every cache, such as `get()` and `aget()`, from one alias and one configuration.
- Hash, list, set, sorted set and stream operations on the cache object.
- TTL and pattern helpers (`ttl()`, `expire()`, `keys()`, `delete_pattern()`).
- Distributed locks with `cache.lock()`.
- Counting and weighted semaphores with `cache.semaphore()`, in-process or distributed, to cap concurrent work at a budget.
- Lua scripting with `eval_script()`, with optional hooks for key prefixing and value encoding and decoding.
- Pluggable serializers (Pickle, JSON, MsgPack, ormsgpack, orjson) and compressors (Zlib, Gzip, LZ4, LZMA, Zstandard), each with a fallback chain to migrate between formats.
- Cache stampede prevention (TTL-based XFetch).
- `TrackingCache`, a local read cache over a Redis or Valkey alias, invalidated by the server's `CLIENT TRACKING` or bounded by a local TTL.
- An opt-in [ORM cache](user-guide/orm-cache.md), `django_cachex.orm`, derived from django-cachalot. It caches ORM query results per table and invalidates them on every write. Write leases keep a write from leaving a stale result behind.
- `LocMemCache` and `DatabaseCache` extensions with the hash, list, set and sorted set operations, `ttl()`/`expire()`/`persist()`, key patterns and admin support. They have no streams, locks, pipelines or Lua.
- An experimental `ValkeyGlideCache` backend on `valkey-glide`, Valkey's official client with a Rust core.
- A Django admin UI to browse keys, inspect and edit values, and flush caches.

## Requirements

- Python 3.14+. The free-threaded build (3.14t) is supported, with one
  caveat: `hiredis` and `libvalkey` are C extensions without free-threading
  support, so importing either on 3.14t re-enables the GIL with a
  `RuntimeWarning`. Run with `PYTHON_GIL=0` (or `-Xgil=0`) to keep it
  disabled; that is how the CI 3.14t job runs the suite.
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 7.2 to 8.x (`redis>=7.2,<9`)
- Valkey 7.2+ or Redis 6.2+ on the server. `set(get=True)` and the
  immediate-expiry conditional writes (`add()` and `set(nx=/xx=/get=)` with
  `timeout=0`) send `SET ... GET` and `SET ... PXAT`, both Redis 6.2 commands.
  `set(nx=True, get=True)` and `expiretime()` need Redis 7.0+
- Hash field expiration needs Valkey 9.0+ or Redis 7.4+, and `hsetex`/`hgetex`
  Valkey 9.0+ or Redis 8.0+. Older servers raise `NotSupportedError` for those
  methods

The `valkey-glide` adapter is optional and experimental. Its interfaces and
behavior can change, and it has less production testing than the redis-py
and valkey-py backends. The `valkey-glide` extra
(`pip install django-cachex[valkey-glide]`) enables `ValkeyGlideCache` and
installs `valkey-glide-sync` and `valkey-glide` (2.5 to 2.x), Valkey's
official client with a Rust core. The adapter needs the cp314 GIL build,
because `valkey-glide` publishes no free-threaded wheels.
`ValkeyGlideClusterCache` supports Cluster. The adapter has no Sentinel
backend, because `valkey-glide` does not ship a Sentinel client.

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

This project started from [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some serializer, compressor and exception code is derived from django-redis, licensed under BSD-3-Clause. The admin UI was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard). The ORM cache (`django_cachex.orm`) is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1 by Bertrand Bordage, licensed under BSD-3-Clause.

The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip), so the numbers are directly comparable.

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT, see [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE), except for two parts under BSD-3-Clause
(the package metadata declares `MIT AND BSD-3-Clause`):

- The ORM cache, `django_cachex/orm/`, derived from django-cachalot. See
  [django_cachex/orm/LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/django_cachex/orm/LICENSE).
- Parts of `django_cachex/serializers/`, `django_cachex/compressors/` and
  `django_cachex/exceptions.py`, derived from django-redis (Copyright (c)
  2011-2016 Andrey Antukh). Each derived file says so in its header. See
  [LICENSE.django-redis](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE.django-redis).
