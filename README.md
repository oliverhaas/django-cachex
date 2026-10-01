# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.
Full documentation at [oliverhaas.github.io/django-cachex](https://oliverhaas.github.io/django-cachex/).

## Quick Start

```console
pip install django-cachex[valkey-py]
```

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    }
}
```

## Features

- One package for Valkey and Redis, standalone, Sentinel and Cluster.
- Sync and async methods on every cache (`get()` and `aget()`), from one alias and one configuration.
- Hash, list, set, sorted set and stream operations, and TTL and pattern helpers (`ttl()`, `expire()`, `keys()`, `delete_pattern()`).
- Distributed locks with `cache.lock()`.
- Counting and weighted semaphores with `cache.semaphore()`, in-process or distributed.
- Lua scripting with `eval_script()`, with optional key-prefixing and encoding hooks.
- Pluggable serializers (Pickle, JSON, MsgPack, ormsgpack, orjson) and compressors (Zlib, Gzip, LZ4, LZMA, Zstandard), each with a fallback chain to migrate between formats.
- Cache stampede prevention (TTL-based XFetch).
- `TrackingCache`, a local read cache over a Redis or Valkey alias, invalidated by the server's `CLIENT TRACKING` or bounded by a local TTL.
- An opt-in [ORM cache](https://oliverhaas.github.io/django-cachex/latest/user-guide/orm-cache/), `django_cachex.orm`, that caches ORM query results per table and invalidates them on every write.
- `LocMemCache` and `DatabaseCache` extensions with the hash, list, set and sorted set operations, `ttl()`/`expire()`/`persist()`, key patterns and admin support, but no streams, locks, pipelines or Lua.
- Experimental `ValkeyGlideCache` and `ValkeyGlideClusterCache` backends on `valkey-glide`, Valkey's official client with a Rust core.

## Cache Admin

Add `django_cachex.admin` to `INSTALLED_APPS` to enable the cache admin:

```python
INSTALLED_APPS = [
    # ...
    "django_cachex.admin",
]
```

The admin lists every configured cache, finds keys by wildcard pattern and type, and edits values and TTLs. It also shows server info and memory statistics, and with `CACHEX_ADMIN = {"ALLOW_FLUSH": True}` it flushes caches.

![Cache list](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-cache-list.png)
![Key list](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-key-list.png)
![Key detail](https://raw.githubusercontent.com/oliverhaas/django-cachex/main/docs/assets/screenshot-key-detail.png)

## Requirements

- Python 3.14+, including the free-threaded build (3.14t). On 3.14t, importing `hiredis` or `libvalkey` re-enables the GIL with a `RuntimeWarning`, unless you run with `PYTHON_GIL=0` (or `-Xgil=0`).
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 7.2 to 8.x (`redis>=7.2,<9`)
- Valkey 7.2+ or Redis 6.2+ on the server. [Installation](https://oliverhaas.github.io/django-cachex/latest/getting-started/installation/) lists the methods that need a newer server.
- `valkey-glide` 2.5 to 2.x (the `valkey-glide` extra) for the glide backends. They need the cp314 GIL build and have no Sentinel variant.

## Acknowledgments

This project started from [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). The admin UI was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard). The ORM cache is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1 by Bertrand Bordage. The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip).

I also want to mention [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel), which I never really used, but are newer and interesting efforts of similar goals as this package has.

## License

MIT, see [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE), except for two parts under BSD-3-Clause:

- The ORM cache, `django_cachex/orm/`, derived from django-cachalot. See
  [django_cachex/orm/LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/django_cachex/orm/LICENSE).
- Parts of `django_cachex/serializers/`, `django_cachex/compressors/` and
  `django_cachex/exceptions.py`, derived from django-redis (Copyright (c)
  2011-2016 Andrey Antukh). Each derived file says so in its header. See
  [LICENSE.django-redis](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE.django-redis).
