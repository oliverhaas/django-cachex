# django-cachex

Valkey and Redis cache backend for Django, with a Django admin UI for cache inspection.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## Features

A drop-in replacement for Django's built-in Redis cache, plus:

- One package for Valkey and Redis, standalone, [Sentinel](user-guide/sentinel.md) and [Cluster](user-guide/cluster.md).
- Sync and [async](user-guide/async.md) methods on every cache (`get()` and `aget()`), from one alias and one configuration.
- Hash, list, set, sorted set and stream operations, and TTL and pattern helpers (`ttl()`, `expire()`, `keys()`, `delete_pattern()`).
- Distributed locks with `cache.lock()`.
- Counting and weighted semaphores with `cache.semaphore()`, in-process or distributed.
- Lua scripting with `eval_script()`, with optional key-prefixing and encoding hooks.
- Pluggable [serializers](user-guide/serializers.md) and [compressors](user-guide/compression.md), each with a fallback chain to migrate between formats.
- Cache stampede prevention (TTL-based XFetch).
- [`TrackingCache`](user-guide/composite-backends.md), a local read cache over a Redis or Valkey alias, invalidated by the server's `CLIENT TRACKING` or bounded by a local TTL.
- An opt-in [ORM cache](user-guide/orm-cache.md), `django_cachex.orm`, that caches ORM query results per table and invalidates them on every write.
- `LocMemCache` and `DatabaseCache` extensions with the hash, list, set and sorted set operations, `ttl()`/`expire()`/`persist()`, key patterns and admin support, but no streams, locks, pipelines or Lua.
- Experimental `ValkeyGlideCache` and `ValkeyGlideClusterCache` backends on `valkey-glide`, Valkey's official client with a Rust core.
- A Django [admin UI](user-guide/admin.md) to browse keys, inspect and edit values, and flush caches.

## Requirements

- Python 3.14+, including the free-threaded build (3.14t). On 3.14t, importing `hiredis` or `libvalkey` re-enables the GIL with a `RuntimeWarning`, unless you run with `PYTHON_GIL=0` (or `-Xgil=0`).
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 7.2 to 8.x (`redis>=7.2,<9`)
- Valkey 7.2+ or Redis 6.2+ on the server

[Installation](getting-started/installation.md) lists the methods that need a newer server and the requirements of the glide backends.

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

INSTALLED_APPS = [
    # ...
    "django_cachex.admin",  # optional cache admin
]
```

## Acknowledgments

This project started from [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). The admin UI was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard). The ORM cache is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1 by Bertrand Bordage. The ASGI benchmark follows the shape of [django-vcache](https://gitlab.com/glitchtip/django-vcache)'s `bench_compare.py` (MIT, by David Burke / GlitchTip).

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT, see [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE), except for two parts under BSD-3-Clause:

- The ORM cache, `django_cachex/orm/`, derived from django-cachalot. See
  [django_cachex/orm/LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/django_cachex/orm/LICENSE).
- Parts of the serializers, compressors and exceptions, derived from django-redis
  (Copyright (c) 2011-2016 Andrey Antukh). See
  [LICENSE.django-redis](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE.django-redis).
