# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

Cache extensions for django, including full featured Valkey and Redis cache backend for Django and a built-in admin interface.

## Installation

```console
pip install django-cachex[valkey]
# or
pip install django-cachex[redis]
```

## Quick Start

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",       # or redis://...
    }
}
```

## Features

- **Full-featured cache backends** for Valkey and Redis with async support, extended data structures, distributed locking, Lua scripting, and more
- **Built-in admin interface** for browsing, searching, and managing cache keys directly from Django admin
- **Drop-in replacement** for Django's built-in Redis backend

## Cache Backends

- **Unified Valkey and Redis support** - Single package for both backends
- **Async support** - Async versions of all extended methods
- **Mixing sync & async support** - Async cache still works in sync code
- **Extended data structures** - Hashes, lists, sets, sorted sets
- **TTL and pattern operations** - `ttl()`, `expire()`, `keys()`, `delete_pattern()`
- **Lua script support** - Register and execute Lua scripts with automatic key prefixing
- **Distributed locking** - `cache.lock()` for cross-process synchronization
- **Sentinel and Cluster** - High availability and horizontal scaling
- **Pluggable serializers** - Pickle, JSON, MsgPack with fallback support
- **Pluggable compressors** - Zlib, Gzip, LZ4, LZMA, Zstandard with fallback support

## Cache Admin

Add `django_cachex.admin` to your `INSTALLED_APPS` to enable the cache admin interface:

```python
INSTALLED_APPS = [
    # ...
    "django_cachex.admin",
]
```

Browse all configured caches, search and filter keys by type, and manage values directly:

![Cache list](docs/assets/screenshot-cache-list.png)
![Key list](docs/assets/screenshot-key-list.png)
![Key detail](docs/assets/screenshot-key-detail.png)

Features:
- Browse all configured cache backends (Valkey, Redis, LocMemCache, DatabaseCache, and more)
- Search keys with wildcard patterns (`user:*`, `*:session`)
- Filter by key type (string, list, set, hash, zset, stream)
- View and edit values with type-specific operations
- Inspect and modify TTL
- View server info and memory statistics
- Flush caches

For [django-unfold](https://github.com/unfoldadmin/django-unfold) users, use `django_cachex.unfold` instead for a themed interface.

## Documentation

Full documentation at [oliverhaas.github.io/django-cachex](https://oliverhaas.github.io/django-cachex/)

## Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.0+ or redis-py 6.0+

## Acknowledgments

This project was inspired by [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors is derived from django-redis, licensed under BSD-3-Clause. The admin functionality was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard). All of the aboved I used in production, noticed some flaws over the years, and for one reason or another a new package ended up the best way for progress for me here.

The Unfold theme integration optionally uses [django-unfold](https://unfoldadmin.com/). 

I also want to mention [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel), which I never really used, but are newer and interesting efforts of similar goals as this package has.

## License

MIT
