# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

Full featured Valkey and Redis cache backend for Django with a built-in admin interface.

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

- **Built-in admin interface** - Browse, search, edit, and delete cache keys directly from Django admin
- **Unified Valkey and Redis support** - Single package for both backends
- **Async support** - Async versions of all extended methods
- **Drop-in replacement** - Easy migration from Django's built-in Redis backend
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

Features:
- Browse all configured cache backends (Valkey, Redis, LocMemCache, DatabaseCache, and more)
- Search keys with wildcard patterns (`user:*`, `*:session`)
- View and edit values (strings, hashes, lists, sets, sorted sets)
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

This project was initially based on [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors is derived from django-redis, licensed under BSD-3-Clause.

The admin functionality was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard). The Unfold theme integration uses [django-unfold](https://unfoldadmin.com/).

## License

MIT
