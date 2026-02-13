# django-cachex

Full featured Valkey and Redis cache backend for Django with a built-in admin interface.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## Why django-cachex?

**Drop-in replacement** for Django's built-in Redis cache backend with extended features:

- **Built-in admin interface** - Browse, search, edit, and delete cache keys from Django admin
- **Unified Valkey and Redis support** - Single package for both backends
- **Extended data structures** - Hashes, lists, sets, sorted sets
- **Sync and async support** - All extended methods available in both sync and async
- **TTL and pattern operations** - `ttl()`, `expire()`, `keys()`, `delete_pattern()`
- **Lua script support** - Register and execute Lua scripts with automatic key prefixing
- **Distributed locking** - `cache.lock()` for cross-process synchronization
- **Sentinel and Cluster** - High availability and horizontal scaling
- **Pluggable serializers** - Pickle, JSON, MsgPack with fallback support
- **Pluggable compressors** - Zlib, Gzip, LZ4, LZMA, Zstandard with fallback support

## Requirements

- Python 3.12+
- Django 5.2+
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
    "django_cachex.admin",  # or "django_cachex.unfold" for django-unfold users
]
```

## Acknowledgments

This project was inspired by [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors is derived from django-redis, licensed under BSD-3-Clause. The admin functionality was inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard).

The Unfold theme integration optionally uses [django-unfold](https://unfoldadmin.com/).

See also [django-valkey](https://github.com/django-commons/django-valkey) and [dj-cache-panel](https://github.com/yassi/dj-cache-panel) for related projects with similar goals.

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
