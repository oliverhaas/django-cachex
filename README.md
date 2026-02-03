# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

Full featured Valkey and Redis cache backend for Django.

## Installation

```console
uv add django-cachex[valkey]

or

uv add django-cachex[redis]
```

## Quick Start

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache", # or django_cachex.cache.RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1", # or redis://127.0.0.1:6379/1
    }
}
```

## Features

- **Unified Valkey and Redis support** - Single package for both backends
- **Async support** - Async versions of all extended methods
- **Drop-in Django cache backend** - Easy migration
- **Extended data structures** - Hashes, lists, sets, sorted sets, and streams
- **TTL and pattern operations** - `ttl()`, `expire()`, `keys()`, `delete_pattern()`
- **Distributed locking** - `cache.lock()` for cross-process synchronization
- **Sentinel and Cluster** - High availability and horizontal scaling
- **Serializer/compressor fallback** - Safe migrations between formats


## Documentation

Full documentation at [oliverhaas.github.io/django-cachex](https://oliverhaas.github.io/django-cachex/)

## Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.0+ or redis-py 6.0+

## Acknowledgments

This project was initially based on [django-redis](https://github.com/jazzband/django-redis) and then Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors are still officially derived from django-redis, licensed under BSD-3-Clause.

The admin functionality was mostly inspired by [django-redisboard](https://github.com/ionelmc/django-redisboard), and to a lesser extend [dj-cache-panel](https://github.com/yassi/dj-cache-panel). The Unfold theme integration uses [django-unfold](https://unfoldadmin.com/).

AI tools were used extensively during the initial development of this package. Almost all AI code in this package has been reviewed, then usually rewritten and refactored at a later point. I do not consider any part of this package "vibe-coded" or similar, especially critical components like the cache backends themselves are of a higher standard regarding AI use than less critical components like the admin functionality.

Also, major thanks to the Django community for their continued work on the framework.

## License

MIT
