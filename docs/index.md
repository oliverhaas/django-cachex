# django-cachex

Full featured Valkey and Redis cache backend for Django.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## Why django-cachex?

**Drop-in replacement** for Django's built-in Redis cache backend (`django.core.cache.backends.redis`) with extended features:

- Uses native valkey-py/redis-py
- Extended functionality of all typical commands for hashes, sets, lists, sorted sets, and streams
- Pluggable serializers
- Pluggable compressors
- Valkey/Redis Sentinel and Cluster support
- Primary/replica support
- Comprehensive test suite
- Facilities for raw access to Valkey/Redis client/connection pool
- Highly configurable

## Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.0+ or redis-py 6.0+

## Quick Start

Install with pip:

```console
pip install django-cachex
```

Configure as cache backend:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache", # or django_cachex.cache.RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1", # or redis://127.0.0.1:6379/1
    }
}
```

## Acknowledgments

This project was inspired by [django-redis](https://github.com/jazzband/django-redis) and Django's official [Redis cache backend](https://docs.djangoproject.com/en/stable/topics/cache/#redis). Some utility code for serializers and compressors is derived from django-redis, licensed under BSD-3-Clause. Thanks to the Django community for their continued work on the framework.

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
