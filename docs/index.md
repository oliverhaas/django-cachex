# django-cachex

Full featured Valkey and Redis cache backend for Django.

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/django-cachex.svg)](https://pypi.org/project/django-cachex/)

## Why django-cachex?

**Drop-in replacement** for Django's built-in Redis cache backend (`django.core.cache.backends.redis`) with extended features:

- Uses native valkey-py/redis-py URL notation connection strings
- Pluggable clients
- Pluggable serializers
- Pluggable compressors
- Primary/replica support in the default client
- Valkey/Redis Sentinel and Cluster support
- Comprehensive test suite
- Used in production as cache and session storage
- Supports infinite timeouts
- Facilities for raw access to Valkey/Redis client/connection pool
- Highly configurable (can emulate memcached exception behavior, for example)
- Unix sockets supported by default

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

## License

MIT License. See [LICENSE](https://github.com/oliverhaas/django-cachex/blob/main/LICENSE) for details.
