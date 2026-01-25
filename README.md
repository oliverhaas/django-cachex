# django-cachex

[![PyPI version](https://img.shields.io/pypi/v/django-cachex.svg?style=flat)](https://pypi.org/project/django-cachex/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex.svg)](https://pypi.org/project/django-cachex/)
[![CI](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex/actions/workflows/ci.yml)

A Redis cache backend for Django with extended features.

## Installation

```console
pip install django-cachex
```

## Quick Start

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
    }
}
```

## Features

- Native redis-py URL connection strings
- Pluggable clients, serializers, and compressors
- Primary/replica replication support
- Redis Sentinel and Cluster support
- Extended data structure operations (hashes, lists, sets, sorted sets)
- Distributed locks
- Comprehensive test suite

## Documentation

Full documentation at [oliverhaas.github.io/django-cachex](https://oliverhaas.github.io/django-cachex/)

## Requirements

- Python 3.10+
- Django 4.2+
- redis-py 5.0+

## License

BSD-3-Clause
