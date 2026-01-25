# Django Redis NG

[![PyPI version](https://img.shields.io/pypi/v/django-cachex-ng.svg?style=flat)](https://pypi.org/project/django-cachex-ng/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-cachex-ng.svg)](https://pypi.org/project/django-cachex-ng/)
[![CI](https://github.com/oliverhaas/django-cachex-ng/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/django-cachex-ng/actions/workflows/ci.yml)

Full featured Redis cache and session backend for Django (Next Generation fork).

## Installation

```console
pip install django-cachex-ng
```

## Quick Start

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        }
    }
}
```

## Features

- Native redis-py URL connection strings
- Pluggable clients, serializers, and compressors
- Compressor fallback for safe migration between formats
- Primary/replica replication support
- Redis Sentinel and Cluster support
- Distributed locks
- Comprehensive test suite

## Documentation

Full documentation at [oliverhaas.github.io/django-cachex-ng](https://oliverhaas.github.io/django-cachex-ng/)

## About This Fork

django-cachex-ng is a "Next Generation" fork of [django-cachex](https://github.com/jazzband/django-cachex). It maintains API compatibility while enabling faster iteration on improvements. The goal is to merge changes upstream when possible.

## Requirements

- Python 3.9+
- Django 4.2+
- redis-py 4.0.2+

## License

BSD-3-Clause
