# Installation

## Requirements

- Python 3.12+
- Django 5.2+
- redis-py 6+
- Redis server 6+

## Install with pip

```console
pip install django-cachex-ng
```

## Install with hiredis (recommended)

For better performance, install with the hiredis parser:

```console
pip install django-cachex-ng[hiredis]
```

The hiredis package provides a C-based parser that can significantly improve performance when parsing Redis replies.

## Verify Installation

```python
>>> import django_cachex
>>> django_cachex.__version__
'6.0.0'
```
