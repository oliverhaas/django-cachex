# Installation

## Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.1+ or redis-py 6+
- Valkey server 7+ or Redis server 6+

## Install with uv

```console
uv add django-cachex
```

## Install with libvalkey/hiredis (recommended)

For better performance, install with the libvalkey (for Valkey) or hiredis (for Redis) parser:

```console
# For Valkey
uv add django-cachex[libvalkey]

# For Redis
uv add django-cachex[hiredis]
```

These provide C-based parsers that significantly improve performance.
