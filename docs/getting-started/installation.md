# Installation

## Requirements

- Python 3.14+ (free-threaded supported)
- Django 6.0+
- valkey-py 6.1+ or redis-py 6.0+
- Valkey server 7.2+ or Redis server 6.0+

## Install with uv

The base package pulls in no client driver; pick the extra that matches your setup:

```console
# For Valkey
uv add django-cachex[valkey-py]

# For Redis
uv add django-cachex[redis-py]
```

## Install with libvalkey/hiredis

For better performance, install with the libvalkey (for Valkey) or hiredis (for Redis) parser:

```console
# For Valkey
uv add django-cachex[libvalkey]

# For Redis
uv add django-cachex[hiredis]
```

These provide C-based parsers that improve protocol parsing throughput on the hot read path.

## Valkey-Glide adapter (optional)

!!! warning "Experimental"
    The valkey-glide adapter is experimental: interfaces and behavior may
    change, and it has seen less production testing than the
    redis-py/valkey-py paths.

The `ValkeyGlideCache` backend wraps Valkey's official client,
[valkey-glide]. It has a Rust core, packaged through PyPI as two
distributions, `valkey-glide-sync` and `valkey-glide`,
pulled in together via the `valkey-glide` extra:

```console
uv add django-cachex[valkey-glide]
```

cp314 GIL only; no cp314t (free-threaded) wheels are published yet.
Standalone (`ValkeyGlideCache`) and cluster (`ValkeyGlideClusterCache`)
backends are wired up; Sentinel is not exposed (`valkey-glide` itself does
not ship a Sentinel client). See the
[user-guide configuration page](../user-guide/configuration.md#valkey-glide)
for setup details.

[valkey-glide]: https://github.com/valkey-io/valkey-glide
