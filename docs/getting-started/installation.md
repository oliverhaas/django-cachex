# Installation

## Requirements

- Python 3.14+, including the free-threaded build (3.14t)
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 7.2 to 8.x (`redis>=7.2,<9`)
- Valkey server 7.2+ or Redis server 6.2+. `set(nx=True, get=True)` and `expiretime()` need Redis 7.0+. Hash field expiration needs Valkey 9.0+ or Redis 7.4+, and `hsetex`/`hgetex` Valkey 9.0+ or Redis 8.0+. Older servers raise `NotSupportedError` for those methods only.

## Install with uv

The base package installs no client driver. Add the extra for your server:

```console
# For Valkey
uv add django-cachex[valkey-py]

# For Redis
uv add django-cachex[redis-py]
```

## Install with libvalkey/hiredis

The `libvalkey` (Valkey) and `hiredis` (Redis) extras add a C parser, which parses server replies faster than the pure-Python parser:

```console
# For Valkey
uv add django-cachex[libvalkey]

# For Redis
uv add django-cachex[hiredis]
```

On the free-threaded build, importing `libvalkey` or `hiredis` re-enables the
GIL for the process and emits a `RuntimeWarning`. Use the plain `valkey-py` or
`redis-py` extra to keep the GIL off.

## Valkey-Glide adapter (optional)

The experimental [valkey-glide](https://github.com/valkey-io/valkey-glide)
adapter needs the `valkey-glide` extra. It installs `valkey-glide-sync` and
`valkey-glide` 2.5 to 2.x:

```console
uv add django-cachex[valkey-glide]
```

The adapter runs on the cp314 GIL build only, because valkey-glide publishes no
free-threaded (cp314t) wheels. See
[Configuration](../user-guide/configuration.md#valkey-glide) for its backends
and setup.
