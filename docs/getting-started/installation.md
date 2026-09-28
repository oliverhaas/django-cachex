# Installation

## Requirements

- Python 3.14+. The free-threaded build (3.14t) is supported, with one caveat for the C parsers below.
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 7.2 to 8.x (`redis>=7.2,<9`)
- Valkey server 7.2+ or Redis server 6.2+. `set(get=True)` and the immediate-expiry conditional writes (`add()` and `set(nx=/xx=/get=)` with `timeout=0`) send `SET ... GET` and `SET ... PXAT`, both Redis 6.2 commands. `set(nx=True, get=True)` and `expiretime()` need Redis 7.0+. Hash field expiration needs Valkey 9.0+ or Redis 7.4+, and `hsetex`/`hgetex` Valkey 9.0+ or Redis 8.0+. Older servers raise `NotSupportedError` for those methods only.

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

Neither `hiredis` nor `libvalkey` declares free-threading support. On the
free-threaded build (3.14t), importing either re-enables the GIL for the
process and emits a `RuntimeWarning`. Use the pure-Python parser (the plain
`valkey-py` or `redis-py` extra) to keep the GIL off.

## Valkey-Glide adapter (optional)

!!! warning "Experimental"
    The valkey-glide adapter is experimental. Its interfaces and behavior can
    change, and it has less production testing than the redis-py and
    valkey-py backends.

`ValkeyGlideCache` wraps [valkey-glide], Valkey's official client with a
Rust core. The `valkey-glide` extra installs its two PyPI distributions,
`valkey-glide-sync` and `valkey-glide`:

```console
uv add django-cachex[valkey-glide]
```

The extra pins `valkey-glide-sync` and `valkey-glide` 2.5 to 2.x. The
adapter runs on the cp314 GIL build only, because valkey-glide publishes no
cp314t (free-threaded) wheels. It has a standalone backend
(`ValkeyGlideCache`) and a cluster backend (`ValkeyGlideClusterCache`). It
has no Sentinel backend, because `valkey-glide` does not ship a Sentinel
client. See [Configuration](../user-guide/configuration.md#valkey-glide)
for the setup.

[valkey-glide]: https://github.com/valkey-io/valkey-glide
