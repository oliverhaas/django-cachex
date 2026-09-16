# Installation

## Requirements

- Python 3.14+. The free-threaded build (3.14t) is supported, with one caveat for the C parsers below.
- Django 6.0 to 6.x (`Django>=6,<7`)
- valkey-py 6.1 to 6.x (`valkey>=6.1,<7`) or redis-py 6.0 to 8.x (`redis>=6,<9`)
- Valkey server 7.2+ or Redis server 6.2+. `set(get=True)` and the immediate-expiry conditional writes (`add()` and `set(nx=/xx=/get=)` with `timeout=0`) send `SET ... GET` and `SET ... PXAT`, both Redis 6.2 commands; `set(nx=True, get=True)` needs Redis 7.0+. Hash field expiration needs Valkey 9.0+ or Redis 7.4+, and `hsetex`/`hgetex` Redis 8.0+; older servers raise `NotSupportedError` for those methods only.

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

Neither `hiredis` nor `libvalkey` declares free-threading support, so on the
free-threaded build (3.14t) importing either re-enables the GIL for the
process, with a `RuntimeWarning` at import. Use the pure-Python parser
(the plain `valkey-py` / `redis-py` extras) to keep the GIL off.

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

The extra pins `valkey-glide-sync` and `valkey-glide` 2.5 to 2.x. cp314 GIL
only; no cp314t (free-threaded) wheels are published yet.
Standalone (`ValkeyGlideCache`) and cluster (`ValkeyGlideClusterCache`)
backends are wired up; Sentinel is not exposed (`valkey-glide` itself does
not ship a Sentinel client). See the
[user-guide configuration page](../user-guide/configuration.md#valkey-glide)
for setup details.

[valkey-glide]: https://github.com/valkey-io/valkey-glide
