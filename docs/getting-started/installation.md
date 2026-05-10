# Installation

## Requirements

- Python 3.14+ (free-threaded supported)
- Django 6.0+
- valkey-py 6.1+ or redis-py 6.0+
- Valkey server 7+ or Redis server 6+

## Install with uv

```console
uv add django-cachex
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

## Rust I/O driver (optional)

The `RedisRsCache` backends are powered by an
opt-in native extension built on PyO3 + tokio + [redis-rs]. It ships as
a separate package, `django-cachex-redis-rs`, so users who only want the
pure-Python backends never carry the binary.

```console
# Pure Python (default; no Rust binary)
uv add django-cachex[valkey-py]

# With the Rust I/O driver
uv add django-cachex[valkey-py,redis-rs]
```

Prebuilt `django-cachex-redis-rs` wheels are published for Linux x86_64,
Linux aarch64, macOS arm64, and Windows amd64 on both cp314 and cp314t.
On other platforms pip will try to build from source, which needs the
Rust toolchain. Drop the `redis-rs` extra to avoid that.

When the binary isn't installed, `RedisRsCache`
classes are still importable but raise a clean `ImportError` on first
use, naming the extra you need.

[redis-rs]: https://github.com/redis-rs/redis-rs

## Valkey-Glide adapter (optional)

The `ValkeyGlideCache` backend wraps Valkey's official client,
[valkey-glide]. It's a Rust core (like `redis-rs`) but packaged separately
through PyPI as two distributions — `valkey-glide-sync` and `valkey-glide`
— pulled in together via the `valkey-glide` extra:

```console
uv add django-cachex[valkey-glide]
```

cp314 GIL only — no cp314t (free-threaded) wheels are published yet.
Standalone (`ValkeyGlideCache`) and cluster (`ValkeyGlideClusterCache`)
backends are wired up; Sentinel is not exposed (`valkey-glide` itself does
not ship a Sentinel client). See the
[user-guide configuration page](../user-guide/configuration.md#valkey-glide)
for setup details.

[valkey-glide]: https://github.com/valkey-io/valkey-glide
