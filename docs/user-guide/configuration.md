# Configuration Reference

Reference for all django-cachex configuration options.

## Basic Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,  # Default timeout in seconds
        "KEY_PREFIX": "myapp",  # Prefix for all keys
        "VERSION": 1,  # Key version number
        "OPTIONS": {
            # See options below
        },
    }
}
```

## Backend Classes

All backends live in `django_cachex.cache`.

### Valkey / Redis (Python driver)

| Backend | Description |
|---------|-------------|
| `ValkeyCache` | Standard Valkey connection |
| `RedisCache` | Standard Redis connection |
| `ValkeySentinelCache` | Valkey Sentinel high availability |
| `RedisSentinelCache` | Redis Sentinel high availability |
| `ValkeyClusterCache` | Valkey Cluster sharding |
| `RedisClusterCache` | Redis Cluster sharding |

### Valkey-Glide

!!! warning "Experimental"
    The valkey-glide adapter is experimental: interfaces and behavior may
    change, and it has seen less production testing than the
    redis-py/valkey-py backends.

Valkey's official client library, with a Rust core. It ships as two PyPI
distributions, `valkey-glide-sync` and `valkey-glide`, both pulled in by the
`valkey-glide` extra. cachex uses the sync distribution's `glide_sync.GlideClient`
for sync calls and the async distribution's `glide.GlideClient` for the `a*`
methods, so one cache alias holds up to one client of each kind.

| Backend | Description |
|---------|-------------|
| `ValkeyGlideCache` | Standard Valkey connection via valkey-glide |
| `ValkeyGlideClusterCache` | Cluster sharding via valkey-glide |

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyGlideCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
    }
}
```

See the upstream [valkey-glide](https://github.com/valkey-io/valkey-glide) docs for client-specific tuning. Sentinel is not exposed (`valkey-glide` itself does not ship a Sentinel client).

### Local backends

| Backend | Description |
|---------|-------------|
| `LocMemCache` | Drop-in replacement for Django's `LocMemCache` with data-structure ops, `ttl()`/`expire()`/`persist()`, and admin support |
| `DatabaseCache` | Drop-in replacement for Django's `DatabaseCache` with the same extensions |

The TTL surface is `ttl()`, `expire()` and `persist()`, and `ttl()` reports
whole seconds. `pttl()`, `pexpire()`, `expireat()`,
`pexpireat()`, `expiretime()` and the hash-field expiration family
(`hexpire()`, `httl()`, `hsetex()`, `hgetex()` and their relatives) raise
`NotSupportedError`, and so do `lock()`, `pipeline()`, `eval_script()`,
`get_client()`, `rename()`, `renamenx()`, `slowlog_get()`, `slowlog_len()`,
the blocking list pops, and the cross-key store commands (`lmove()`,
`smove()`, `sinterstore()` and friends). Streams are not implemented at all.

What does work on both: the hash, list, set and sorted-set commands, `type()`,
`touch()`, `info()`, key listing (`keys()`, `iter_keys()`, `scan()`,
`delete_pattern()`) and the admin. Key patterns use Redis's glob dialect on
both, and `DatabaseCache` matches case-sensitively on every database vendor.
`LocMemCache` also has `semaphore()`, backed by the in-process
`django_cachex.Semaphore`; `DatabaseCache` has no semaphore.

`incr_version()` and `decr_version()` move the key rather than copying it, the
way Redis `RENAME` does: any key type moves, collections included, and the key
keeps its remaining TTL.

On MySQL, run the connection at `READ COMMITTED`, which is also Django's own
recommendation for `DatabaseCache`. The compound operations (`lpush()`,
`sadd()`, `hincrby()` and the rest) take a `SELECT ... FOR UPDATE` row lock,
and under the InnoDB default of `REPEATABLE READ` that lock takes a gap lock on
a row that does not exist yet, so two clients creating the same key at the same
time deadlock and one gets an `OperationalError` (MySQL error 1213) instead of
falling through to the insert retry.

### Composite backends

| Backend | Description |
|---------|-------------|
| `StreamCache` | In-memory store synchronized across pods via a Redis Stream consumer |
| `TrackingCache` | Read-through local cache over a Redis/Valkey alias, invalidated by the server's `CLIENT TRACKING` |

!!! note "Valkey and Redis Compatibility"
    Valkey and Redis are protocol-compatible, so either backend works with either server. Valkey is recommended as it remains fully open source.

### Shared base classes

`django_cachex.cache` also exports `RespCache`, `RespClusterCache` and
`RespSentinelCache`. They hold the shared implementation that the Valkey/Redis
backends above inherit. They bind no driver, so naming one as `BACKEND` raises
`ImproperlyConfigured` on the first operation; they exist for subclassing and
typing. Read a mention of
them elsewhere as shorthand for "every Valkey/Redis backend".

## LOCATION

Server URL(s):

```python
# Single server (Valkey)
"LOCATION": "valkey://127.0.0.1:6379/1"

# Single server (Redis)
"LOCATION": "redis://127.0.0.1:6379/1"

# With authentication
"LOCATION": "valkey://user:password@127.0.0.1:6379/1"

# SSL/TLS
"LOCATION": "valkeys://127.0.0.1:6379/1"  # or rediss://

# Unix socket
"LOCATION": "unix:///path/to/socket?db=1"

# Multiple servers (read replicas)
"LOCATION": [
    "valkey://127.0.0.1:6379/1",  # Primary (writes)
    "valkey://127.0.0.1:6380/1",  # Replica (reads)
]

# Or comma/semicolon separated
"LOCATION": "valkey://127.0.0.1:6379/1,valkey://127.0.0.1:6380/1"
```

The multi-URL form works on the valkey-glide backends too. There the extra URLs
become replica node addresses and the client is built with
`read_from=PREFER_REPLICA`, so reads go to a replica when one is reachable and
fall back to the primary otherwise. Two constraints come with it:

- Every URL in the list must agree on TLS scheme, username, password and
  database, because glide applies one connection setting to the whole address
  list. A mismatch raises `ImproperlyConfigured` when the backend first
  connects.
- Listing the same URL twice does not add a replica: duplicate host/port
  entries collapse to a single address.

## OPTIONS Reference

Which keys are honored depends on the backend:

| Keys | Honored by |
|------|------------|
| `serializer`, `compressor`, `stampede_prevention`, `username`, `password` | Every Valkey/Redis backend, valkey-glide included |
| `pool_class`, `async_pool_class`, `parser_class`, `sentinels`, `sentinel_kwargs`, plus everything forwarded to the driver's `from_url()` (`socket_timeout`, `socket_connect_timeout`, `retry_on_timeout`, `ssl_*`, `db`, ...) | redis-py and valkey-py backends |
| `db`, `use_tls` / `ssl`, `request_timeout`, `client_name` | valkey-glide backends, see [Valkey-Glide OPTIONS](#valkey-glide-options) |

`db` appears in both driver rows because each reads it its own way: redis-py and
valkey-py hand it to `from_url()`, valkey-glide turns it into a `database_id` on
the client config.

The valkey-glide adapter builds its client configuration from that short list and
ignores every other `OPTIONS` key without warning, so a `socket_timeout` or
`pool_class` copied from a valkey-py alias has no effect there.

### Serialization

```python
"OPTIONS": {
    # Single serializer (string path, class, or instance)
    "serializer": "django_cachex.serializers.pickle.PickleSerializer",

    # Or with fallback for migration
    "serializer": [
        "django_cachex.serializers.msgpack.MsgpackSerializer",  # Write
        "django_cachex.serializers.pickle.PickleSerializer",    # Fallback read
    ],
}
```

Available serializers:

| Serializer | Description |
|------------|-------------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default) |
| `django_cachex.serializers.json.JsonSerializer` | JSON via `DjangoJSONEncoder` |
| `django_cachex.serializers.msgpack.MsgpackSerializer` | MessagePack (requires `msgpack`) |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON (requires `orjson`) |
| `django_cachex.serializers.ormsgpack.OrmsgpackSerializer` | Rust-backed MessagePack (requires `ormsgpack`) |

See [Serializers](serializers.md) for type-compatibility details and benchmarks.

### Compression

```python
"OPTIONS": {
    # Single compressor
    "compressor": "django_cachex.compressors.zstd.ZstdCompressor",

    # Or with fallback for migration
    "compressor": [
        "django_cachex.compressors.zstd.ZstdCompressor",  # Write
        "django_cachex.compressors.zlib.ZlibCompressor",  # Fallback read
    ],
}
```

Available compressors:

| Compressor | Description |
|------------|-------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | zlib (stdlib) |
| `django_cachex.compressors.gzip.GzipCompressor` | gzip (stdlib) |
| `django_cachex.compressors.lzma.LzmaCompressor` | LZMA (stdlib) |
| `django_cachex.compressors.zstd.ZstdCompressor` | Zstandard (stdlib on 3.14+) |
| `django_cachex.compressors.lz4.Lz4Compressor` | LZ4 (requires `lz4`) |

Compression is only applied to values larger than `min_length` bytes (default: 256).

### Connection Pool

Applies to the redis-py and valkey-py backends. valkey-glide manages its own
connections and reads none of these keys.

```python
"OPTIONS": {
    # Custom pool class (use valkey.ConnectionPool for Valkey)
    "pool_class": "valkey.ConnectionPool",

    # Custom async pool class, used by the a* methods
    "async_pool_class": "valkey.asyncio.ConnectionPool",

    "retry_on_timeout": True,

    # Socket timeouts
    "socket_connect_timeout": 5,
    "socket_timeout": 5,
}
```

`pool_class` and `async_pool_class` take a dotted path or a class; each defaults
to the driver's own sync or async `ConnectionPool`. See
[Async support](async.md#custom-async-pool-class) for the async pool.

On a Sentinel backend, `pool_class` picks the Sentinel-managed pool and so must
be `SentinelConnectionPool` or a subclass of it; anything else raises
`ImproperlyConfigured` at startup, because a plain connection pool takes none of
the primary/replica discovery arguments.

Extra keys you add are forwarded to the underlying pool's `from_url(...)`, so you
can pin driver-specific options (`socket_keepalive`, `health_check_interval`, etc.)
the same way. Seven keys are handled by cachex instead of being forwarded:
`pool_class`, `async_pool_class`, `serializer`, `compressor`,
`stampede_prevention`, `sentinels` and `sentinel_kwargs`. `parser_class` is
resolved to a class first and then passed to the pool.

### Parser

```python
"OPTIONS": {
    # Dotted path or class; defaults to the driver's DefaultParser
    "parser_class": "valkey.connection.DefaultParser",  # or "redis.connection.DefaultParser"
}
```

Also redis-py and valkey-py only. You rarely need to set this. When omitted, the driver's `DefaultParser`
is used, which resolves to the C-accelerated parser when `libvalkey`
(Valkey) or `hiredis` (Redis) is installed and to the pure-Python RESP
parser otherwise. To get the C parser, install the `libvalkey` or
`hiredis` extra; no `parser_class` setting is required.

### Cache stampede prevention

Probabilistic early recompute (XFetch) to avoid thundering-herd recompute when a hot key expires:

```python
"OPTIONS": {
    # Enable with defaults (buffer=60s, beta=1.0, delta=1.0)
    "stampede_prevention": True,

    # Or tune individually
    "stampede_prevention": {
        "buffer": 30,   # extra TTL added to writes; recompute window inside this buffer
        "beta": 1.0,    # higher = more aggressive early recompute
        "delta": 1.0,   # estimated recompute cost (seconds)
    },
}
```

`buffer` is a non-negative `int` (seconds); `beta` and `delta` are finite
non-negative numbers. Zero for `beta` or `delta` is valid and means "no
probabilistic early recompute, the key expires logically at its timeout". An
out-of-range value raises `TypeError` or `ValueError` when the cache is
configured, not later on a write.

Per-call overrides accept the same shapes via the `stampede_prevention=` keyword on `get`/`set`/`add`/`touch`/`get_or_set`/`get_many`/`set_many`, on the TTL readers and setters (`ttl`, `pttl`, `expire`, `expireat`, `pexpire`, `pexpireat`, `expiretime`), and on their `a`-prefixed async counterparts. On `touch` the keyword decides whether the refreshed TTL gets the buffer added back, so it should match what the original write used.

!!! warning "Valkey/Redis backends only"
    Stampede prevention is implemented in the RESP cache layer and the
    valkey-py and valkey-glide adapters. `LocMemCache`, `DatabaseCache`
    and `StreamCache` ignore both `OPTIONS["stampede_prevention"]` and the
    per-call keyword. `TrackingCache` follows its transport's setting.

### Valkey-Glide OPTIONS

The valkey-glide adapter builds a `GlideClientConfiguration` rather than a
connection pool, so it reads its own short set of keys:

```python
"OPTIONS": {
    "db": 2,                  # database index, standalone only
    "use_tls": True,          # "ssl" is accepted as an alias
    "username": "app",
    "password": "secret",
    "request_timeout": 250,   # milliseconds
    "client_name": "web-1",
}
```

| Option | Description |
|--------|-------------|
| `db` | Database index. Beats a `?db=` query, which beats the URL path. Ignored by `ValkeyGlideClusterCache`, since cluster only serves db 0. |
| `use_tls` / `ssl` | Force TLS on or off. Without either key, TLS follows the `rediss://` or `valkeys://` scheme. `use_tls` is read first. |
| `username` / `password` | ACL credentials. Either one present builds a `ServerCredentials`; `OPTIONS` wins over the URL. |
| `request_timeout` | Per-request timeout. Coerced to `int` and passed through as glide's own `request_timeout`, which glide reads as milliseconds. |
| `client_name` | Name reported to the server, visible in `CLIENT LIST`. |

`serializer`, `compressor` and `stampede_prevention` also apply, since those are
handled above the adapter. Every other key is ignored.

### Choosing an adapter

The adapter (the layer that talks to the underlying client lib) is
selected by your `BACKEND`. Each cache class has a fixed adapter:

| Backend                                         | Adapter      |
|-------------------------------------------------|--------------|
| `django_cachex.cache.RedisCache`                | redis-py     |
| `django_cachex.cache.RedisSentinelCache`        | redis-py     |
| `django_cachex.cache.RedisClusterCache`         | redis-py     |
| `django_cachex.cache.ValkeyCache`               | valkey-py    |
| `django_cachex.cache.ValkeySentinelCache`       | valkey-py    |
| `django_cachex.cache.ValkeyClusterCache`        | valkey-py    |
| `django_cachex.cache.ValkeyGlideCache`          | valkey-glide |
| `django_cachex.cache.ValkeyGlideClusterCache`   | valkey-glide |

To use a different adapter, change `BACKEND`.

## Authentication

### Password in URL

```python
"LOCATION": "valkey://user:password@127.0.0.1:6379/1"
```

### Password with Special Characters

For passwords with special characters, pass via OPTIONS:

```python
"LOCATION": "valkey://127.0.0.1:6379/1",
"OPTIONS": {
    "password": "my$pecial!password",
}
```

### Valkey/Redis ACLs

```python
"LOCATION": "valkey://username@127.0.0.1:6379/1",
"OPTIONS": {
    "password": "password",
}
```

`username` also works as an OPTIONS key, which is what you want when the ACL
user name contains characters that would need URL-escaping:

```python
"LOCATION": "valkey://127.0.0.1:6379/1",
"OPTIONS": {
    "username": "app@service",
    "password": "password",
}
```

`OPTIONS` wins over the URL for both `username` and `password`.

## SSL/TLS

### Basic SSL

```python
"LOCATION": "valkeys://127.0.0.1:6379/1"  # or rediss://
```

### Self-Signed Certificates

```python
"LOCATION": "valkeys://127.0.0.1:6379/1",
"OPTIONS": {
    "ssl_cert_reqs": None,  # Disable verification
}
```

### Custom Certificates

```python
"LOCATION": "valkeys://127.0.0.1:6379/1",
"OPTIONS": {
    "ssl_ca_certs": "/path/to/ca.crt",
    "ssl_certfile": "/path/to/client.crt",
    "ssl_keyfile": "/path/to/client.key",
}
```

The `ssl_*` keys go to the redis-py or valkey-py connection pool and have no
effect on valkey-glide, where the only TLS input cachex passes on is the
`use_tls` flag. See [Valkey-Glide OPTIONS](#valkey-glide-options).

## Sentinel Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisSentinelCache",
        "LOCATION": "redis://mymaster/0",  # Master name
        "OPTIONS": {
            "sentinels": [
                ("sentinel1.example.com", 26379),
                ("sentinel2.example.com", 26379),
                ("sentinel3.example.com", 26379),
            ],
            "sentinel_kwargs": {
                "password": "sentinel-password",
            },
        },
    }
}
```

## Cluster Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisClusterCache",
        "LOCATION": "redis://127.0.0.1:7000",
    }
}
```

## Timeouts

### Default Timeout

```python
"TIMEOUT": 300  # 5 minutes, None for no expiry
```

### Special Values

```python
cache.set("key", "value", timeout=0)  # Delete immediately
cache.set("key", "value", timeout=None)  # Never expires
```

## Key Configuration

### Key Prefix

```python
"KEY_PREFIX": "myapp"
# Keys become: myapp:1:keyname
```

### Key Version

```python
"VERSION": 1
# Keys become: prefix:1:keyname
```

### Custom Key Function

```python
def my_key_func(key, key_prefix, version):
    return f"{key_prefix}:v{version}:{key}"

CACHES = {
    "default": {
        ...
        "KEY_FUNCTION": "myapp.cache.my_key_func",
    }
}
```

### Reverse Key Function

```python
def my_reverse_key_func(key):
    return key.split(":", 2)[2]

CACHES = {
    "default": {
        ...
        "REVERSE_KEY_FUNCTION": "myapp.cache.my_reverse_key_func",
    }
}
```

The inverse of `KEY_FUNCTION`: it takes the full internal key and returns the user key. Dotted path or callable, same as `KEY_FUNCTION`. Reach for it when a custom `KEY_FUNCTION` makes the default `prefix:version:` stripping wrong; the default handles a `KEY_PREFIX` containing colons on its own.

Only `reverse_key()` consults it, so it changes what `keys()`, `iter_keys()`, `scan()` and the blocking list pops (`blpop`, `brpop`) hand back, plus their async counterparts. Stored keys are untouched.

!!! warning "RESP backends only"
    `LocMemCache`, `DatabaseCache` and `StreamCache` strip the prefix
    themselves and ignore `REVERSE_KEY_FUNCTION`. `TrackingCache` forwards
    `reverse_key()` to its transport, so it belongs on that alias rather
    than the composite one.

## Complete Example

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,
        "KEY_PREFIX": "myapp",
        "VERSION": 1,
        "OPTIONS": {
            # Serialization
            "serializer": "django_cachex.serializers.pickle.PickleSerializer",
            # Compression
            "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
            # Connection pool
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
        },
    }
}
```
