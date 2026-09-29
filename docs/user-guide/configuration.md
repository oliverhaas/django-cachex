# Configuration Reference

## Basic Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,  # seconds; None never expires, 0 expires immediately
        "KEY_PREFIX": "myapp",  # keys become myapp:1:keyname
        "VERSION": 1,
        "OPTIONS": {
            "serializer": "django_cachex.serializers.pickle.PickleSerializer",
            "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
        },
    }
}
```

To cache ORM query results in this alias too, add `"django_cachex.orm"` to
`INSTALLED_APPS`. See [ORM Cache](orm-cache.md).

## Backend Classes

All backends live in `django_cachex.cache`. Valkey and Redis are
protocol-compatible, so the Valkey and Redis backends each work with either
server.

`RespCache`, `RespClusterCache` and `RespSentinelCache` are the Valkey/Redis
base classes. They bind no driver, so use them for subclassing and typing, not
as `BACKEND`.

### Valkey / Redis (Python driver)

| Backend | Adapter | Topology |
|---------|---------|----------|
| `ValkeyCache` | valkey-py | Standalone |
| `RedisCache` | redis-py | Standalone |
| `ValkeySentinelCache` | valkey-py | Sentinel high availability |
| `RedisSentinelCache` | redis-py | Sentinel high availability |
| `ValkeyClusterCache` | valkey-py | Cluster sharding |
| `RedisClusterCache` | redis-py | Cluster sharding |

### Valkey-Glide

!!! warning "Experimental"
    The valkey-glide adapter can change its interfaces and behavior. It has
    less production testing than the redis-py and valkey-py backends.

[valkey-glide](https://github.com/valkey-io/valkey-glide) is Valkey's official
client library, with a Rust core. Install it with the `valkey-glide` extra. It
ships no Sentinel client, so there is no glide Sentinel backend.

| Backend | Topology |
|---------|----------|
| `ValkeyGlideCache` | Standalone |
| `ValkeyGlideClusterCache` | Cluster sharding |

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyGlideCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
    }
}
```

glide has no Unix-socket transport, so `LOCATION` must be a `redis://`,
`rediss://`, `valkey://` or `valkeys://` URL. A glide pipeline queues only the
documented commands. Send any other command with `pipe.execute_command(*args)`.

### Local backends

| Backend | Description |
|---------|-------------|
| `LocMemCache` | Drop-in replacement for Django's `LocMemCache` |
| `DatabaseCache` | Drop-in replacement for Django's `DatabaseCache` |

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.LocMemCache",
        "LOCATION": "unique-name",  # one store per LOCATION within the process
        "OPTIONS": {
            "MAX_ENTRIES": 300,  # default; cull when the store reaches this many keys
            "CULL_FREQUENCY": 3,  # default; evict 1/N of the entries per cull, 0 empties the store
        },
    },
    "db": {
        "BACKEND": "django_cachex.cache.DatabaseCache",
        "LOCATION": "django_cache_table",  # the cache table name
        "OPTIONS": {
            "MAX_ENTRIES": 300,
            "CULL_FREQUENCY": 3,
        },
    },
}
```

A collection counts as one entry toward `MAX_ENTRIES`. `DatabaseCache` uses the
same table as Django's stock backend. Create it with `manage.py
createcachetable` before first use.

Both backends support the other data-structure commands, `ttl()` in whole
seconds, `expire()`, `persist()`, `type()`, `touch()`, `info()`, key listing
(`keys()`, `iter_keys()`, `scan()`, `delete_pattern()`) and the admin. Key
patterns use the Redis glob dialect and match case-sensitively on every
database vendor. These methods and their `a*` twins raise `NotSupportedError`:

- `pttl()`, `pexpire()`, `expireat()`, `pexpireat()`, `expiretime()` and the
  hash-field expiration family (`hexpire()`, `httl()`, `hsetex()`, `hgetex()`
  and their relatives)
- `lock()`, `pipeline()`, `eval_script()` and `get_client()`
- `rename()`, `renamenx()`, `sscan()`, `sscan_iter()`, `clear_all_versions()`
  and `flush_db()`
- `slowlog_get()`, `slowlog_len()`, `memory_usage()` and `largest_keys()`
- the blocking list pops and the cross-key store commands (`lmove()`,
  `smove()`, `sinterstore()` and the like)
- `semaphore()` on `DatabaseCache`. On `LocMemCache`, it returns the
  in-process `django_cachex.Semaphore`.

Streams are not implemented.

`incr_version()` and `decr_version()` move keys of every type and keep their
TTL, like Redis `RENAME`. `DatabaseCache.incr()` and `decr()` update the row
atomically and keep the key's TTL. Like Django, they raise `ValueError` on a
missing key, where the Valkey/Redis backends create it.

On MySQL, keep the connection at `READ COMMITTED`, Django's default for MySQL.
The compound operations (`lpush()`, `sadd()`, `hincrby()` and the rest) lock
their row with `SELECT ... FOR UPDATE`. Under the InnoDB default of
`REPEATABLE READ`, two clients that create the same key at the same time
deadlock. One of them gets an `OperationalError` (MySQL error 1213).

Inside `transaction.atomic()`, `ATOMIC_REQUESTS` included, the row lock of a
compound operation or `incr()` lasts until the outer transaction ends. Keep
these calls out of long-running transactions.

### Composite backend

`TrackingCache` is a read-through local cache over a Redis/Valkey alias,
invalidated by the server's `CLIENT TRACKING`. See
[TrackingCache](composite-backends.md).

## LOCATION

`LOCATION` takes one or more server URLs:

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

On `ValkeyGlideCache`, the URLs after the first are replicas, and reads prefer
a reachable replica over the primary. A valkey-glide URL list must agree on TLS
scheme, username and password, and on `ValkeyGlideCache` also on the database.
`OPTIONS["db"]`, `["username"]` and `["password"]` apply to every URL.

## OPTIONS Reference

| Keys | Honored by |
|------|------------|
| `serializer`, `compressor`, `stampede_prevention`, `username`, `password` | Every Valkey/Redis backend, valkey-glide included |
| `pool_class`, `async_pool_class`, `parser_class` | redis-py and valkey-py backends except cluster, see [Connection Pool](#connection-pool) |
| `sentinels`, `sentinel_kwargs` | redis-py and valkey-py Sentinel backends |
| Any other key, such as `socket_timeout`, `socket_connect_timeout`, `retry_on_timeout`, `ssl_*` and `db` | redis-py and valkey-py backends, which pass it to the driver's `from_url()` |
| `db`, `use_tls` / `ssl`, `request_timeout`, `client_name` | valkey-glide backends, see [Valkey-Glide OPTIONS](#valkey-glide-options) |

valkey-glide silently ignores every other key, such as `socket_timeout`.

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

| Serializer | Description |
|------------|-------------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default) |
| `django_cachex.serializers.json.JsonSerializer` | JSON via `DjangoJSONEncoder` |
| `django_cachex.serializers.msgpack.MsgpackSerializer` | MessagePack (requires `msgpack`) |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON (requires `orjson`) |
| `django_cachex.serializers.ormsgpack.OrmsgpackSerializer` | Rust-backed MessagePack (requires `ormsgpack`) |

See [Serializers](serializers.md) for type compatibility and constructor options.

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

| Compressor | Description |
|------------|-------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | zlib (stdlib) |
| `django_cachex.compressors.gzip.GzipCompressor` | gzip (stdlib) |
| `django_cachex.compressors.lzma.LzmaCompressor` | LZMA (stdlib) |
| `django_cachex.compressors.zstd.ZstdCompressor` | Zstandard (stdlib on 3.14+) |
| `django_cachex.compressors.lz4.Lz4Compressor` | LZ4 (requires `lz4`) |

Compression applies only to values longer than the compressor's `min_length`
(default 256 bytes). See [Compression](compression.md).

### Connection Pool

```python
"OPTIONS": {
    # Dotted path or class; defaults to the driver's ConnectionPool
    "pool_class": "valkey.ConnectionPool",

    # Async pool for the a* methods; defaults to the driver's async ConnectionPool
    "async_pool_class": "valkey.asyncio.ConnectionPool",

    # Defaults to the driver's DefaultParser, which is the C parser when
    # libvalkey (Valkey) or hiredis (Redis) is installed
    "parser_class": "valkey.connection.DefaultParser",  # or "redis.connection.DefaultParser"

    # Every other key goes to the driver's from_url()
    "retry_on_timeout": True,
    "socket_connect_timeout": 5,
    "socket_timeout": 5,
}
```

On a Sentinel backend, `pool_class` and `async_pool_class` default to the
driver's sync and async `SentinelConnectionPool`, and a replacement must
subclass the matching class. The redis-py and valkey-py cluster backends reject
all three `*_class` keys. See [Async support](async.md#custom-async-pool-class)
for the async pool.

### Cache stampede prevention

Probabilistic early recompute (XFetch) keeps the expiry of a hot key from
setting off a thundering herd of recomputes:

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

The option takes `True` for the defaults, `False` or `None` for off, a dict, or
a `django_cachex.StampedeConfig`. Any other value, such as the string `"False"`
from an environment variable, raises `ImproperlyConfigured`. `buffer` is a
non-negative `int` in seconds. `beta` and `delta` are finite non-negative
numbers, and 0 for either turns off early recompute, so keys expire logically
at their timeout.

The `stampede_prevention=` keyword overrides the option for one call. It takes
`True`, `False`, a `StampedeConfig`, or `None`, the default, which keeps the
`OPTIONS` setting. `get`, `set`, `add`, `touch`, `get_or_set`, `get_many`,
`set_many`, `ttl`, `pttl`, `expire`, `expireat`, `pexpire`, `pexpireat`,
`expiretime` and their `a*` twins take it. On `touch`, pass the value the
original write used, because it decides whether the new TTL includes the
buffer.

A pipelined `get()` skips the early-recompute check. It returns the stored
value until the key expires, up to `buffer` seconds after its timeout.

!!! warning "Valkey/Redis backends only"
    `LocMemCache` and `DatabaseCache` ignore the option, and passing the
    keyword to them raises `TypeError`. `TrackingCache` follows its
    transport's option and takes no keyword.

### Valkey-Glide OPTIONS

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
| `db` | Database index, overriding the URL. `ValkeyGlideClusterCache` ignores it. |
| `use_tls` / `ssl` | Force TLS on or off. Without either key, TLS follows the `rediss://` or `valkeys://` scheme. |
| `username` / `password` | ACL credentials, overriding the URL. A `username` without a `password` has no effect, so a nopass ACL user connects as `default`. |
| `request_timeout` | Per-request timeout in milliseconds. |
| `client_name` | Client name, shown in `CLIENT LIST`. |

## Authentication

```python
# Credentials in the URL
"LOCATION": "valkey://user:password@127.0.0.1:6379/1"

# Credentials in OPTIONS, for characters that need URL escaping
"LOCATION": "valkey://127.0.0.1:6379/1",
"OPTIONS": {
    "username": "app@service",  # Valkey/Redis ACL user
    "password": "my$pecial!password",
}
```

`OPTIONS` wins over the URL for `username` and `password`, unless its value is
`None` or `""`.

## SSL/TLS

```python
"LOCATION": "valkeys://127.0.0.1:6379/1",  # or rediss://

# Self-signed certificate: disable verification
"OPTIONS": {"ssl_cert_reqs": None}

# Custom CA and client certificate
"OPTIONS": {
    "ssl_ca_certs": "/path/to/ca.crt",
    "ssl_certfile": "/path/to/client.crt",
    "ssl_keyfile": "/path/to/client.key",
}
```

valkey-glide ignores the `ssl_*` keys and takes only a TLS flag, see
[Valkey-Glide OPTIONS](#valkey-glide-options).

## Sentinel Configuration

`LOCATION` is one URL naming the Sentinel service. List the Sentinel nodes in
`OPTIONS["sentinels"]`. See [Sentinel](sentinel.md).

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

A `LOCATION` list seeds node discovery with every URL, so the cache connects
while a seed node is down. On `RedisClusterCache` and `ValkeyClusterCache`,
credentials, TLS and the other connection options come from the first URL.
A valkey-glide list must agree on them, as [LOCATION](#location) describes.

See [Cluster](cluster.md) for the cluster restrictions.

## Key Configuration

```python
def my_key_func(key, key_prefix, version):
    return f"{key_prefix}:v{version}:{key}"


def my_reverse_key_func(key):
    return key.split(":", 2)[2]


CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "KEY_FUNCTION": "myapp.cache.my_key_func",
        "REVERSE_KEY_FUNCTION": "myapp.cache.my_reverse_key_func",
    }
}
```

`REVERSE_KEY_FUNCTION` takes the full internal key and returns the user key,
the inverse of `KEY_FUNCTION`. It accepts a dotted path or a callable. Set it
when a custom `KEY_FUNCTION` breaks the default `prefix:version:` stripping. It
changes the keys that `keys()`, `iter_keys()`, `scan()`, `blpop()`, `brpop()`
and their `a*` twins return, not the stored keys.

!!! warning "Valkey/Redis backends only"
    `LocMemCache` and `DatabaseCache` ignore `REVERSE_KEY_FUNCTION`. For a
    `TrackingCache`, set it on the transport alias.
