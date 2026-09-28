# Configuration Reference

## Basic Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,  # seconds
        "KEY_PREFIX": "myapp",
        "VERSION": 1,
        "OPTIONS": {
            # see the OPTIONS reference below
        },
    }
}
```

## Backend Classes

All backends live in `django_cachex.cache`. Each class has a fixed adapter, the
layer that talks to the client library. To use a different adapter, change
`BACKEND`.

Valkey and Redis are protocol-compatible, so the Valkey and Redis backends each
work with either server. Valkey is recommended because it remains fully open
source.

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
    The valkey-glide adapter is experimental. Its interfaces and behavior can
    change, and it has seen less production testing than the redis-py and
    valkey-py backends.

[valkey-glide](https://github.com/valkey-io/valkey-glide) is Valkey's official
client library, with a Rust core. The `valkey-glide` extra installs both of its
PyPI distributions. cachex uses `glide_sync.GlideClient` from
`valkey-glide-sync` for sync calls, and `glide.GlideClient` from `valkey-glide`
for the `a*` methods.

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

valkey-glide ships no Sentinel client, so there is no glide Sentinel backend.

Differences from the redis-py and valkey-py backends:

- `LOCATION` must be a `redis://`, `rediss://`, `valkey://` or `valkeys://`
  URL. glide has no Unix-socket transport. A Unix-socket URL (`unix://`,
  `redis+socket://`, `valkey+socket://`), a schemeless `host:port` or a
  non-numeric port raises `ImproperlyConfigured` at `caches[alias]`.
- `xtrim()` and `axtrim()` without `maxlen` or `minid` raise
  `ValueError("xtrim requires maxlen or minid")` before anything is sent,
  directly and on a pipeline.
- A pipeline queues only the documented commands. An unknown attribute raises
  `AttributeError` instead of becoming a command, so send raw commands with
  `pipe.execute_command(*args)`.

### Local backends

| Backend | Description |
|---------|-------------|
| `LocMemCache` | Drop-in replacement for Django's `LocMemCache` with data-structure ops, `ttl()`/`expire()`/`persist()`, and admin support |
| `DatabaseCache` | Drop-in replacement for Django's `DatabaseCache` with the same extensions |

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.LocMemCache",
        "LOCATION": "unique-name",  # one store per LOCATION within the process
        "OPTIONS": {
            "MAX_ENTRIES": 300,  # cull when the store reaches this many keys
            "CULL_FREQUENCY": 3,  # evict 1/N of the entries per cull; 0 empties the store
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

`MAX_ENTRIES` (default 300) and `CULL_FREQUENCY` (default 3) keep their Django
meaning. The count covers the whole store, collections included. A
`LocMemCache` hash or list counts as one entry. A `DatabaseCache` compound
operation (`rpush()`, `sadd()`, `hset()` and the like) that inserts a new row
runs the same cull check as `set()`.

`DatabaseCache` stores everything in the table named by `LOCATION`, the same
table Django's stock backend uses. Create it with `manage.py createcachetable`
before first use. The data structures live in the existing `value` column and
need no schema change.

The TTL surface is `ttl()`, `expire()` and `persist()`, and `ttl()` reports
whole seconds. These methods and any `a*` twins raise `NotSupportedError`:

- `pttl()`, `pexpire()`, `expireat()`, `pexpireat()`, `expiretime()` and the
  hash-field expiration family (`hexpire()`, `httl()`, `hsetex()`, `hgetex()`
  and their relatives)
- `lock()`, `pipeline()`, `eval_script()` and `get_client()`
- `rename()`, `renamenx()`, `sscan()`, `sscan_iter()` and
  `clear_all_versions()`
- `slowlog_get()`, `slowlog_len()`, `memory_usage()` and `largest_keys()`
- the blocking list pops and the cross-key store commands (`lmove()`,
  `smove()`, `sinterstore()` and the like)

Streams are not implemented.

Both backends support the other hash, list, set and sorted-set commands,
`type()`, `touch()`, `info()`, key listing (`keys()`, `iter_keys()`, `scan()`,
`delete_pattern()`) and the admin. Key patterns use the Redis glob dialect on
both, and `DatabaseCache` matches case-sensitively on every database vendor.
`LocMemCache` also has `semaphore()`, backed by the in-process
`django_cachex.Semaphore`. `DatabaseCache` has no semaphore.

`incr_version()` and `decr_version()` move the key the way Redis `RENAME` does.
Every key type moves, collections included, and the key keeps its remaining
TTL. `DatabaseCache.incr()` and `decr()` are atomic row updates that keep the
key's TTL. Like Django, they raise `ValueError` on a missing key, where the
Valkey/Redis backends create it.

On MySQL, keep the connection at `READ COMMITTED`, Django's default isolation
level for MySQL. The compound operations (`lpush()`, `sadd()`, `hincrby()`
and the rest) take a `SELECT ... FOR UPDATE` row lock. Under the InnoDB default
of `REPEATABLE READ`, that lock takes a gap lock on a row that does not exist
yet. Two clients that create the same key at the same time then deadlock, and
one gets an `OperationalError` (MySQL error 1213).

Inside a caller's `transaction.atomic()` block, `ATOMIC_REQUESTS` included, the
row lock of a compound operation or `incr()` lasts until the outer transaction
ends. A savepoint release does not unlock rows. Keep these calls out of
long-running transactions.

### Composite backend

| Backend | Description |
|---------|-------------|
| `TrackingCache` | Read-through local cache over a Redis/Valkey alias, invalidated by the server's `CLIENT TRACKING` |

### Shared base classes

`django_cachex.cache` also exports `RespCache`, `RespClusterCache` and
`RespSentinelCache`, the shared implementation that the Valkey/Redis backends
inherit. They bind no driver, so naming one as `BACKEND` raises
`ImproperlyConfigured` at `caches[alias]`. Use them for subclassing and typing.
Elsewhere in these docs, a mention of them means every Valkey/Redis backend.

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

A blank or missing `LOCATION` on a Valkey/Redis backend raises
`ImproperlyConfigured` at `caches[alias]`. The adapter is built there too,
without opening a connection. A missing driver, a `pool_class` or
`async_pool_class` of the wrong type, or a multi-entry Sentinel `LOCATION`
therefore fails there, before the first command.

The valkey-glide backends accept a URL list too. On `ValkeyGlideCache`, the
URLs after the first become replica addresses, and the client is built with
`read_from=PREFER_REPLICA`. Reads then go to a replica when one is reachable,
and to the primary otherwise. Two constraints apply:

- Every URL must agree on TLS scheme, username, password and, on
  `ValkeyGlideCache`, database, because glide applies one connection setting
  to the whole address list. `OPTIONS["db"]`, `["username"]` and
  `["password"]` apply to every URL and so settle a mismatch in their value.
  Any other mismatch raises `ImproperlyConfigured` when the backend first
  connects.
- Duplicate host/port entries collapse to one address, so listing the same URL
  twice does not add a replica.

## OPTIONS Reference

Which keys are honored depends on the backend:

| Keys | Honored by |
|------|------------|
| `serializer`, `compressor`, `stampede_prevention`, `username`, `password` | Every Valkey/Redis backend, valkey-glide included |
| `pool_class`, `async_pool_class`, `parser_class`, `sentinels`, `sentinel_kwargs`, plus every key forwarded to the driver's `from_url()`, such as `socket_timeout`, `socket_connect_timeout`, `retry_on_timeout`, `ssl_*` and `db` | redis-py and valkey-py backends. The cluster backends reject `pool_class`, `async_pool_class` and `parser_class`, see [Connection Pool](#connection-pool). |
| `db`, `use_tls` / `ssl`, `request_timeout`, `client_name` | valkey-glide backends, see [Valkey-Glide OPTIONS](#valkey-glide-options) |

valkey-glide ignores every other `OPTIONS` key without a warning, so a
`socket_timeout` or `pool_class` copied from a valkey-py alias has no effect
there.

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

| Compressor | Description |
|------------|-------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | zlib (stdlib) |
| `django_cachex.compressors.gzip.GzipCompressor` | gzip (stdlib) |
| `django_cachex.compressors.lzma.LzmaCompressor` | LZMA (stdlib) |
| `django_cachex.compressors.zstd.ZstdCompressor` | Zstandard (stdlib on 3.14+) |
| `django_cachex.compressors.lz4.Lz4Compressor` | LZ4 (requires `lz4`) |

Compression applies only to values longer than `min_length` bytes (default
256).

### Connection Pool

These keys apply to the redis-py and valkey-py backends. valkey-glide manages
its own connections and reads none of them.

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

`pool_class` and `async_pool_class` take a dotted path or a class. They default
to the driver's sync and async `ConnectionPool`. See
[Async support](async.md#custom-async-pool-class) for the async pool.

On a Sentinel backend, `pool_class` must be `SentinelConnectionPool` or a
subclass, because a plain connection pool takes none of the primary/replica
discovery arguments. `async_pool_class` must be the driver's async
`SentinelConnectionPool` or a subclass, and the `a*` methods use it. Any other
class raises `ImproperlyConfigured` at `caches[alias]`.

The cluster backends reject `pool_class`, `async_pool_class` and `parser_class`
with `ImproperlyConfigured`. The cluster client owns its per-node pools and
picks its own parser, the C parser when `hiredis` or `libvalkey` is installed.

Other keys go to the pool's `from_url()`, so driver options such as
`socket_keepalive` and `health_check_interval` work the same way. cachex
handles seven keys itself and does not forward them: `pool_class`,
`async_pool_class`, `serializer`, `compressor`, `stampede_prevention`,
`sentinels` and `sentinel_kwargs`. `parser_class` is resolved to a class and
then passed to the pool on the standalone and Sentinel backends.

### Parser

```python
"OPTIONS": {
    # Dotted path or class; defaults to the driver's DefaultParser
    "parser_class": "valkey.connection.DefaultParser",  # or "redis.connection.DefaultParser"
}
```

`parser_class` applies to the standalone and Sentinel backends of redis-py and
valkey-py. The default, the driver's `DefaultParser`, resolves to the C parser
when `libvalkey` (Valkey) or `hiredis` (Redis) is installed, and to the
pure-Python RESP parser otherwise. Installing the `libvalkey` or `hiredis` extra is enough
to get the C parser.

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

The option accepts four forms:

- `True` turns prevention on with the defaults.
- `False` or `None` turns it off.
- A dict sets any of `buffer`, `beta` and `delta`. An empty dict turns
  prevention off, and unknown keys are dropped with a warning.
- A `django_cachex.StampedeConfig` is used as is.

Anything else, such as the string `"False"` an environment variable yields,
raises `ImproperlyConfigured` at `caches[alias]` instead of switching
prevention on.

`buffer` is a non-negative `int` in seconds. `beta` and `delta` are finite
non-negative numbers. Zero for `beta` or `delta` is valid and turns off the
probabilistic early recompute, so the key expires logically at its timeout. A
wrong type raises `TypeError` and a negative or non-finite value raises
`ValueError`, both when the configuration is built rather than on a write.

The `stampede_prevention=` keyword overrides the option per call. It takes
`True`, `False`, a `django_cachex.StampedeConfig`, or `None`, the default,
which keeps the `OPTIONS` setting. The dict form works only in `OPTIONS`, and a
dict here raises `TypeError`.

`get`, `set`, `add`, `touch`, `get_or_set`, `get_many` and `set_many` take the
keyword. So do the TTL readers and setters (`ttl`, `pttl`, `expire`,
`expireat`, `pexpire`, `pexpireat`, `expiretime`) and all their `a*` twins. On
`touch`, the keyword decides whether the refreshed TTL gets the buffer added
back, so pass the value the original write used.

!!! warning "Valkey/Redis backends only"
    Only the Valkey/Redis backends, valkey-glide included, accept the
    `stampede_prevention=` keyword. `LocMemCache` and `DatabaseCache` ignore
    `OPTIONS["stampede_prevention"]`, and their methods have no such
    parameter, so passing it raises `TypeError`. `TrackingCache` follows its
    transport's `OPTIONS` setting and has no per-call keyword either.

### Valkey-Glide OPTIONS

The valkey-glide backends read their own set of keys:

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
| `db` | Database index. It overrides a `?db=` query, which overrides the URL path. `ValkeyGlideClusterCache` ignores it, because a cluster serves only db 0. |
| `use_tls` / `ssl` | Force TLS on or off. Without either key, TLS follows the `rediss://` or `valkeys://` scheme. `use_tls` wins when both are set. |
| `username` / `password` | ACL credentials. `OPTIONS` wins over the URL. glide rejects a username without a password, so a `username` alone is ignored and a nopass ACL user connects as `default`. |
| `request_timeout` | Per-request timeout in milliseconds, converted to `int` and passed to glide. |
| `client_name` | Name reported to the server, visible in `CLIENT LIST`. |

`serializer`, `compressor` and `stampede_prevention` also apply. Every other key
is ignored.

## Authentication

### Password in URL

```python
"LOCATION": "valkey://user:password@127.0.0.1:6379/1"
```

### Password with Special Characters

Pass a password with special characters in `OPTIONS`:

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

Pass the user name in `OPTIONS` when it contains characters that need URL
escaping:

```python
"LOCATION": "valkey://127.0.0.1:6379/1",
"OPTIONS": {
    "username": "app@service",
    "password": "password",
}
```

`OPTIONS` wins over the URL for both `username` and `password`. An `OPTIONS`
value of `None` or `""` does not override. With `CACHE_PASSWORD` unset,
`"password": os.environ.get("CACHE_PASSWORD")` falls back to the URL's
password.

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

The `ssl_*` keys go to the redis-py and valkey-py connection pools. They have
no effect on valkey-glide, which receives only a `use_tls` flag. See
[Valkey-Glide OPTIONS](#valkey-glide-options).

## Sentinel Configuration

`LOCATION` is one URL naming the Sentinel service, and a list with more than
one entry raises `ImproperlyConfigured`. List the Sentinel nodes in
`OPTIONS["sentinels"]`.

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
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
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
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "KEY_FUNCTION": "myapp.cache.my_key_func",
        "REVERSE_KEY_FUNCTION": "myapp.cache.my_reverse_key_func",
    }
}
```

`REVERSE_KEY_FUNCTION` is the inverse of `KEY_FUNCTION`. It takes the full
internal key and returns the user key. Like `KEY_FUNCTION`, it accepts a dotted
path or a callable. Set it when a custom `KEY_FUNCTION` breaks the default
`prefix:version:` stripping. The default handles a `KEY_PREFIX` that contains
colons.

Only `reverse_key()` reads it. It changes the keys that `keys()`,
`iter_keys()`, `scan()`, `blpop()`, `brpop()` and their `a*` twins return.
Stored keys stay unchanged.

!!! warning "RESP backends only"
    `LocMemCache` and `DatabaseCache` strip the prefix themselves and ignore
    `REVERSE_KEY_FUNCTION`. `TrackingCache` forwards `reverse_key()` to its
    transport, so set `REVERSE_KEY_FUNCTION` on the transport alias.

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
            "serializer": "django_cachex.serializers.pickle.PickleSerializer",
            "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
            # Connection pool
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
        },
    }
}
```

To cache ORM query results in this alias too, add `"django_cachex.orm"` to
`INSTALLED_APPS`. See [ORM Cache](orm-cache.md).
