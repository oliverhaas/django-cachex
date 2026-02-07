# Configuration Reference

Complete reference for all django-cachex configuration options.

## Basic Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",  # or RedisCache
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,              # Default timeout in seconds
        "KEY_PREFIX": "myapp",       # Prefix for all keys
        "VERSION": 1,                # Key version number
        "OPTIONS": {
            # See options below
        }
    }
}
```

## Backend Classes

| Backend | Description |
|---------|-------------|
| `ValkeyCache` | Standard Valkey connection |
| `RedisCache` | Standard Redis connection |
| `ValkeySentinelCache` | Valkey Sentinel high availability |
| `RedisSentinelCache` | Redis Sentinel high availability |
| `ValkeyClusterCache` | Valkey Cluster sharding |
| `RedisClusterCache` | Redis Cluster sharding |

All backends are in `django_cachex.cache`.

!!! note "Valkey and Redis Compatibility"
    Valkey and Redis are probably still fully compatible, meaning you could use either backend with either server.
    We recommend Valkey as it remains fully open source.

## LOCATION

Server URL(s). Supports multiple formats:

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

## OPTIONS Reference

### Serialization

```python
"OPTIONS": {
    # Single serializer (string path, class, or instance)
    "serializer": "django_cachex.serializers.pickle.PickleSerializer",

    # Or with fallback for migration
    "serializer": [
        "django_cachex.serializers.msgpack.MessagePackSerializer",  # Write
        "django_cachex.serializers.pickle.PickleSerializer",    # Fallback read
    ],
}
```

Available serializers:

| Serializer | Description |
|------------|-------------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default) |
| `django_cachex.serializers.json.JSONSerializer` | JSON |
| `django_cachex.serializers.msgpack.MessagePackSerializer` | MessagePack (requires msgpack) |

### Compression

```python
"OPTIONS": {
    # Single compressor
    "compressor": "django_cachex.compressors.zstd.ZStdCompressor",

    # Or with fallback for migration
    "compressor": [
        "django_cachex.compressors.zstd.ZStdCompressor",  # Write
        "django_cachex.compressors.zlib.ZlibCompressor",  # Fallback read
    ],
}
```

Available compressors:

| Compressor | Description |
|------------|-------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | zlib compression |
| `django_cachex.compressors.gzip.GzipCompressor` | gzip compression |
| `django_cachex.compressors.lz4.Lz4Compressor` | LZ4 (requires lz4) |
| `django_cachex.compressors.lzma.LzmaCompressor` | LZMA |
| `django_cachex.compressors.zstd.ZStdCompressor` | Zstandard (requires zstd) |

Compression is only applied to values larger than `min_length` bytes (default: 256).

### Connection Pool

```python
"OPTIONS": {
    # Custom pool class (use valkey.ConnectionPool for Valkey)
    "pool_class": "valkey.ConnectionPool",

    # Pool size and options (passed to pool constructor)
    "max_connections": 100,
    "retry_on_timeout": True,

    # Socket timeouts
    "socket_connect_timeout": 5,
    "socket_timeout": 5,
}
```

### Parser

```python
"OPTIONS": {
    # For Valkey with libvalkey
    "parser_class": "valkey.connection.LibvalkeyParser",
    # For Redis with hiredis
    # "parser_class": "redis.connection.HiredisParser",
}
```

### Exception Handling

```python
"OPTIONS": {
    # Ignore connection errors (return default/None instead)
    "ignore_exceptions": True,

    # Log ignored exceptions
    "log_ignored_exceptions": True,
}
```

### Connection Lifecycle

```python
"OPTIONS": {
    # Close connections after each request
    "close_connection": True,
}
```

## Authentication

### Password in URL

```python
"LOCATION": "valkey://user:password@127.0.0.1:6379/1"
```

### Password with Special Characters

For passwords with special characters, pass separately:

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

## Sentinel Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.client.RedisSentinelCache",
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
        }
    }
}
```

## Cluster Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.client.RedisClusterCache",
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
cache.set("key", "value", timeout=0)     # Delete immediately
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
            "compressor": "django_cachex.compressors.zstd.ZStdCompressor",

            # Connection pool
            "max_connections": 50,
            "socket_connect_timeout": 5,
            "socket_timeout": 5,

            # Exception handling
            "ignore_exceptions": False,
        }
    }
}
```
