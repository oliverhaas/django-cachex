# Valkey/Redis Sentinel

For the basic setup, see [Configuration](configuration.md#sentinel-configuration).

## Configuration Options

| Option | Description |
|--------|-------------|
| `sentinels` | List of `(host, port)` tuples for the Sentinel nodes (required) |
| `sentinel_kwargs` | Dict of kwargs for the connections to the Sentinel nodes, such as `password` |

`LOCATION` is one URL of the form `redis://service_name/db`, or
`valkey://service_name/db` for `ValkeySentinelCache`. `service_name` is the
master name configured in Sentinel. Sentinel discovers the primary and replicas
itself, so a list or comma-separated string with more than one entry raises
`ImproperlyConfigured` at `caches[alias]`.

`OPTIONS["pool_class"]` must be the driver's `SentinelConnectionPool` or a
subclass, and `OPTIONS["async_pool_class"]` its async counterpart, which the
`a*` methods use. Any other class raises `ImproperlyConfigured` at
`caches[alias]`. See [Connection Pool](configuration.md#connection-pool).

## TLS

A TLS scheme in `LOCATION` is `rediss://service_name/db` for
`RedisSentinelCache` and `valkeys://service_name/db` for `ValkeySentinelCache`.
With it, the backend still discovers the primary and replicas through Sentinel
and connects to them over TLS. `sentinel_kwargs` configures the connections to
the Sentinel nodes themselves, independent of the scheme.

## How It Works

The backend asks the Sentinel nodes for the current primary and keeps separate
connection pools for the primary (writes) and the replicas (reads). After a
failover, it connects to the new primary.
