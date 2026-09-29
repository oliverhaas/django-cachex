# Valkey/Redis Sentinel

The Sentinel backends ask the Sentinel nodes for the current primary, and
connect to the new primary after a failover. Writes go to the primary and reads
to the replicas. For the basic setup, see
[Configuration](configuration.md#sentinel-configuration).

| Option | Description |
|--------|-------------|
| `sentinels` | List of `(host, port)` tuples for the Sentinel nodes (required) |
| `sentinel_kwargs` | Dict of kwargs for the connections to the Sentinel nodes, such as `password` |

`LOCATION` is one URL, `redis://service_name/db` or, for `ValkeySentinelCache`,
`valkey://service_name/db`. `service_name` is the master name configured in
Sentinel. Sentinel discovers the primary and replicas itself, so a `LOCATION`
with more than one URL raises `ImproperlyConfigured`.

For TLS, use `rediss://service_name/db` for `RedisSentinelCache` or
`valkeys://service_name/db` for `ValkeySentinelCache`. The backend then connects
to the primary and replicas over TLS. The scheme does not apply to the Sentinel
nodes, so configure those connections in `sentinel_kwargs`.

`pool_class` must subclass the driver's `SentinelConnectionPool`, and
`async_pool_class` its async counterpart. See
[Connection Pool](configuration.md#connection-pool).
