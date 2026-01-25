# Valkey/Redis Sentinel

For basic sentinel setup, see [Configuration](configuration.md#sentinel-configuration).

## Configuration Options

| Option | Description |
|--------|-------------|
| `sentinels` | List of (host, port) tuples for Sentinel nodes (required) |
| `sentinel_kwargs` | Dict of kwargs passed to Sentinel connection (e.g., password) |

The `LOCATION` URL format is `redis://service_name/db` where `service_name` is the master name configured in Sentinel.

## How It Works

The Sentinel backend automatically:

1. Connects to Sentinel nodes to discover the current primary
2. Creates separate connection pools for primary (writes) and replica (reads)
3. Handles failover automatically when the primary changes
