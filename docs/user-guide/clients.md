# Pluggable Clients

django-cachex provides several pluggable client implementations for different use cases.

## Default Client

The default client supports primary/replica replication:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": [
            "redis://127.0.0.1:6379/1",  # Primary
            "redis://127.0.0.1:6378/1",  # Replica
        ],
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        }
    }
}
```

!!! warning
    Replication setup is not heavily tested in production environments.

## Sentinel Client

For Redis Sentinel high availability setups. See [Sentinel](sentinel.md) for detailed configuration.

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.SentinelClient",
            "SENTINELS": [
                ("sentinel-1", 26379),
                ("sentinel-2", 26379),
            ],
        },
    },
}
```

## Cluster Client

For Redis Cluster deployments with server-side sharding across multiple nodes. See [Cluster](cluster.md) for detailed configuration and slot handling.

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:7000",  # Any cluster node
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.ClusterClient",
            "CONNECTION_FACTORY": "django_cachex.pool.ClusterConnectionFactory",
        }
    }
}
```

!!! note "Cluster Behavior"
    - Django cache interface methods (`get_many`, `set_many`, `delete_many`, `keys`, `clear`, etc.) are cluster-aware and handle cross-slot keys automatically
    - Direct Redis method wrappers (sets, lists, hashes) pass through to Redis - use hash tags for multi-key operations
    - See the [Cluster documentation](cluster.md) for details on slot handling and hash tags
