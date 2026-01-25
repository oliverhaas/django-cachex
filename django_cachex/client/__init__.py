# Cache backends (extend Django's BaseCache) - use these as BACKEND
from django_cachex.client.cache import (
    KeyValueCache,
    RedisCache,
    ValkeyCache,
)

# Cluster implementations
from django_cachex.client.cluster import (
    KeyValueClusterCache,
    KeyValueClusterCacheClient,
    RedisClusterCache,
    RedisClusterCacheClient,
    ValkeyClusterCache,
    ValkeyClusterCacheClient,
)

# Cache clients (do actual Redis operations) - internal use
from django_cachex.client.default import (
    KeyValueCacheClient,
    RedisCacheClient,
    ValkeyCacheClient,
)

# Sentinel implementations
from django_cachex.client.sentinel import (
    KeyValueSentinelCache,
    KeyValueSentinelCacheClient,
    RedisSentinelCache,
    RedisSentinelCacheClient,
    ValkeySentinelCache,
    ValkeySentinelCacheClient,
)

__all__ = [
    # Standard cache backends (use as BACKEND)
    "KeyValueCache",
    # Standard cache clients (internal)
    "KeyValueCacheClient",
    # Cluster cache backends (use as BACKEND)
    "KeyValueClusterCache",
    # Cluster cache clients (internal)
    "KeyValueClusterCacheClient",
    # Sentinel cache backends (use as BACKEND)
    "KeyValueSentinelCache",
    # Sentinel cache clients (internal)
    "KeyValueSentinelCacheClient",
    "RedisCache",
    "RedisCacheClient",
    "RedisClusterCache",
    "RedisClusterCacheClient",
    "RedisSentinelCache",
    "RedisSentinelCacheClient",
    "ValkeyCache",
    "ValkeyCacheClient",
    "ValkeyClusterCache",
    "ValkeyClusterCacheClient",
    "ValkeySentinelCache",
    "ValkeySentinelCacheClient",
]
