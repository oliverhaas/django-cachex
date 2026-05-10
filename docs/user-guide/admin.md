# Cache Admin

django-cachex provides a Django admin interface for browsing cache keys, viewing values, and adding, editing, or deleting entries.

## Installation

Add `django_cachex.admin` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ... other apps
    "django.contrib.admin",
    "django_cachex.admin",  # Cache admin interface
]
```

The cache admin will appear in the Django admin sidebar under "Caches".

## Permissions

The admin uses Django's built-in permission system. Superusers have full access. Staff users need explicit permissions:

- `django_cachex.view_cache` / `view_key`: view caches and keys
- `django_cachex.change_cache`: flush caches, update TTL
- `django_cachex.add_key`: create keys
- `django_cachex.change_key`: edit values
- `django_cachex.delete_key`: delete keys

## Support Levels

Different cache backends have different levels of support:

| Badge | Level | Description |
|-------|-------|-------------|
| **cachex** | Full Support | django-cachex backends (`ValkeyCache`, `RedisCache`, `LocMemCache`, `DatabaseCache`, etc.). All features: key listing, pattern search, TTL inspection, and data type operations. |
| **limited** | Limited Support | Stock Django backends (`django.core.cache.backends.*`) and custom backends. The cache is listed and configurable, but key browsing isn't available because Django's `BaseCache` doesn't expose key listing. |

### Using Django's stock LocMemCache or DatabaseCache?

Switch to `django_cachex.cache.LocMemCache` / `django_cachex.cache.DatabaseCache` for full admin support. Both are drop-in replacements for the stock Django classes.

### Using Django's stock Redis backend?

Switch to `ValkeyCache` / `RedisCache` for full functionality. See the [quickstart guide](../getting-started/quickstart.md) for migration instructions.

## Views

### Caches (Index)

Lists all configured caches showing name, backend class, location, and support level.

**Actions:** Flush selected caches (delete all entries).

### Key Browser

Click a cache name to browse its keys with wildcard search (`*`), data type display, TTL, and pagination.

**Actions:** Delete selected keys, add new key.

### Key Detail

View and edit a specific key's value (formatted JSON for objects/arrays), data type, and TTL. Supports editing values/timeout and deleting the key.

### Cache Info

View server information: configuration, server version/uptime, memory usage, connected clients, command statistics, and keyspace data.

### Add Key

Create a new cache entry with key name, value (JSON objects/arrays are parsed automatically), and optional timeout in seconds.

## Backend Abilities

The admin adapts based on backend capabilities:

| Feature | RESP backends (Valkey/Redis) | LocMemCache / DatabaseCache | limited |
|---------|------------------------------|-----------------------------|---------|
| List keys | Yes | Yes | No |
| Get key | Yes | Yes | No |
| Delete key | Yes | Yes | No |
| Edit key | Yes | Yes | No |
| Get TTL | Yes | Yes | No |
| Get type | Yes | Limited (no RESP types) | No |
| Cache info | Yes | Yes | No |
| Flush cache | Yes | Yes | No |

## Tips

- **Pattern Search**: Use `*` as a wildcard (e.g., `user:*` finds all keys starting with "user:").
- **JSON Values**: Enter valid JSON when editing to store objects or arrays.
- **Help Button**: Each view has a help button with context-specific tips.
- **Refresh**: Use the refresh action to update key lists and statistics.
