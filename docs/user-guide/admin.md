# Cache Admin

django-cachex provides a Django admin interface for browsing cache keys, viewing values, and adding, editing, or deleting entries.

## Installation

### Standard Django Admin

Add `django_cachex.admin` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ... other apps
    "django.contrib.admin",
    "django_cachex.admin",  # Cache admin interface
]
```

The cache admin will appear in the Django admin sidebar under "Caches".

### Django Unfold Theme

For users of [django-unfold](https://github.com/unfoldadmin/django-unfold), use `django_cachex.unfold` instead:

```python
INSTALLED_APPS = [
    # django-unfold must be before django.contrib.admin
    "unfold",
    "unfold.contrib.filters",
    "unfold.contrib.forms",
    # Django apps
    "django.contrib.admin",
    # ...
    "django_cachex.unfold",  # Unfold-styled cache admin
]
```

Install the unfold extra:

```console
pip install django-cachex[unfold]
```

!!! note "Choose One"
    Use either `django_cachex.admin` OR `django_cachex.unfold`, not both.
    They provide the same functionality with different styling.

## Permissions

The admin uses Django's built-in permission system. Superusers have full access. Staff users need explicit permissions:

- `django_cachex.view_cache` / `view_key` — view caches and keys
- `django_cachex.change_cache` — flush caches, update TTL
- `django_cachex.add_key` — create keys
- `django_cachex.change_key` — edit values
- `django_cachex.delete_key` — delete keys

## Support Levels

Different cache backends have different levels of support:

| Badge | Level | Description |
|-------|-------|-------------|
| **cachex** | Full Support | django-cachex backends (`ValkeyCache`, `RedisCache`, etc.) -- all features including key listing, pattern search, TTL inspection, and data type operations. |
| **wrapped** | Wrapped Support | Django builtin backends (`LocMemCache`, `DatabaseCache`, etc.) -- most features available through wrapper compatibility. |
| **limited** | Limited Support | Custom or unknown backends -- basic operations may work but key listing and advanced features may not be available. |

### Using Redis/Valkey?

If you are using Django's builtin Redis backend (`django.core.cache.backends.redis.RedisCache`), consider switching to django-cachex's `ValkeyCache` or `RedisCache` backends for full admin functionality: key browsing, pattern search, TTL inspection, native data type support, and server statistics. See the [quickstart guide](../getting-started/quickstart.md) for migration instructions.

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

| Feature | cachex | LocMemCache | DatabaseCache | FileBasedCache | limited |
|---------|--------|-------------|---------------|----------------|---------|
| List keys | Yes | Yes | Yes | Yes* | No |
| Get key | Yes | Yes | Yes | No | Yes |
| Delete key | Yes | Yes | Yes | No | Yes |
| Edit key | Yes | Yes | Yes | No | Yes |
| Get TTL | Yes | Yes | Yes | No | No |
| Get type | Yes | No | No | No | No |
| Cache info | Yes | Yes | Yes | Yes | No |
| Flush cache | Yes | Yes | Yes | Yes | Varies |

*FileBasedCache shows MD5 hashes instead of original key names (one-way hash)

## Tips

- **Pattern Search**: Use `*` as a wildcard (e.g., `user:*` finds all keys starting with "user:").
- **JSON Values**: Enter valid JSON when editing to store objects or arrays.
- **Help Button**: Each view has a help button with context-specific tips.
- **Refresh**: Use the refresh action to update key lists and statistics.
