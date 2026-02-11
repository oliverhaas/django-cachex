# Cache Admin

django-cachex provides a Django admin interface for inspecting and managing caches. It allows you to browse cache keys, view their values, and perform operations like adding, editing, and deleting cache entries.

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

Different cache backends have different levels of support in the Cache Admin:

| Badge | Level | Description |
|-------|-------|-------------|
| **cachex** | Full Support | django-cachex backends (`ValkeyCache`, `RedisCache`, etc.) with full access to all features including key listing, pattern search, TTL inspection, and all data type operations. |
| **wrapped** | Wrapped Support | Django builtin backends (`LocMemCache`, `DatabaseCache`, etc.). Most features available through wrapper compatibility. |
| **limited** | Limited Support | Custom or unknown cache backends. Basic operations may work but key listing and advanced features may not be available. |

### Using Redis/Valkey?

If you are using Django's builtin Redis backend (`django.core.cache.backends.redis.RedisCache`), consider switching to django-cachex's `ValkeyCache` or `RedisCache` backends for full admin functionality including:

- Key browsing and pattern search
- TTL inspection and modification
- Native Redis data type support (lists, sets, hashes, sorted sets)
- Server info and memory statistics

See the [quickstart guide](../getting-started/quickstart.md) for migration instructions.

## Views

### Caches (Index)

The main view lists all configured caches with:

- **Cache Name**: The alias used in `settings.CACHES`
- **Backend**: The full backend class path
- **Location**: Connection string or path
- **Support Level**: Badge indicating feature availability

**Actions:**

- **Flush selected caches**: Delete all entries from selected caches (available for backends that support it)

### Key Browser

Click on a cache name to browse its keys. Features include:

- **Search**: Use wildcards (`*`) to filter keys (e.g., `user:*`, `*:session`)
- **Key Type**: Shows Redis/Valkey data types (string, list, set, hash, zset)
- **TTL**: Shows remaining time-to-live for each key
- **Pagination**: Browse large key sets with configurable page size

**Actions:**

- **Delete selected keys**: Remove multiple keys at once
- **Add key**: Create a new cache entry (top-right button)

### Key Detail

View and edit a specific key:

- **Value Display**: Shows the key's value (formatted JSON for objects/arrays)
- **Type Information**: For complex types (list, hash, set, zset), shows the data structure
- **TTL**: Shows remaining expiry time
- **Edit**: Modify value and timeout
- **Delete**: Remove the key

### Cache Info

View server information and statistics:

- **Configuration**: Backend, location, key prefix, version
- **Server**: Version, operating system, uptime
- **Memory**: Used memory, peak memory, max memory, eviction policy
- **Clients**: Connected clients, blocked clients
- **Statistics**: Total connections, commands processed, hit/miss rates
- **Keyspace**: Per-database key counts and TTL statistics

### Add Key

Create a new cache entry:

- **Key Name**: Unique identifier for the cache entry
- **Value**: JSON objects/arrays are parsed automatically, plain text for strings
- **Timeout**: Optional expiration time in seconds (leave empty for cache default)

## Backend Abilities

The admin adapts its interface based on what each backend supports:

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

- **Pattern Search**: Use `*` as a wildcard. For example, `user:*` finds all keys starting with "user:".
- **JSON Values**: When editing, you can enter valid JSON to store objects or arrays.
- **Help Button**: Each view has a help button that shows context-specific tips.
- **Refresh**: Use the refresh action to update key lists and statistics.
