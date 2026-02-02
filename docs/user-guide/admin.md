# Cache Admin

django-cachex provides a Django admin interface for inspecting and managing cache instances. It allows you to browse cache keys, view their values, and perform operations like adding, editing, and deleting cache entries.

## Installation

Add `django_cachex.admin` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ... other apps
    "django.contrib.admin",
    "django_cachex",
    "django_cachex.admin",  # Cache admin interface
]
```

The cache admin will appear in the Django admin sidebar under "Caches".

## Support Levels

Different cache backends have different levels of support in the Cache Admin:

| Badge | Level | Description |
|-------|-------|-------------|
| **cachex** | Full Support | django-cachex backends (`ValkeyCache`, `RedisCache`, etc.) with full access to all features including key listing, pattern search, TTL inspection, and all data type operations. |
| **wrapped** | Wrapped Support | Django builtin backends (`LocMemCache`, `DatabaseCache`, etc.) and `django-redis`. Most features available through wrapper compatibility. |
| **limited** | Limited Support | Custom or unknown cache backends. Basic operations may work but key listing and advanced features may not be available. |

## Views

### Cache Instances (Index)

The main view lists all configured cache instances with:

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

### Add Key

Create a new cache entry:

- **Key Name**: Unique identifier for the cache entry
- **Value**: JSON objects/arrays are parsed automatically, plain text for strings
- **Timeout**: Optional expiration time in seconds (leave empty for cache default)

## Backend Abilities

The admin adapts its interface based on what each backend supports:

| Feature | cachex | wrapped | limited |
|---------|--------|---------|---------|
| List keys | Yes | Varies | No |
| Get key | Yes | Yes | Yes |
| Delete key | Yes | Yes | Yes |
| Edit key | Yes | Yes | Yes |
| Get TTL | Yes | Varies | No |
| Get type | Yes | No | No |
| Flush cache | Yes | Yes | Varies |

## Tips

- **Pattern Search**: Use `*` as a wildcard. For example, `user:*` finds all keys starting with "user:".
- **JSON Values**: When editing, you can enter valid JSON to store objects or arrays.
- **Help Button**: Each view has a help button (info icon) that shows context-specific tips.
