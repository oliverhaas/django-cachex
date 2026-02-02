# django-cachex Admin Requirements

This document specifies the feature requirements for the django-cachex cache admin interface. These specifications serve as the target architecture and should guide implementation and testing.

## Overview

The cache admin provides a Django admin interface for inspecting and managing cache backends configured in the Django project.

### Architecture

The admin uses a **CacheService** class that expects a cachex-like interface. Non-cachex backends are adapted via **wrapper classes**.

```
┌─────────────────────────────────────────────────────────────┐
│                      CacheService                           │
│  (single class expecting cachex-like interface)             │
│  - keys(), ttl(), type(), get(), set(), hset(), etc.        │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   Native (cachex)      LocMemWrapper       DatabaseWrapper
   - ValkeyCache        - adapts LocMem     - adapts DB cache
   - RedisCache           to cachex API       to cachex API
```

### Support Levels

| Level | Description | Backends |
|-------|-------------|----------|
| `native` | Full support - all features, works directly with CacheService | `ValkeyCache`, `RedisCache`, `KeyValueCache` |
| `wrapped` | Adapted support - wrapper provides cachex-like interface, unsupported operations raise `NotSupportedError` | `LocMemCache`, `DatabaseCache`, `FileBasedCache`, `PyMemcacheCache`, `PyLibMCCache` |

**Error Handling:** Views use try/except to catch `NotSupportedError` and display warning messages to users. No feature detection checks in UI code.

---

## Views

### 1. Cache List View (Index)

**URL:** `/admin/django_cachex/cache/`

**Purpose:** Display all configured cache instances and provide navigation to key browsers.

#### Required Elements

- **Page title:** "Cache Admin - Instances"
- **Breadcrumbs:** Home > Cache Instances
- **Object tools:**
  - Help button (toggles help message display)

#### Data Table

| Column | Description |
|--------|-------------|
| Checkbox | For bulk selection |
| Cache Name | Links to key browser for that cache |
| Backend | Full backend class path (monospace) |
| Location | Cache server location/path |
| Support | Badge showing support level (native/wrapped) |

#### Actions

- **Flush selected caches:** Bulk action to clear selected caches
  - Requires confirmation dialog
  - Shows success message with count of flushed caches
  - Shows warning if flush fails for any cache

#### Filtering

- **By Support:** Filter by support level (All, native, wrapped)
- **Search:** Filter by cache name, backend, or location

#### Pagination

- Shows total count of cache instances

#### Empty State

- If no caches configured: Show configuration help with example `CACHES` setting

---

### 2. Key Browser View (Key Search)

**URL:** `/admin/django_cachex/cache/<cache_name>/keys/`

**Purpose:** Browse, search, and manage keys in a specific cache.

#### Required Elements

- **Page title:** "Keys in '<cache_name>'"
- **Breadcrumbs:** Home > Cache Instances > {cache_name}
- **Object tools:**
  - Help button
  - Info button (links to cache info view)
  - Slow Log button (links to slow log view)
  - Add key button - uses `addlink` class

#### Search

- **Search box:** Pattern-based search supporting wildcards (`*`, `?`)
  - Empty search defaults to `*` (all keys)
  - Exact key lookup (no wildcards): Direct key fetch
- **Results count:** Shown next to search box

#### Data Table

| Column | Description |
|--------|-------------|
| Checkbox | For bulk selection |
| Key | Links to key detail view (monospace) |
| Type | Type badge (string/list/set/hash/zset/stream) |
| TTL | Time-to-live in seconds, or "No expiry" |
| Size | Length for collections, bytes for strings |

#### Actions

- **Delete selected keys:** Bulk delete action
  - Requires confirmation dialog
  - Shows success message with count

#### Pagination

- Standard Django admin pagination
- Shows: Previous | 1 | 2 | ... | N | Next
- Shows total key count and current range

---

### 3. Key Detail View

**URL:** `/admin/django_cachex/cache/<cache_name>/keys/<key>/`

**Purpose:** View and manage a specific cache key's value and metadata.

#### Required Elements

- **Page title:** "Key: {key}"
- **Breadcrumbs:** Home > Cache Instances > {cache_name} > {key}
- **Object tools:**
  - Help button

#### Key Information Section (All Types)

| Field | Description |
|-------|-------------|
| Key | User-facing key name (monospace) |
| Cache | Cache name with metadata (location, prefix, version) |
| Raw Key | Full Redis key with prefix (monospace, quiet) |
| Type | Type badge with count (e.g., "list (5 items)") |
| TTL | Time-to-live in seconds, or "No expiry" |

#### TTL Controls

- **Set TTL form:** Number input + "Set TTL" button
- **Persist button:** Remove TTL (only shown if key has TTL)
  - Requires confirmation dialog

#### Key Not Found State

- Error note: "This key does not exist in the cache."

---

### 3.1 String Type Detail

**Condition:** `redis_type == 'string'` or no type data available

#### Value Section

- **Textarea:** Editable value (monospace, 12 rows)
- **TTL field:** Optional TTL in seconds (part of main form)

#### String Operations Bar

Only shown when value is numeric:

| Operation | Inputs |
|-----------|--------|
| Increment | Delta input (default: 1) + button |
| Decrement | Delta input (default: 1) + button |

#### Submit Row

- **Save button:** Submit form (default style)
- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 3.2 List Type Detail

**Condition:** `redis_type == 'list'`

#### List Items Section

- **Header:** "List Items ({count})"

#### Operations Bar

| Group | Operations |
|-------|------------|
| Pop | Pop Left (with count), Pop Right (with count) |
| Push | Push Left (with value), Push Right (with value) |
| Trim | Trim (start index, stop index) - requires confirmation |

#### Items Table

| Column | Description |
|--------|-------------|
| Index | 0-based index (quiet style) |
| Value | Item value (monospace) |
| Actions | Remove button |

#### Submit Row

- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 3.3 Set Type Detail

**Condition:** `redis_type == 'set'`

#### Set Members Section

- **Header:** "Set Members ({count})"

#### Operations Bar

| Group | Operations |
|-------|------------|
| Pop | Pop Random (with count) |
| Add | Add Member (with value) |

#### Members Table

| Column | Description |
|--------|-------------|
| Member | Member value (monospace) |
| Actions | Remove button |

#### Submit Row

- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 3.4 Hash Type Detail

**Condition:** `redis_type == 'hash'`

#### Hash Fields Section

- **Header:** "Hash Fields ({count})"

#### Operations Bar

| Operation | Inputs |
|-----------|--------|
| Set Field | Field name + value + button |

#### Fields Table

| Column | Description |
|--------|-------------|
| Field | Field name (monospace) |
| Value | Editable text input with current value |
| Actions | Update button + Delete button |

#### Submit Row

- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 3.5 Sorted Set Type Detail

**Condition:** `redis_type == 'zset'`

#### Sorted Set Members Section

- **Header:** "Sorted Set Members ({count})"

#### Operations Bar

| Group | Operations |
|-------|------------|
| Pop | Pop Min, Pop Max |
| Add | Member input + Score input + Flags (NX, XX, GT, LT) + button |

**ZADD Flags:**
- NX: Only add new elements
- XX: Only update existing elements
- GT: Only update if new score > current
- LT: Only update if new score < current

#### Members Table

| Column | Description |
|--------|-------------|
| Member | Member value (monospace) |
| Score | Editable number input with current score |
| Actions | Update button + Remove button |

#### Submit Row

- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 3.6 Stream Type Detail

**Condition:** `redis_type == 'stream'`

#### Stream Entries Section

- **Header:** "Stream Entries ({count})"

#### Operations Bar

| Group | Operations |
|-------|------------|
| Add | Field name + value + "Add Entry" button |
| Trim | Max length input + "Trim" button (requires confirmation) |

#### Entries Table

| Column | Description |
|--------|-------------|
| Entry ID | Stream entry ID (monospace, quiet) |
| Fields | All field:value pairs (monospace) |
| Actions | Delete button |

#### Submit Row

- **Close link:** Return to key browser
- **Delete link:** Delete key with confirmation

---

### 4. Add Key View

**URL:** `/admin/django_cachex/cache/<cache_name>/add/`

**Purpose:** Create a new cache key of any supported type.

#### Required Elements

- **Page title:** "Add key to '{cache_name}'"
- **Breadcrumbs:** Home > Cache Instances > {cache_name} > Add Key
- **Object tools:**
  - Help button

#### Form Fields

| Field | Description | Validation |
|-------|-------------|------------|
| Key Name | Text input (monospace) | Required |
| Type | Dropdown (String, List, Set, Hash, Sorted Set) | Required |
| Timeout | Number input (seconds) | Optional, >= 0 |

#### Type-Specific Value Fields

| Type | Field | Description |
|------|-------|-------------|
| String | Value textarea | JSON auto-parsed |
| List | Items textarea | One item per line |
| Set | Members textarea | One member per line |
| Hash | Fields textarea | JSON object |
| Sorted Set | Members textarea | JSON object with member:score pairs |

#### Client-Side Validation

- Key name required
- Hash: Valid JSON object
- Sorted Set: Valid JSON object with numeric scores

#### Submit Row

- **Save button:** Create key and return to key browser
- **Save and add another button:** Create key and show empty form
- **Cancel link:** Return to key browser

---

### 5. Cache Info View

**URL:** `/admin/django_cachex/cache/<cache_name>/info/`

**Purpose:** Display detailed server information for a cache instance.

#### Required Elements

- **Page title:** "Cache Info: {cache_name}"
- **Breadcrumbs:** Home > Cache Instances > {cache_name} > Info
- **Object tools:**
  - Back to Keys link
  - Help button

#### Configuration Section

| Field | Description |
|-------|-------------|
| Backend | Full backend class path |
| Location | Server location |
| Key Prefix | Configured key prefix |
| Version | Cache version number |

#### Server Section (native backends)

| Field | Description |
|-------|-------------|
| Redis/Valkey Version | Server version |
| Operating System | Server OS |
| Architecture | 32/64-bit |
| TCP Port | Server port |
| Uptime | Days and seconds |
| Process ID | Server PID |

#### Memory Section (native backends)

| Field | Description |
|-------|-------------|
| Used Memory | Human-readable + bytes |
| Peak Memory | Human-readable + bytes |
| Max Memory | Configured limit |
| Eviction Policy | Memory eviction policy |

#### Clients Section (native backends)

| Field | Description |
|-------|-------------|
| Connected Clients | Current connection count |
| Blocked Clients | Blocked connection count |

#### Statistics Section (native backends)

| Field | Description |
|-------|-------------|
| Total Connections | Lifetime connection count |
| Total Commands | Lifetime command count |
| Ops/sec | Current operations per second |
| Keyspace Hits | Cache hit count |
| Keyspace Misses | Cache miss count |
| Expired Keys | Expired key count |
| Evicted Keys | Evicted key count |

#### Keyspace Section (native backends)

- Grid of cards, one per database (db0, db1, etc.)
- Each card shows: Keys count, Expires count, Avg TTL

#### Submit Row

- **Back to Keys link**

---

### 6. Slow Log View

**URL:** `/admin/django_cachex/cache/<cache_name>/slowlog/`

**Purpose:** Display slow query log for performance analysis.

#### Required Elements

- **Page title:** "Slow Log: {cache_name}"
- **Breadcrumbs:** Home > Cache Instances > {cache_name} > Slow Log
- **Object tools:**
  - Back to Keys link
  - Info link
  - Help button

#### Count Selector

- Dropdown to select number of entries: 10, 25, 50, 100
- Shows total entries in log

#### Slow Queries Table

| Column | Description |
|--------|-------------|
| ID | Query ID |
| Time | Timestamp (Y-m-d H:i:s format) |
| Duration | Microseconds with color coding |
| Command | Full command (monospace, scrollable) |
| Client | Client address and name |

**Duration Color Coding:**
- >= 100ms (100,000 μs): Red (slow)
- >= 10ms (10,000 μs): Orange (medium)
- < 10ms: Green (fast)

#### Help Text

- Explanation of slow log threshold and duration highlighting

#### Submit Row

- **Back to Keys link**

---

### 7. Help System

**URL Parameter:** `?help=1` on any view

**Purpose:** Display contextual help information for each view.

#### Implementation

- Help button in object-tools toggles help display
- Help messages displayed as Django admin info messages
- Help content is HTML-formatted

#### Help Content by View

| View | Topics Covered |
|------|----------------|
| Index | Support levels, actions, navigation |
| Key Search | Search patterns, table columns, actions |
| Key Detail | Key information fields, type-specific operations |
| Key Add | Key naming, value formats, timeout |

---

## CacheService API

The `CacheService` class provides a unified interface for cache operations. All methods work with user-facing keys (without prefix).

### Core Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `keys(pattern)` | List keys matching glob pattern | `list[str]` |
| `get(key)` | Get key value | `Any` |
| `set(key, value, timeout)` | Set key value with optional TTL | `bool` |
| `delete(key)` | Delete a key | `bool` |
| `clear()` | Flush all keys in cache | `bool` |

### Key Metadata

| Method | Description | Returns |
|--------|-------------|---------|
| `ttl(key)` | Get remaining TTL in seconds | `int` (-1 = no expiry, -2 = not found) |
| `expire(key, timeout)` | Set TTL on key | `bool` |
| `persist(key)` | Remove TTL from key | `bool` |
| `type(key)` | Get Redis data type | `str` (string/list/set/hash/zset/stream) |
| `size(key)` | Get size/length of key | `int` |

### String Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `incr(key, delta)` | Increment numeric value | `int` |
| `decr(key, delta)` | Decrement numeric value | `int` |

### List Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `lrange(key, start, stop)` | Get list slice | `list` |
| `llen(key)` | Get list length | `int` |
| `lpush(key, *values)` | Push to left | `int` |
| `rpush(key, *values)` | Push to right | `int` |
| `lpop(key, count)` | Pop from left | `Any` |
| `rpop(key, count)` | Pop from right | `Any` |
| `lrem(key, count, value)` | Remove elements | `int` |
| `ltrim(key, start, stop)` | Trim list | `bool` |

### Set Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `smembers(key)` | Get all members | `set` |
| `scard(key)` | Get set size | `int` |
| `sadd(key, *members)` | Add members | `int` |
| `srem(key, *members)` | Remove members | `int` |
| `spop(key, count)` | Pop random members | `Any` |

### Hash Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `hgetall(key)` | Get all fields | `dict` |
| `hlen(key)` | Get field count | `int` |
| `hset(key, field, value)` | Set field | `int` |
| `hdel(key, *fields)` | Delete fields | `int` |

### Sorted Set Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `zrange(key, start, stop, withscores)` | Get range | `list` |
| `zcard(key)` | Get member count | `int` |
| `zadd(key, mapping, nx, xx, gt, lt)` | Add members | `int` |
| `zrem(key, *members)` | Remove members | `int` |
| `zpopmin(key, count)` | Pop min score | `list` |
| `zpopmax(key, count)` | Pop max score | `list` |

### Stream Operations

| Method | Description | Returns |
|--------|-------------|---------|
| `xrange(key, min, max, count)` | Get entries | `list` |
| `xlen(key)` | Get stream length | `int` |
| `xadd(key, fields)` | Add entry | `str` (entry ID) |
| `xdel(key, *ids)` | Delete entries | `int` |
| `xtrim(key, maxlen)` | Trim stream | `int` |

### Server Operations (native only)

| Method | Description | Returns |
|--------|-------------|---------|
| `info()` | Get server info | `dict` |
| `slowlog_get(count)` | Get slow queries | `list` |

---

## Wrapper Limitations

Wrappers adapt Django built-in caches to the CacheService API. Unsupported operations raise `NotSupportedError`.

### LocMemCacheWrapper

| Feature | Supported | Notes |
|---------|-----------|-------|
| keys() | ✓ | Via internal `_cache` dict |
| get/set/delete | ✓ | Standard Django API |
| ttl/expire/persist | ✗ | LocMem doesn't expose TTL |
| type() | ✗ | No type information |
| Data structures | ✗ | Only string-like values |
| info/slowlog | ✗ | No server stats |

### DatabaseCacheWrapper

| Feature | Supported | Notes |
|---------|-----------|-------|
| keys() | ✓ | Via database query |
| get/set/delete | ✓ | Standard Django API |
| ttl/expire | ✓ | Via `expires` column |
| persist | ✗ | DB cache requires expiry |
| type() | ✗ | No type information |
| Data structures | ✗ | Only string-like values |
| info/slowlog | ✗ | No server stats |

### FileCacheWrapper

| Feature | Supported | Notes |
|---------|-----------|-------|
| keys() | ✓ | Via filesystem scan |
| get/set/delete | ✓ | Standard Django API |
| ttl/expire/persist | ✗ | File cache doesn't expose TTL |
| type() | ✗ | No type information |
| Data structures | ✗ | Only string-like values |
| info/slowlog | ✗ | No server stats |

### MemcachedCacheWrapper

| Feature | Supported | Notes |
|---------|-----------|-------|
| keys() | ✗ | Memcached doesn't support key listing |
| get/set/delete | ✓ | Standard Django API |
| ttl/expire/persist | ✗ | Memcached doesn't expose TTL |
| type() | ✗ | No type information |
| Data structures | ✗ | Only string-like values |
| info | ✓ | Via `stats` command |
| slowlog | ✗ | No slow log |

---

## UI/UX Requirements

### Django Admin Compatibility

- All views must extend `admin/base_site.html`
- Use standard Django admin CSS classes:
  - `module`, `aligned` for fieldsets
  - `submit-row` for action buttons
  - `deletelink`, `closelink` for links
  - `button`, `default` for buttons
  - `results` for tables
  - `paginator` for pagination
  - `object-tools` for toolbar buttons
  - `breadcrumbs` for navigation

### Confirmation Dialogs

Required for destructive actions:
- Flush cache
- Delete key(s)
- Trim list
- Trim stream
- Persist key (remove TTL)

### Error Handling

- **NotSupportedError:** Display as warning message to user
- **Connection errors:** Display as error message with details
- **Validation errors:** Display inline with form fields
- Use Django messages framework for operation results

### Accessibility

- Proper form labels
- Semantic HTML (th, scope, etc.)
- Keyboard navigation support

---

## Testing Requirements

Each requirement in this document should have corresponding test coverage:

1. **Unit tests:** CacheService methods, wrapper implementations
2. **View tests:** All views render correctly, handle POST actions
3. **Integration tests:** End-to-end workflows (add key, edit, delete)
4. **Permission tests:** Staff member required for all views
5. **Error handling tests:** NotSupportedError displays warning messages

Tests should verify:
- Correct elements displayed for each view
- Actions produce expected results
- Error states handled gracefully (try/except pattern)
- Pagination works correctly
- Search/filter works correctly
