# django-cachex Admin TODO

Remaining tasks for the cache admin interface.

---

## Architecture Refactor ✓

Completed CacheService + Wrappers pattern. See [REQUIREMENTS.md](REQUIREMENTS.md) for full API specification.

**Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│                      CacheService                           │
│  (single class expecting cachex-like interface)             │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   Native (cachex)      LocMemWrapper       DatabaseWrapper
```

**Completed:**
- [x] Create `CacheService` class in `django_cachex/admin/service.py`
- [x] Create `NotSupportedError` exception in `django_cachex/exceptions.py`
- [x] Create wrapper classes in `django_cachex/admin/wrappers.py`: `LocMemCacheWrapper`, `DatabaseCacheWrapper`, `FileCacheWrapper`, `MemcachedCacheWrapper`, `DummyCacheWrapper`
- [x] Update views to use `get_cache_service()` and try/except pattern
- [x] Rename settings: `CACHEX_PANEL` → `CACHEX_ADMIN`
- [x] Deprecate `backend.py` (kept for reference)

---

## Pending Improvements

### Key Detail View Styling

- [ ] Match Django admin change form styling exactly
  - Study `admin/change_form.html`, `admin/submit_line.html`
  - Goal: Visually indistinguishable from native Django admin forms

### Safe/Atomic Operations

Use Lua scripts for "check-then-update" operations to handle concurrent modifications.

- [ ] Zset score updates (check old score before update)
- [ ] Hash field updates (check old value)
- [ ] String value updates (check old value)

Example Lua script:
```lua
-- Only update score if member still has expected old score
local current_score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if current_score == nil then
    return {err = 'Member no longer exists'}
end
if tonumber(current_score) ~= tonumber(ARGV[2]) then
    return {err = 'Score was modified'}
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return {ok = 'Updated'}
```

### Serialization Handling

- [ ] Detect pickle vs JSON serialization
- [ ] Show warning for pickle-serialized keys (read-only)
- [ ] Add serialization indicator in Key Information section

---

## Testing

### E2E Tests (pytest-playwright)

- [ ] Login flow → navigate to cache admin
- [ ] Cache list page renders all configured caches
- [ ] Key list page with search/filter functionality
- [ ] Key detail page shows value and TTL
- [ ] Playwright page/browser fixtures
- [ ] Run playwright tests in CI (headless mode)

### Unfold Testing

- [ ] Add unfold to test matrix
- [ ] E2E tests with unfold enabled

---

## Future Features

### Advanced Features

- [ ] Lua script registry viewer
- [ ] Memory analysis per key pattern

### Example Projects

Future additions to `examples/full/`:
- [ ] Redis Stack with modules
- [ ] Redis Cluster (6 nodes)
- [ ] Master-Replica setup
- [ ] Sentinel setup

### Extended Builtin Backends

Provide extended versions of Django's builtin cache backends with data structure support.

| Backend | Extended Version | Priority |
|---------|------------------|----------|
| `LocMemCache` | `ExtendedLocMemCache` | High (testing) |
| `DatabaseCache` | `ExtendedDatabaseCache` | Medium |
| `FileBasedCache` | `ExtendedFileBasedCache` | Low |

---

## Configuration

```python
INSTALLED_APPS = [
    "django_cachex",
    "django_cachex.admin",  # or "django_cachex.unfold" for Unfold theme
]

CACHEX_ADMIN_SETTINGS = {
    "SCAN_COUNT": 100,        # Keys per page
    "MAX_VALUE_SIZE": 10000,  # Truncate large values
    "READONLY": False,        # Disable write operations
}
```

---

## References

- [dj-cache-panel](https://github.com/yassi/dj-cache-panel) - Main inspiration
- [django-redisboard](https://github.com/ionelmc/django-redisboard) - Custom view patterns
- [pytest-playwright](https://playwright.dev/python/docs/test-runners) - E2E testing
- [django-unfold](https://unfoldadmin.com/) - Modern Django admin theme
