"""Compare-and-swap helpers for safe admin operations.

Uses Lua scripts to atomically check that a value hasn't changed before
updating it. This prevents silent overwrites when two users edit the
same value concurrently in the admin.

Each CAS function returns:
    1  — success (value matched, update applied)
    0  — conflict (value changed since page load)
   -1  — gone (key/field/index no longer exists)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from django_cachex.cache.default import KeyValueCache

# =============================================================================
# SHA1 reader scripts (used at page load to fingerprint current values)
# =============================================================================

_GET_STRING_SHA1 = """\
local v = redis.call('GET', KEYS[1])
if v == false then return false end
return redis.sha1hex(v)
"""

_GET_HASH_FIELD_SHA1S = """\
local fields = redis.call('HGETALL', KEYS[1])
local result = {}
for i = 1, #fields, 2 do
    result[#result+1] = fields[i]
    result[#result+1] = redis.sha1hex(fields[i+1])
end
return result
"""

_GET_LIST_SHA1S = """\
local items = redis.call('LRANGE', KEYS[1], 0, -1)
local result = {}
for i = 1, #items do
    result[i] = redis.sha1hex(items[i])
end
return result
"""

_GET_LIST_SHA1S_RANGE = """\
local items = redis.call('LRANGE', KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]))
local result = {}
for i = 1, #items do
    result[i] = redis.sha1hex(items[i])
end
return result
"""

_GET_HASH_FIELDS_SHA1S = """\
local result = {}
for i = 1, #ARGV do
    local v = redis.call('HGET', KEYS[1], ARGV[i])
    if v ~= false then
        result[#result+1] = ARGV[i]
        result[#result+1] = redis.sha1hex(v)
    end
end
return result
"""

# =============================================================================
# CAS write scripts (used at form submit to atomically check-then-update)
# =============================================================================

_CAS_STRING_UPDATE = """\
local v = redis.call('GET', KEYS[1])
if v == false then return -1 end
if redis.sha1hex(v) == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
    return 1
end
return 0
"""

_CAS_HASH_UPDATE = """\
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v == false then return -1 end
if redis.sha1hex(v) == ARGV[2] then
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
    return 1
end
return 0
"""

_CAS_ZSET_SCORE_UPDATE = """\
local v = redis.call('ZSCORE', KEYS[1], ARGV[1])
if v == false then return -1 end
if tonumber(v) == tonumber(ARGV[2]) then
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
    return 1
end
return 0
"""

_CAS_LIST_UPDATE = """\
local v = redis.call('LINDEX', KEYS[1], tonumber(ARGV[1]))
if v == false then return -1 end
if redis.sha1hex(v) == ARGV[2] then
    redis.call('LSET', KEYS[1], tonumber(ARGV[1]), ARGV[3])
    return 1
end
return 0
"""


# =============================================================================
# SHA1 reader helpers (page load)
# =============================================================================


def get_string_sha1(cache: KeyValueCache, key: str) -> str | None:
    """Get SHA1 fingerprint of the raw encoded string value."""
    result = cache.eval_script(_GET_STRING_SHA1, keys=[key], pre_hook=keys_only_pre)
    if result is None:
        return None
    return result.decode() if isinstance(result, bytes) else str(result)


def get_hash_field_sha1s(cache: KeyValueCache, key: str) -> dict[str, str]:
    """Get SHA1 fingerprints of all hash field values.

    Returns:
        Dict mapping field names to their SHA1 hex strings.
    """
    result = cache.eval_script(_GET_HASH_FIELD_SHA1S, keys=[key], pre_hook=keys_only_pre)
    if not result:
        return {}
    sha1s: dict[str, str] = {}
    for i in range(0, len(result), 2):
        field = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
        sha1 = result[i + 1].decode() if isinstance(result[i + 1], bytes) else str(result[i + 1])
        sha1s[field] = sha1
    return sha1s


def get_list_sha1s(cache: KeyValueCache, key: str) -> list[str]:
    """Get SHA1 fingerprints of all list elements."""
    result = cache.eval_script(_GET_LIST_SHA1S, keys=[key], pre_hook=keys_only_pre)
    if not result:
        return []
    return [r.decode() if isinstance(r, bytes) else str(r) for r in result]


def get_list_sha1s_range(cache: KeyValueCache, key: str, start: int, stop: int) -> list[str]:
    """Get SHA1 fingerprints for a range of list elements.

    Args:
        start: Start index (inclusive), same semantics as LRANGE.
        stop: Stop index (inclusive), same semantics as LRANGE.
    """
    result = cache.eval_script(
        _GET_LIST_SHA1S_RANGE,
        keys=[key],
        args=[start, stop],
        pre_hook=keys_only_pre,
    )
    if not result:
        return []
    return [r.decode() if isinstance(r, bytes) else str(r) for r in result]


def get_hash_field_sha1s_for(cache: KeyValueCache, key: str, fields: list[str]) -> dict[str, str]:
    """Get SHA1 fingerprints for specific hash fields.

    Args:
        fields: List of field names to fingerprint.
    """
    if not fields:
        return {}
    result = cache.eval_script(
        _GET_HASH_FIELDS_SHA1S,
        keys=[key],
        args=fields,
        pre_hook=keys_only_pre,
    )
    if not result:
        return {}
    sha1s: dict[str, str] = {}
    for i in range(0, len(result), 2):
        field = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
        sha1 = result[i + 1].decode() if isinstance(result[i + 1], bytes) else str(result[i + 1])
        sha1s[field] = sha1
    return sha1s


# =============================================================================
# CAS update helpers (form submit)
# =============================================================================


def cas_update_string(
    cache: KeyValueCache,
    key: str,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a string value if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = key gone.
    """
    encoded = cache._cache.encode(new_value)
    return cache.eval_script(
        _CAS_STRING_UPDATE,
        keys=[key],
        args=[expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )


def cas_update_hash_field(
    cache: KeyValueCache,
    key: str,
    field: str,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a hash field value if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = field gone.
    """
    encoded = cache._cache.encode(new_value)
    return cache.eval_script(
        _CAS_HASH_UPDATE,
        keys=[key],
        args=[field, expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )


def cas_update_zset_score(
    cache: KeyValueCache,
    key: str,
    member: Any,
    expected_score: str,
    new_score: float,
) -> int:
    """Atomically update a sorted set member's score if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = member gone.
    """
    encoded_member = cache._cache.encode(member)
    return cache.eval_script(
        _CAS_ZSET_SCORE_UPDATE,
        keys=[key],
        args=[encoded_member, expected_score, new_score],
        pre_hook=keys_only_pre,
    )


def cas_update_list_element(
    cache: KeyValueCache,
    key: str,
    index: int,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a list element if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = index gone.
    """
    encoded = cache._cache.encode(new_value)
    return cache.eval_script(
        _CAS_LIST_UPDATE,
        keys=[key],
        args=[index, expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )
