"""Compare-and-swap helpers for safe admin operations.

Uses Lua scripts to atomically check that a value hasn't changed before
updating it. This prevents silent overwrites when two users edit the
same value concurrently in the admin.

Each CAS function returns:
    1: success (value matched, update applied)
    0: conflict (value changed since page load)
   -1: gone (key/field/index no longer exists)
   -2: target name already taken (hash field rename only)
"""

from typing import TYPE_CHECKING, Any

from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from django_cachex.cache.resp import RespCache


def supports_cas(cache: Any) -> bool:
    """Report whether ``cache`` can run the CAS scripts.

    ``BaseCachex`` declares ``eval_script`` and raises from it, so only the
    ``encode`` that every CAS helper calls tells the backends apart.
    """
    return hasattr(cache, "encode") and hasattr(cache, "eval_script")


# =============================================================================
# Reader scripts (used at page load to fingerprint current values)
# =============================================================================

# One script per type, so a concurrent write cannot pair a stale value
# with a fresh SHA1 between two separate reads.

_GET_STRING_WITH_SHA1 = """\
local v = redis.call('GET', KEYS[1])
if v == false then return false end
return {v, redis.sha1hex(v)}
"""

_GET_LIST_RANGE_WITH_SHA1S = """\
local items = redis.call('LRANGE', KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]))
local result = {}
for i = 1, #items do
    result[#result+1] = items[i]
    result[#result+1] = redis.sha1hex(items[i])
end
return result
"""

_GET_HASH_FIELDS_WITH_SHA1S = """\
local result = {}
for i = 1, #ARGV do
    local v = redis.call('HGET', KEYS[1], ARGV[i])
    if v ~= false then
        result[#result+1] = ARGV[i]
        result[#result+1] = v
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
    redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
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

_CAS_HASH_RENAME = """\
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v == false then return -1 end
if redis.sha1hex(v) ~= ARGV[3] then return 0 end
if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then return -2 end
redis.call('HSET', KEYS[1], ARGV[2], ARGV[4])
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
"""

_CAS_ZSET_SCORE_UPDATE = """\
local v = redis.call('ZSCORE', KEYS[1], ARGV[1])
if v == false then return -1 end
-- Compare the raw Redis-canonicalised ZSCORE string first; both sides
-- come from ZSCORE so identical scores yield identical strings without
-- going through ``tonumber``. Fall back to numeric equality only if the
-- literal strings differ, so a Python round-trip of an unchanged value
-- (``0.5`` read back as the string ``"0.5"``) doesn't trigger a spurious CAS conflict.
if v == ARGV[2] or tonumber(v) == tonumber(ARGV[2]) then
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
# Reader helpers (page load)
# =============================================================================


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def get_string_with_sha1(cache: RespCache, key: str) -> tuple[Any, str] | None:
    """Read a string value and its SHA1 fingerprint in one atomic call.

    Returns the decoded value with the fingerprint, or None for a missing key.
    """
    result = cache.eval_script(_GET_STRING_WITH_SHA1, keys=[key], pre_hook=keys_only_pre)
    if not result:
        return None
    raw, sha1 = result
    return cache.decode(raw), _text(sha1)


def get_list_range_with_sha1s(cache: RespCache, key: str, start: int, stop: int) -> list[tuple[Any, str]]:
    """Read a range of list elements with their SHA1 fingerprints in one atomic call.

    Args:
        start: Start index (inclusive), same semantics as LRANGE.
        stop: Stop index (inclusive), same semantics as LRANGE.
    """
    result = cache.eval_script(
        _GET_LIST_RANGE_WITH_SHA1S,
        keys=[key],
        args=[start, stop],
        pre_hook=keys_only_pre,
    )
    if not result:
        return []
    return [(cache.decode(result[i]), _text(result[i + 1])) for i in range(0, len(result), 2)]


def get_hash_fields_with_sha1s(cache: RespCache, key: str, fields: list[str]) -> list[tuple[str, Any, str]]:
    """Read the given hash fields with their SHA1 fingerprints in one atomic call.

    Fields that no longer exist are left out.
    """
    if not fields:
        return []
    result = cache.eval_script(
        _GET_HASH_FIELDS_WITH_SHA1S,
        keys=[key],
        args=fields,
        pre_hook=keys_only_pre,
    )
    if not result:
        return []
    return [(_text(result[i]), cache.decode(result[i + 1]), _text(result[i + 2])) for i in range(0, len(result), 3)]


# =============================================================================
# CAS update helpers (form submit)
# =============================================================================


def cas_update_string(
    cache: RespCache,
    key: str,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a string value if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = key gone.
    """
    encoded = cache.encode(new_value)
    return cache.eval_script(
        _CAS_STRING_UPDATE,
        keys=[key],
        args=[expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )


def cas_update_hash_field(
    cache: RespCache,
    key: str,
    field: str,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a hash field value if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = field gone.
    """
    encoded = cache.encode(new_value)
    return cache.eval_script(
        _CAS_HASH_UPDATE,
        keys=[key],
        args=[field, expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )


def cas_rename_hash_field(
    cache: RespCache,
    key: str,
    field: str,
    new_field: str,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically move a hash field to a new name if its value hasn't changed.

    Redis has no HRENAME, so a rename is HSET plus HDEL. Running the pair in a
    script keeps it atomic and costs a single round trip.

    Returns:
        1 = success, 0 = conflict, -1 = field gone, -2 = new name already taken.
    """
    encoded = cache.encode(new_value)
    return cache.eval_script(
        _CAS_HASH_RENAME,
        keys=[key],
        args=[field, new_field, expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )


def cas_update_zset_score(
    cache: RespCache,
    key: str,
    member: Any,
    expected_score: str,
    new_score: float,
) -> int:
    """Atomically update a sorted set member's score if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = member gone.
    """
    encoded_member = cache.encode(member)
    return cache.eval_script(
        _CAS_ZSET_SCORE_UPDATE,
        keys=[key],
        args=[encoded_member, expected_score, new_score],
        pre_hook=keys_only_pre,
    )


def cas_update_list_element(
    cache: RespCache,
    key: str,
    index: int,
    expected_sha1: str,
    new_value: Any,
) -> int:
    """Atomically update a list element if it hasn't changed.

    Returns:
        1 = success, 0 = conflict, -1 = index gone.
    """
    encoded = cache.encode(new_value)
    return cache.eval_script(
        _CAS_LIST_UPDATE,
        keys=[key],
        args=[index, expected_sha1, encoded],
        pre_hook=keys_only_pre,
    )
