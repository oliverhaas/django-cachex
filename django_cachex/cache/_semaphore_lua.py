"""Lua scripts for the RESP-backed Semaphore.

Three Redis keys cooperate per semaphore name (passed as ``KEYS[1..3]``):

  1. ``{name}:state`` - hash with fields ``capacity``, ``used``.
  2. ``{name}:claims`` - hash mapping ``token`` -> ``weight``.
  3. ``{name}:queue`` - sorted set, score = enqueue timestamp (ms), member = ``token``.

Plus a per-claim TTL key ``{name}:state:claim:<token>`` (string, PEXPIRE = lease_ms)
that the scripts manage internally.

The ``{name}`` hash-tag prefix colocates all keys for one semaphore on the
same cluster slot, which is what Redis Cluster requires for atomic multi-key
Lua. Cluster mode is supported (see ``RespCache.semaphore``).
"""

# ARGV: token, weight, capacity, lease_ms, now_ms
ACQUIRE_LUA = r"""
local state_key = KEYS[1]
local claims_key = KEYS[2]
local queue_key = KEYS[3]
local token = ARGV[1]
local weight = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local lease_ms = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])

-- Sync capacity: caller's value wins (capacity-at-call-site).
local stored_cap = tonumber(redis.call('HGET', state_key, 'capacity') or '0')
if stored_cap ~= capacity then
  redis.call('HSET', state_key, 'capacity', capacity)
end

local used = tonumber(redis.call('HGET', state_key, 'used') or '0')

-- Head-of-queue check: caller admits only if queue is empty OR caller is at the head.
local head = redis.call('ZRANGE', queue_key, 0, 0)
local at_head = (#head == 0) or (head[1] == token)

-- Reap expired claims only when admission is otherwise blocked. The O(N)
-- walk over the claims hash recovers capacity from crashed holders whose
-- TTL key has expired. If the caller is at the head AND fits using the
-- visible 'used' counter, skip the walk entirely (the fast path). When
-- the caller is NOT at the head, or doesn't fit, the walk still runs;
-- the head waiter that polls next benefits from the freshly-reaped used
-- value. Worst case is unchanged O(N), best case is O(1).
if not (at_head and used + weight <= capacity) then
  local claims = redis.call('HGETALL', claims_key)
  local reaped = 0
  for i = 1, #claims, 2 do
    local t = claims[i]
    local w = tonumber(claims[i+1])
    if redis.call('EXISTS', state_key .. ':claim:' .. t) == 0 then
      redis.call('HDEL', claims_key, t)
      reaped = reaped + w
    end
  end
  if reaped > 0 then
    used = math.max(0, used - reaped)
    redis.call('HSET', state_key, 'used', used)
  end
end

if at_head and used + weight <= capacity then
  used = used + weight
  redis.call('HSET', state_key, 'used', used)
  redis.call('HSET', claims_key, token, weight)
  redis.call('SET', state_key .. ':claim:' .. token, '1', 'PX', lease_ms)
  redis.call('ZREM', queue_key, token)
  return {'acquired', used, capacity}
end

-- Not admitted: enqueue if not already in queue.
if redis.call('ZSCORE', queue_key, token) == false then
  redis.call('ZADD', queue_key, now_ms, token)
end
return {'queued', used, capacity}
"""


# ARGV layout: token (the claim's owner identifier).
RELEASE_LUA = r"""
local state_key = KEYS[1]
local claims_key = KEYS[2]
local token = ARGV[1]

local weight = tonumber(redis.call('HGET', claims_key, token) or '0')
if weight == 0 then
  -- Either already released, or the lease expired and we were reaped.
  return {'not_owned', 0}
end
redis.call('HDEL', claims_key, token)
redis.call('DEL', state_key .. ':claim:' .. token)
local used = tonumber(redis.call('HGET', state_key, 'used') or '0')
used = math.max(0, used - weight)
redis.call('HSET', state_key, 'used', used)
return {'released', used, 0}
"""


# ARGV: token, additional_ms
EXTEND_LUA = r"""
local state_key = KEYS[1]
local claims_key = KEYS[2]
local token = ARGV[1]
local additional_ms = tonumber(ARGV[2])

if redis.call('HEXISTS', claims_key, token) == 0 then
  return 0
end
local ttl_key = state_key .. ':claim:' .. token
local current = redis.call('PTTL', ttl_key)
if current < 0 then current = 0 end
redis.call('PEXPIRE', ttl_key, current + additional_ms)
return 1
"""


# ARGV layout: token (the waiter to drop from the queue).
DEQUEUE_LUA = r"""
local queue_key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', queue_key, token)
return 1
"""
