"""Lua scripts for the RESP-backed Semaphore.

Three Redis keys cooperate per semaphore name (passed as ``KEYS[1..3]``):

  1. ``{name}:state`` - hash with fields ``capacity``, ``used``.
  2. ``{name}:claims`` - hash mapping ``token`` -> ``weight``.
  3. ``{name}:queue`` - sorted set, score = enqueue timestamp (ms), member = ``token``.

All three carry a guard TTL, refreshed by ACQUIRE, EXTEND and RELEASE to twice
the longest lease (or waiter TTL) seen. A clean RELEASE drops them, so the TTL
only matters when the last holder dies without releasing: without it, a
semaphore named per job ("job:<id>") would leak three keys per name until that
exact name was acquired again. Twice the lease keeps the guard comfortably
longer than any claim it has to outlive, since losing the hashes while a claim
is still live would over-admit past capacity.

Plus per-token TTL keys the scripts manage internally:

  - ``{name}:state:claim:<token>`` - claim lease (``PX`` lease_ms); expiry
    marks the holder dead so its weight can be reaped.
  - ``{name}:queue:waiter:<token>`` - waiter liveness heartbeat (``PX``
    waiter_ttl_ms, refreshed on every acquire poll); expiry marks the waiter
    dead so its queue entry can be reaped.

The ``{name}`` hash-tag prefix colocates all keys for one semaphore on the
same cluster slot, which is what Redis Cluster requires for atomic multi-key
Lua. Cluster mode is supported (see ``RespCache.semaphore``).

Eviction resilience differs per key. ACQUIRE re-derives ``used`` from the
live claims and RELEASE refuses to resurrect a missing counter, so losing
``{name}:state`` or ``{name}:claims`` to a ``allkeys-*`` maxmemory policy
costs at most a queue position, never correctness. The per-token keys have
no such recovery: evicting a LIVE holder's ``{name}:state:claim:<token>``
is indistinguishable from that holder's lease expiring, so the next ACQUIRE
reaps its weight and hands the budget to someone else while it is still
running. Only ``maxmemory-policy noeviction`` protects them: ``allkeys-*``
can evict any key, and ``volatile-*`` evicts precisely the keys that carry
an expire, which is these. Point semaphores at a ``noeviction`` server (or
a dedicated database) if the mutual exclusion has to hold under memory
pressure.
"""

# ARGV: token, weight, capacity, lease_ms, waiter_ttl_ms
ACQUIRE_LUA = r"""
local state_key = KEYS[1]
local claims_key = KEYS[2]
local queue_key = KEYS[3]
local token = ARGV[1]
local weight = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local lease_ms = tonumber(ARGV[4])
local waiter_ttl_ms = tonumber(ARGV[5])

-- Guard TTL for the three shared keys, long enough to outlive every claim it
-- covers. Only lengthens: a short lease must not shorten a long one's guard.
local guard_ms = lease_ms * 2
if waiter_ttl_ms * 2 > guard_ms then
  guard_ms = waiter_ttl_ms * 2
end

local function bump(key, ms)
  if redis.call('PTTL', key) < ms then
    redis.call('PEXPIRE', key, ms)
  end
end

-- Sync capacity: caller's value wins (capacity-at-call-site).
local stored_cap = tonumber(redis.call('HGET', state_key, 'capacity') or '0')
if stored_cap ~= capacity then
  redis.call('HSET', state_key, 'capacity', capacity)
end

-- A missing 'used' field can't be trusted as zero: claims may still be live
-- and only the walk below can tell. Costs no extra call to notice.
local raw_used = redis.call('HGET', state_key, 'used')
local used = tonumber(raw_used or '0')

-- Head-of-queue check: caller admits only if queue is empty OR caller is at the head.
local head = redis.call('ZRANGE', queue_key, 0, 0)
local at_head = (#head == 0) or (head[1] == token)

-- Reap expired claims and dead waiters only when admission is otherwise
-- blocked. The O(N) walk over the claims hash recovers capacity from
-- crashed holders whose TTL key has expired. If the caller is at the head
-- AND fits using the visible 'used' counter, skip the walk entirely (the
-- fast path). When the caller is NOT at the head, or doesn't fit, the walk
-- still runs; the head waiter that polls next benefits from the
-- freshly-reaped used value. Worst case is unchanged O(N), best case is O(1).
if raw_used == false or not (at_head and used + weight <= capacity) then
  -- Derive 'used' from the surviving claims rather than subtracting a delta.
  -- The walk already visits every claim, so this costs nothing extra and it
  -- self-heals when 'used' and the claims hash disagree. Eviction under
  -- maxmemory is how they diverge: lose the claims hash and a delta-only
  -- reaper has nothing left to subtract, so 'used' stays pinned at capacity
  -- and every later acquire fails forever. Lose the state hash instead and
  -- 'used' reads 0, over-admitting past capacity, which is why a missing
  -- counter forces the walk even when the fast path would have admitted.
  -- The claims hash is the only record written on every admission and every
  -- release, and RELEASE deliberately refuses to resurrect a missing counter,
  -- so the live sum is the authoritative value whenever the two disagree.
  local claims = redis.call('HGETALL', claims_key)
  local live = 0
  for i = 1, #claims, 2 do
    local t = claims[i]
    local w = tonumber(claims[i+1])
    if redis.call('EXISTS', state_key .. ':claim:' .. t) == 0 then
      redis.call('HDEL', claims_key, t)
    else
      live = live + w
    end
  end
  if live ~= used then
    used = live
    redis.call('HSET', state_key, 'used', used)
  end
  -- Reap dead waiters: a queue entry whose liveness key has expired belongs
  -- to a waiter that crashed (or stalled past the TTL) after enqueueing.
  -- Left in place it would hold the head slot forever and block every later
  -- acquirer. A reaped-but-alive waiter re-enqueues at the tail on its next
  -- poll, so the worst case for a stalled process is a lost queue position.
  local queued = redis.call('ZRANGE', queue_key, 0, -1)
  local dropped = false
  for i = 1, #queued do
    local t = queued[i]
    if t ~= token and redis.call('EXISTS', queue_key .. ':waiter:' .. t) == 0 then
      redis.call('ZREM', queue_key, t)
      dropped = true
    end
  end
  if dropped then
    head = redis.call('ZRANGE', queue_key, 0, 0)
    at_head = (#head == 0) or (head[1] == token)
  end
end

if at_head and used + weight <= capacity then
  used = used + weight
  redis.call('HSET', state_key, 'used', used)
  redis.call('HSET', claims_key, token, weight)
  redis.call('SET', state_key .. ':claim:' .. token, '1', 'PX', lease_ms)
  redis.call('ZREM', queue_key, token)
  redis.call('DEL', queue_key .. ':waiter:' .. token)
  bump(state_key, guard_ms)
  bump(claims_key, guard_ms)
  bump(queue_key, guard_ms)
  return {'acquired', used, capacity}
end

-- Not admitted: enqueue if not already in queue; refresh liveness either way.
if redis.call('ZSCORE', queue_key, token) == false then
  -- Score from the SERVER clock, not the caller's. Admission is strictly
  -- head-of-queue, so a client-supplied score lets a host with a skewed
  -- clock sort ahead of every other host's waiters forever. TIME is the one
  -- clock every contender shares. It is the only place a wall clock is read:
  -- lease_ms and waiter_ttl_ms are relative PX durations, and the reapers key
  -- off key existence rather than off any stored timestamp.
  -- Reading TIME needs script *effects* replication so replicas and the AOF
  -- get the resulting ZADD rather than a re-run that reads their own clock.
  -- That has been the only mode since Redis 5, so TIME no longer taints a
  -- script; on Redis 4 and older this would have needed replicate_commands().
  local t = redis.call('TIME')
  local now_ms = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
  redis.call('ZADD', queue_key, now_ms, token)
end
redis.call('SET', queue_key .. ':waiter:' .. token, '1', 'PX', waiter_ttl_ms)
bump(state_key, guard_ms)
bump(claims_key, guard_ms)
bump(queue_key, guard_ms)
return {'queued', used, capacity}
"""


# ARGV: token (the claim's owner identifier), lease_ms
RELEASE_LUA = r"""
local state_key = KEYS[1]
local claims_key = KEYS[2]
local queue_key = KEYS[3]
local token = ARGV[1]
local guard_ms = tonumber(ARGV[2]) * 2

local function bump(key, ms)
  if redis.call('PTTL', key) < ms then
    redis.call('PEXPIRE', key, ms)
  end
end

local weight = tonumber(redis.call('HGET', claims_key, token) or '0')
if weight == 0 then
  -- Either already released, or the lease expired and we were reaped.
  return {'not_owned', 0}
end
redis.call('HDEL', claims_key, token)
redis.call('DEL', state_key .. ':claim:' .. token)

local raw_used = redis.call('HGET', state_key, 'used')
if raw_used == false then
  -- The state hash is gone: evicted, or its guard TTL lapsed after every
  -- lease it covered had already expired.
  -- Writing `0 - weight` clamped to 0 here would invent a counter that is
  -- wrong whenever other claims are still live, and ACQUIRE trusts a present
  -- counter on its fast path: it only re-derives `used` from the claims hash
  -- when the field is MISSING. Resurrecting it would therefore defeat exactly
  -- the self-heal that is supposed to cover this case and over-admit past
  -- capacity. Leave the field absent and let the next ACQUIRE do the walk.
  -- -1 reports "counter unknown" to the caller.
  return {'released', -1, 0}
end

local used = math.max(0, tonumber(raw_used) - weight)
if used == 0 and redis.call('HLEN', claims_key) == 0 then
  -- Last holder out drops the state hash so dynamically-named semaphores
  -- ("job:<id>") don't leak one un-expiring key per name. ACQUIRE recreates
  -- it (capacity comes from the caller on every call), and a missing counter
  -- is the safe direction: it forces the derive-from-claims walk.
  redis.call('DEL', state_key)
else
  redis.call('HSET', state_key, 'used', used)
  bump(state_key, guard_ms)
  bump(claims_key, guard_ms)
  bump(queue_key, guard_ms)
end
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
-- The claim hash entry is the source of truth for ownership; the TTL key is
-- just the liveness signal the reaper checks. If the TTL key already expired
-- (PTTL < 0) the holder is still the owner (not yet reaped), so re-establish
-- the lease with SET rather than PEXPIRE: PEXPIRE cannot recreate a missing
-- key, so it would silently no-op and leave the claim unprotected while still
-- reporting success.
local ttl_key = state_key .. ':claim:' .. token
local current = redis.call('PTTL', ttl_key)
if current < 0 then current = 0 end
local lease_ms = current + additional_ms
redis.call('SET', ttl_key, '1', 'PX', lease_ms)

-- Keep the shared keys ahead of the lease they now cover.
local guard_ms = lease_ms * 2
if redis.call('PTTL', state_key) < guard_ms then
  redis.call('PEXPIRE', state_key, guard_ms)
end
if redis.call('PTTL', claims_key) < guard_ms then
  redis.call('PEXPIRE', claims_key, guard_ms)
end
return 1
"""


# ARGV layout: token (the waiter to drop from the queue).
DEQUEUE_LUA = r"""
local queue_key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', queue_key, token)
redis.call('DEL', queue_key .. ':waiter:' .. token)
return 1
"""
