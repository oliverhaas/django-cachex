# Why a custom `LocMemCache`?

django-cachex ships its own `LocMemCache` rather than backing a `RespCache`
with [fakeredis](https://github.com/cunla/fakeredis). This page captures
the reasoning so the next maintainer doesn't have to re-litigate the
trade-off from first principles.

## What our `LocMemCache` does

`django_cachex.cache.LocMemCache` extends Django's built-in
`LocMemCache` with the full cachex extension surface (lists, sets,
hashes, sorted sets, TTL ops, key scanning, admin info), implemented
natively against the underlying in-process dict.

It mirrors RESP semantics where it counts:

- A key's RESP type is determined by the command that wrote it. A plain
  Python `list` stored via `cache.set()` is opaque ("string" in RESP
  terms); only values created via `lpush`/`rpush` carry the `_List` tag
  and are accepted by the list typed accessor. Cross-type access raises
  `WrongTypeError` (a `TypeError` subclass), the same exception the
  RESP adapters raise after translating their backend's `WRONGTYPE`
  response.
- Tagged collections (`_List`/`_Set`/`_Hash`/`_ZSet`) live as long-lived
  Python objects in `self._collections`, separate from Django's
  pickled-bytes `self._cache`. Mutations are in-place under the cache
  lock; no pickle round-trip per op.
- Sorted sets carry a `sortedcontainers.SortedList` sidecar, so
  `zrange`/`zrank`/`zpopmin` are O(log N) instead of the O(N log N)
  re-sort the naive dict-based approach pays.

## Why not just use fakeredis?

[fakeredis](https://github.com/cunla/fakeredis) is a pure-Python Redis
emulator with full protocol fidelity. In principle we could delete
~1500 lines of `LocMemCache` code and ship a `RedisCache` configured
with a `FakeRedis` client instead. We benchmarked it; the answer is no.

### Per-op latency (microseconds, single-threaded, best of 3)

| Op | LocMem (any N) | fakeredis-backed (any N) | Real Redis container | AWS ElastiCache |
|---|---|---|---|---|
| `set`/`get`/`del` | 2-3 | ~620 | ~100 | ~1000 |
| `lpush`/`lpop` | 2-3 | ~620 | ~100 | ~1000 |
| `zrange` head 10 | 2.5 | ~680 | ~100 | ~1000 |
| `zrank` | 1.5-2 | ~620 | ~100 | ~1000 |
| `hgetall` (10k fields) | 38 | 14 600 | n/a | n/a |
| `lrange` 0..-1 (10k) | 52 | 21 500 | n/a | n/a |
| `hset` (10k-field hash) | 1.7 | 1 600 | n/a | n/a |

(Reproduce: `python -m django_cachex.benchmarks.locmem_vs_fakeredis`
isn't checked in; it's a one-off, see the commit that introduced this
page for the script.)

### What this tells us

1. **fakeredis has a ~600 µs floor regardless of op or N.** That's the
   cost of the redis-py client serializing the command to RESP wire
   format, fakeredis interpreting it, formatting the response, and
   redis-py deserializing back. Even though everything is in-process,
   the protocol layer dominates.
2. **fakeredis lands in the same ballpark as Redis on localhost**: it
   behaves like Redis-on-localhost for latency, ~6x slower than a
   container on the same pod.
3. **Our `LocMemCache` is ~250-400× faster per single op** than
   fakeredis, and the gap widens with N for collection-spanning ops
   (`hgetall`, `lrange 0..-1`) where fakeredis's internal data
   structures aren't tuned for big working sets.

### Where that matters

- **Test suites.** A 12k-test suite that touches the cache ten times
  per test would pay an extra ~70 s of wall-clock per run on the
  fakeredis path. Multiplied across CI, contributor laptops, and the
  inner-loop `pytest -k foo` cycle, that compounds.
- **Dev REPL feedback.** A cache op that takes ~1 ms instead of ~2 µs
  changes the feel of "let me poke this in the shell" from
  instantaneous to noticeable.
- **`TieredCache` L1 hits.** `LocMemCache` is the default L1 in front
  of a Redis L2. Inflating the L1 hit cost defeats the point.

### Where fakeredis would actually be useful

For testing the **adapter layer** (redis-py / valkey-py command paths,
the `WRONGTYPE` translation, pipelines, scripts) without standing up a
testcontainer Redis. We currently rely on a real Valkey/Redis container
for adapter integration tests, which is correct but requires Docker.

We haven't wired this in. It would be a `[testing]` extra and a
fixture, not a runtime dependency, and the testcontainer path is fast
enough today (~140 s for the full suite) that the marginal benefit
hasn't justified the setup. Worth revisiting if a contributor needs to
work without Docker, or if the suite gets meaningfully larger.

## Latency context

For perspective, where each backend lives on the latency curve:

```
LocMemCache (in-process):   ~2 µs    (0.002 ms)
fakeredis + RespCache:      ~620 µs  (0.6 ms)
Redis on same pod:          ~100 µs  (0.1 ms)
AWS ElastiCache (cross-AZ): ~1000 µs (1.0 ms)
```

`LocMemCache` is the fastest option by two orders of magnitude. It's
also the only option that doesn't pay protocol-serialization cost. Use
it as L1 in `TieredCache`, as the dev/test backend, and anywhere
in-process caching dominates the access pattern.
