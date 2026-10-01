"""Roles of the ORM cache benchmark's worker processes, the scorecard aside."""

import itertools
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from multiprocessing.shared_memory import SharedMemory
from statistics import median, quantiles
from typing import Any

from django.core.cache import caches
from django.core.management import call_command
from django.db import connection, transaction
from django.db.models import F

from benchmarks.ormbench.models import Counter, Item, Legacy, Note, Threshold
from benchmarks.ormbench.shared import COMMITTED, PHASE, READY, RUN, STOP, WAIT

ITEMS = 1000
# Gives an item row about the size of a typical model's.
DESCRIPTION = "A benchmark item with a description long enough to give the row a realistic size. " * 2
# A miss takes three round trips of up to 1 ms each. This long after a write, every read that raced it has stored
# its result.
SETTLE_S = 0.005


class _Queries:
    """Counts the statements sent to the database: a read that sends none was served from the cache."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self, execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any) -> Any:
        self.count += 1
        return execute(sql, params, many, context)


class _NotCachedError(Exception):
    """Raised in place of a query, so a read the cache can't serve neither queries nor caches anything."""


def _refuse(execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any) -> Any:
    raise _NotCachedError


def _seed() -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "TRUNCATE ormbench_item, ormbench_counter, ormbench_threshold, ormbench_parent, ormbench_child, "
            'ormbench_note, "OrmBench_Legacy", ormbench_ledger RESTART IDENTITY',
        )
    Item.objects.bulk_create(Item(name=f"item {i}", qty=i % 10, description=DESCRIPTION) for i in range(ITEMS))
    Counter.objects.bulk_create([Counter(), Counter()])
    Threshold.objects.create(level=5)
    Legacy.objects.create()
    Note.objects.bulk_create(Note(text=f"note {i}") for i in range(10))
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW ormbench_notecount")
    caches["default"].flush_db()


def setup(args: dict[str, Any]) -> dict[str, Any]:
    call_command("migrate", verbosity=0)
    _seed()
    return {}


def reset(args: dict[str, Any]) -> dict[str, Any]:
    _seed()
    return {}


def _version() -> int:
    return Counter.objects.filter(pk=1).values_list("version", flat=True)[0]


def _cached_version() -> int | None:
    """The version the cache serves, or None if it serves none."""
    try:
        with connection.execute_wrapper(_refuse):
            return _version()
    except _NotCachedError:
        return None


@contextmanager
def _slots(name: str) -> Iterator[memoryview]:
    shm = SharedMemory(name=name, track=False)
    slots = shm.buf.cast("q")
    try:
        yield slots
    finally:
        slots.release()
        shm.close()


def _ready(slots: memoryview, index: int) -> None:
    slots[READY + index] = 1
    while slots[PHASE] == WAIT:
        time.sleep(0.001)


def _sleep_until(deadline: float) -> None:
    while (left := deadline - time.perf_counter()) > 0:
        time.sleep(left)


def race_reader(args: dict[str, Any]) -> dict[str, Any]:
    """Start a read every ``pace`` seconds, or back to back; a read is stale if older than a version committed before it."""
    pace = args["pace"]
    with _slots(args["shm"]) as slots:
        _version()
        _ready(slots, args["index"])
        queries = _Queries()
        reads = stale = 0
        # Readers starting in step would all race the writer at the same moments.
        due = time.perf_counter() + pace * random.Random(args["index"]).random()
        with connection.execute_wrapper(queries):
            while slots[PHASE] == RUN:
                if pace:
                    _sleep_until(due)
                    due += pace
                committed = slots[COMMITTED]
                if _version() < committed:
                    stale += 1
                reads += 1
    return {"reads": reads, "stale": stale, "hits": reads - queries.count}


def _publish(slots: memoryview, version: int) -> None:
    slots[COMMITTED] = version


def _commit(slots: memoryview, version: int, mode: str, hook: float) -> None:
    rows = Counter.objects.filter(pk=1)
    if mode == "atomic":
        with transaction.atomic():
            rows.update(version=version)
            transaction.on_commit(partial(_publish, slots, version))
            if hook:
                # Other hooks' work, like a task sent to a broker.
                transaction.on_commit(partial(time.sleep, hook))
    else:
        rows.update(version=version)
        _publish(slots, version)


def race_writer(args: dict[str, Any]) -> dict[str, Any]:
    """Write versions 1, 2, ... at Poisson times and count the writes the cache still hides at the next write.

    A write the next one follows within ``SETTLE_S`` goes unchecked, as the reads it raced can still store results.
    Commits take ``commit`` seconds longer, and on_commit() hooks after the publishing one take ``hook`` seconds.
    """
    writes = args["writes"]
    rng = random.Random(args["seed"])
    gaps = [rng.expovariate(1 / args["gap"]) for _ in range(writes + 1)]
    with _slots(args["shm"]) as slots:
        with connection.cursor() as cursor:
            cursor.execute("SELECT set_config('ormbench.commit_s', %s, false)", [repr(args["commit"])])
        _version()
        _ready(slots, args["index"])
        checked = hidden = 0
        start = due = written = time.perf_counter()
        for version in range(1, writes + 2):
            due += gaps[version - 1]
            _sleep_until(due)
            if version > 1 and time.perf_counter() - written >= SETTLE_S:
                cached = _cached_version()
                checked += 1
                hidden += cached is not None and cached < version - 1
            if version <= writes:
                _commit(slots, version, args["mode"], args["hook"])
                written = time.perf_counter()
        seconds = time.perf_counter() - start
        slots[PHASE] = STOP
    return {"writes": writes, "checked": checked, "hidden": hidden, "seconds": seconds}


def _timings(call: Callable[[], Any], ops: int, warmup: int) -> dict[str, float]:
    for _ in range(warmup):
        call()
    samples = []
    for _ in range(ops):
        start = time.perf_counter_ns()
        call()
        samples.append(time.perf_counter_ns() - start)
    return {"median_us": median(samples) / 1000, "p95_us": quantiles(samples, n=100)[94] / 1000}


def latency(args: dict[str, Any]) -> dict[str, Any]:
    ops, warmup = args["ops"], args["warmup"]
    fresh = itertools.count()

    def hit() -> None:
        list(Item.objects.filter(pk=7))

    def miss() -> None:
        # A parameter no earlier query had, on the same row.
        list(Item.objects.filter(pk=7, qty__gt=-1 - next(fresh)))

    def write() -> None:
        Item.objects.filter(pk=8).update(qty=F("qty") + 1)

    def commit() -> None:
        with transaction.atomic():
            Item.objects.filter(pk=8).update(qty=F("qty") + 1)

    phases = {"hit": hit, "miss": miss, "write": write, "commit": commit}
    return {name: _timings(call, ops, warmup) for name, call in phases.items()}


def sizes(args: dict[str, Any]) -> dict[str, Any]:
    return {
        str(rows): _timings(lambda rows=rows: list(Item.objects.filter(pk__lte=rows)), ops, args["warmup"])
        for rows, ops in args["sizes"]
    }


def mixed(args: dict[str, Any]) -> dict[str, Any]:
    """Read ``rows`` items by one of ``keys`` queries, or with probability ``write_share`` update a random item."""
    rng = random.Random(args["seed"])
    keys, rows, write_share, ops = args["keys"], args["rows"], args["write_share"], args["ops"]

    def read(first: int) -> None:
        list(Item.objects.filter(pk__gte=first, pk__lt=first + rows))

    for first in range(1, keys + 1):
        read(first)
    queries = _Queries()
    reads = hits = 0
    start = time.perf_counter()
    with connection.execute_wrapper(queries):
        for _ in range(ops):
            if rng.random() < write_share:
                Item.objects.filter(pk=rng.randint(1, ITEMS)).update(qty=F("qty") + 1)
                continue
            before = queries.count
            read(rng.randint(1, keys))
            reads += 1
            hits += queries.count == before
    return {"ops_per_s": ops / (time.perf_counter() - start), "hit_ratio": hits / reads}


def migrate_noop(args: dict[str, Any]) -> dict[str, Any]:
    """Cache ``queries`` queries, run a ``migrate`` with nothing to apply, and count the queries still cached."""
    reads = [lambda pk=pk: list(Item.objects.filter(pk=pk)) for pk in range(1, args["queries"] + 1)]
    for read in reads:
        read()
    call_command("migrate", verbosity=0)
    queries = _Queries()
    hits = 0
    with connection.execute_wrapper(queries):
        for read in reads:
            before = queries.count
            read()
            hits += queries.count == before
    return {"queries": len(reads), "hits": hits}


ROLES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "setup": setup,
    "reset": reset,
    "race_reader": race_reader,
    "race_writer": race_writer,
    "latency": latency,
    "sizes": sizes,
    "mixed": mixed,
    "migrate_noop": migrate_noop,
}
