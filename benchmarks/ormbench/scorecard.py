"""Cases where a cached query result can differ from the database, each checked against an uncached read."""

import os
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.db import connection, connections, transaction
from django.db.models import F, Subquery
from django.db.models.functions import Coalesce, Now, Random
from django.utils import timezone

from benchmarks.ormbench.models import Child, Counter, Item, Ledger, Legacy, Note, NoteCount, Parent, Threshold

LIBRARY = os.environ.get("BENCH_ORM_CONTENDER", "none").partition("+")[0]


@dataclass(frozen=True)
class Case:
    name: str
    # What a failing contender serves.
    failure: str
    check: Callable[[], bool]


CASES: list[Case] = []


def _case(name: str, failure: str = "stale") -> Callable[[Callable[[], bool]], Callable[[], bool]]:
    def register(check: Callable[[], bool]) -> Callable[[], bool]:
        CASES.append(Case(name, failure, check))
        return check

    return register


def _uncached() -> AbstractContextManager[Any]:
    if LIBRARY == "cachalot":
        from cachalot.api import cachalot_disabled

        return cachalot_disabled()
    if LIBRARY == "cachex":
        from django_cachex.orm.api import orm_cache_disabled

        return orm_cache_disabled()
    return nullcontext()


def _matches_database(read: Callable[[], Any]) -> bool:
    served = read()
    with _uncached():
        return served == read()


def _in_thread(call: Callable[[], Any]) -> Any:
    """Run ``call`` on a thread of its own, with its own database connection, as another process would."""
    outcome: dict[str, Any] = {}

    def target() -> None:
        try:
            outcome["value"] = call()
        except BaseException as e:
            outcome["error"] = e
        finally:
            connections.close_all()

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("value")


def _version() -> list[int]:
    return list(Counter.objects.filter(pk=2).values_list("version", flat=True))


@_case("Another process reads while an autocommit UPDATE runs")
def read_during_write() -> bool:
    _version()

    def read_first(execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any) -> Any:
        if sql.startswith("UPDATE"):
            _in_thread(_version)
        return execute(sql, params, many, context)

    with connection.execute_wrapper(read_first):
        Counter.objects.filter(pk=2).update(version=F("version") + 1)
    return _matches_database(_version)


@_case("An on_commit() hook hands the new row to another process")
def on_commit_handoff() -> bool:
    _version()
    seen = []
    with transaction.atomic():
        Counter.objects.filter(pk=2).update(version=F("version") + 1)
        transaction.on_commit(lambda: seen.append(_in_thread(_version)))
    with _uncached():
        return seen == [_version()]


@_case("Subquery nested in a filter() expression")
def nested_subquery() -> bool:
    Threshold.objects.filter(pk=1).update(level=5)
    Item.objects.create(name="nested", qty=3)

    def read() -> list[int]:
        level = Coalesce(Subquery(Threshold.objects.filter(pk=1).values("level")), 0)
        return list(Item.objects.filter(name="nested", qty__gte=level).values_list("pk", flat=True))

    read()
    Threshold.objects.filter(pk=1).update(level=1)
    return _matches_database(read)


@_case("Now() nested in a filter() expression")
def nested_now() -> bool:
    Item.objects.create(name="expiring", due=timezone.now() + timedelta(hours=1, milliseconds=300))

    def read() -> list[int]:
        due_soon = Now() + timedelta(hours=1)
        return list(Item.objects.filter(name="expiring", due__lt=due_soon).values_list("pk", flat=True))

    read()
    time.sleep(0.5)
    return _matches_database(read)


@_case("order_by(Random())", failure="same rows every time")
def random_order() -> bool:
    def read() -> tuple[int, ...]:
        return tuple(Item.objects.order_by(Random()).values_list("pk", flat=True)[:5])

    return len({read() for _ in range(5)}) > 1


@_case("JSON filter values that differ after 35 characters", failure="another query's rows")
def long_json() -> bool:
    text = "x" * 60
    Item.objects.bulk_create([Item(name="json", data={"text": text, "n": n}) for n in (1, 2)])

    def read(n: int) -> list[int]:
        return list(Item.objects.filter(data={"text": text, "n": n}).values_list("pk", flat=True))

    read(1)
    return _matches_database(lambda: read(2))


@_case("Rows the database deletes in cascade (DB_CASCADE)")
def db_cascade() -> bool:
    parent = Parent.objects.create()
    Child.objects.create(parent=parent)

    def read() -> int:
        return Child.objects.count()

    read()
    Parent.objects.filter(pk=parent.pk).delete()
    return _matches_database(read)


@_case("Raw TRUNCATE")
def raw_truncate() -> bool:
    Note.objects.create(text="truncated")

    def read() -> int:
        return Note.objects.count()

    read()
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE ormbench_note")
    return _matches_database(read)


@_case("Raw REFRESH MATERIALIZED VIEW")
def refresh_view() -> bool:
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW ormbench_notecount")

    def read() -> list[int]:
        return list(NoteCount.objects.values_list("notes", flat=True))

    read()
    Note.objects.create(text="counted")
    with connection.cursor() as cursor:
        cursor.execute("REFRESH MATERIALIZED VIEW ormbench_notecount")
    return _matches_database(read)


@_case("Raw UPDATE of a table named in mixed case")
def mixed_case_table() -> bool:
    def read() -> list[int]:
        return list(Legacy.objects.values_list("value", flat=True))

    read()
    with connection.cursor() as cursor:
        cursor.execute('UPDATE "OrmBench_Legacy" SET value = value + 1')
    return _matches_database(read)


@_case("Raw INSERT into a table Django does not manage")
def unmanaged_table() -> bool:
    def read() -> int:
        return Ledger.objects.count()

    read()
    with connection.cursor() as cursor:
        cursor.execute("INSERT INTO ormbench_ledger (amount) VALUES (5)")
    return _matches_database(read)


@_case("A REPEATABLE READ transaction reads a row changed after its snapshot")
def repeatable_read() -> bool:
    def read() -> list[int]:
        return list(Threshold.objects.filter(pk=1).values_list("level", flat=True))

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                # Takes the snapshot.
                cursor.execute("SELECT 1")
            _in_thread(lambda: Threshold.objects.filter(pk=1).update(level=F("level") + 1))
            read()
        return _matches_database(read)
    finally:
        # The ORM cache treats the connection as a snapshot one until it reconnects.
        connection.close()


def run(args: dict[str, Any]) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for case in CASES:
        try:
            correct = case.check()
        except Exception as e:
            results.append({"case": case.name, "failure": case.failure, "correct": None, "error": repr(e)})
        else:
            results.append({"case": case.name, "failure": case.failure, "correct": correct})
    return {"cases": results}
