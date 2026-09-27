"""Stale entries that timestamp invalidation, inherited from django-cachalot, leaves behind."""

import os
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import skipUnless
from unittest.mock import patch

import pytest
from django.conf import settings
from django.core.cache import DEFAULT_CACHE_ALIAS, caches
from django.db import connection
from django.test import TransactionTestCase

from tests.orm.app.models import Test
from tests.orm.utils import TestUtilsMixin

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def skewed_clock(offset: float) -> Iterator[None]:
    """Make the wall clock of this process run ``offset`` seconds off."""
    real_time = time.time

    def skewed_time() -> float:
        return real_time() + offset

    with ExitStack() as stack:
        stack.enter_context(patch("time.time", skewed_time))
        # Modules that did `from time import time` hold the real function.
        for name, module in list(sys.modules.items()):
            if name.startswith("django_cachex") and getattr(module, "time", None) is real_time:
                stack.enter_context(patch.object(module, "time", skewed_time))
        yield


def run_in_other_process(*args: str) -> str:
    """Run ``tests.orm.process`` with its own interpreter, database connection and cache client."""
    env = {**os.environ, "CACHEX_ORM_TEST_DB_NAME": connection.settings_dict["NAME"]}
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "tests.orm.process", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode:
        pytest.fail(f"tests.orm.process {' '.join(args)} failed:\n{completed.stderr}")
    return completed.stdout.strip()


class ClockSkewTestCase(TestUtilsMixin, TransactionTestCase):
    def tearDown(self):
        super().tearDown()
        # A result stamped in the future outlives the flush between tests.
        for cache in caches.all(initialized_only=True):
            cache.clear()

    @pytest.mark.xfail(strict=True, reason="timestamp invalidation trusts the clocks of every process")
    def test_reader_clock_ahead_of_writer(self):
        # A process whose clock runs ahead caches a result, then a process with
        # the right clock writes to the table.
        with skewed_clock(60):
            self.assertIsNone(Test.objects.first())
        t = Test.objects.create(name="test")

        self.assertEqual(Test.objects.first(), t)


# Processes share PostgreSQL and a Redis cache; SQLite in memory and LocMemCache
# are private to each process.
@skipUnless(
    connection.vendor == "postgresql"
    and settings.CACHES[DEFAULT_CACHE_ALIAS]["BACKEND"] != "django_cachex.cache.LocMemCache",
    "needs a database and a cache shared between processes",
)
class TwoProcessTestCase(TestUtilsMixin, TransactionTestCase):
    @pytest.mark.xfail(strict=True, reason="timestamp invalidation caches a result read before a concurrent write")
    def test_other_process_writes_between_query_and_caching(self):
        created = []

        def write_after_read(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            created.append(int(run_in_other_process("create", "test")))
            return result

        with connection.execute_wrapper(write_after_read):
            self.assertIsNone(Test.objects.first())

        expected = Test.objects.get(pk=created[0])
        self.assertEqual(Test.objects.first(), expected)

    @pytest.mark.xfail(strict=True, reason="timestamp invalidation caches a read taken during an autocommit write")
    def test_other_process_reads_during_autocommit_write(self):
        results = []

        def read_before_write(execute, sql, params, many, context):
            results.append(run_in_other_process("first"))
            return execute(sql, params, many, context)

        with connection.execute_wrapper(read_before_write):
            t = Test.objects.create(name="test")

        self.assertListEqual(results, [""])
        self.assertEqual(run_in_other_process("first"), str(t.pk))
