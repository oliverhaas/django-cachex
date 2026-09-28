"""Writes that left stale entries behind under django-cachalot's timestamp invalidation."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from django.conf import settings
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.db import connection

from tests.orm.app.models import Test

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.django_db(transaction=True)

needs_shared_backends = pytest.mark.skipif(
    connection.vendor != "postgresql"
    or settings.CACHES[DEFAULT_CACHE_ALIAS]["BACKEND"] == "django_cachex.cache.LocMemCache",
    reason="needs a database and a cache shared between processes",
)


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


def test_reader_clock_ahead_of_writer(mocker):
    real_time = time.time
    mocker.patch("time.time", side_effect=lambda: real_time() + 60)
    assert Test.objects.first() is None
    mocker.stopall()
    t = Test.objects.create(name="test")

    assert Test.objects.first() == t


@needs_shared_backends
def test_other_process_writes_between_query_and_caching():
    created = []

    def write_after_read(execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        created.append(int(run_in_other_process("create", "test")))
        return result

    with connection.execute_wrapper(write_after_read):
        assert Test.objects.first() is None

    expected = Test.objects.get(pk=created[0])
    assert Test.objects.first() == expected


@needs_shared_backends
def test_other_process_reads_during_autocommit_write():
    results = []

    def read_before_write(execute, sql, params, many, context):
        results.append(run_in_other_process("first"))
        return execute(sql, params, many, context)

    with connection.execute_wrapper(read_before_write):
        t = Test.objects.create(name="test")

    assert results == [""]
    assert run_in_other_process("first") == str(t.pk)
