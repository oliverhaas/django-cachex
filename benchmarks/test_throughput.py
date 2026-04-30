"""Driver and serializer throughput benchmarks.

Two parametrized tests:

- ``test_drivers`` — fixed pickle serializer, varies the driver. Isolates the
  driver/parser/connection stack.
- ``test_serializers`` — fixed rust-valkey driver, varies the serializer.
  Isolates serializer cost (driver overhead is minimal at that point).

Each test runs the workload K_RUNS times and feeds aggregated metrics into a
session-scoped ``results`` sink that prints a final table at session end.
"""

from __future__ import annotations

import pytest

from benchmarks.configs import DRIVER_BY_ID, DRIVER_CONFIGS, SERIALIZER_BY_ID, SERIALIZER_CONFIGS
from benchmarks.runner import format_summary, run_benchmark


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_drivers(driver, server_url, results, capsys) -> None:
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_benchmark(driver, pickle_serializer, location)
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("serializer", SERIALIZER_CONFIGS, ids=lambda c: c.id)
def test_serializers(serializer, server_url, results, capsys) -> None:
    rust_driver = DRIVER_BY_ID["rust-valkey"]
    location = server_url(rust_driver.server)

    result = run_benchmark(rust_driver, serializer, location)
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))
