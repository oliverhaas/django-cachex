"""Driver, serializer and compressor throughput benchmarks.

Parametrized tests:

- ``test_drivers`` — fixed pickle serializer, varies the driver. Isolates the
  driver/parser/connection stack.
- ``test_serializers`` — fixed rust-valkey driver, varies the serializer.
  Isolates serializer cost (driver overhead is minimal at that point).
- ``test_compressors_macro`` — fixed rust-valkey + pickle, varies the
  compressor on a large payload. End-to-end ops/sec showing the compress
  cost vs network savings tradeoff in real cache calls.
- ``test_compressors_micro`` — pure compress/decompress in-process, no
  driver or container. Reports ratio and MB/s for each compressor.

Each test runs the workload K_RUNS times and feeds aggregated metrics into a
session-scoped ``results`` sink that prints a final table at session end.
"""

from __future__ import annotations

import pytest

from benchmarks.configs import (
    COMPRESSOR_CONFIGS,
    DRIVER_BY_ID,
    DRIVER_CONFIGS,
    SERIALIZER_BY_ID,
    SERIALIZER_CONFIGS,
)
from benchmarks.runner import format_summary, run_benchmark, run_compressor_micro


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


@pytest.mark.parametrize("compressor", COMPRESSOR_CONFIGS, ids=lambda c: c.id)
def test_compressors_macro(compressor, server_url, results, capsys) -> None:
    rust_driver = DRIVER_BY_ID["rust-valkey"]
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(rust_driver.server)

    result = run_benchmark(
        rust_driver,
        pickle_serializer,
        location,
        compressor=compressor,
        payload_kind="large",
    )
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("compressor", COMPRESSOR_CONFIGS, ids=lambda c: c.id)
def test_compressors_micro(compressor, micro_results, capsys) -> None:
    micro = run_compressor_micro(compressor)
    if micro is None:
        pytest.skip("no-compression baseline has no micro numbers")
    micro_results.add(micro)

    with capsys.disabled():
        print()
        print(
            f"  compressor={micro.compressor_id}  "
            f"input={micro.input_bytes:,} B  "
            f"output={micro.output_bytes:,} B ({micro.ratio:.1%})  "
            f"compress={micro.compress_mb_s:,.1f} MB/s  "
            f"decompress={micro.decompress_mb_s:,.1f} MB/s",
        )
