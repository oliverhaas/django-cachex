"""Driver, serializer and compressor throughput benchmarks.

Parametrized tests:

- ``test_adapters_sync`` — fixed pickle serializer, varies the adapter.
  Isolates the adapter/parser/connection stack.
- ``test_serializers`` — fixed rust-valkey adapter, varies the serializer.
  Isolates serializer cost (adapter overhead is minimal at that point).
- ``test_compressors_macro`` — fixed rust-valkey + pickle, varies the
  compressor on a large payload. End-to-end ops/sec showing the compress
  cost vs network savings tradeoff in real cache calls.
- ``test_compressors_micro`` — pure compress/decompress in-process, no
  adapter or container. Reports ratio and MB/s for each compressor.
- ``test_adapters_request_cycle`` — same shape as ``test_adapters_sync`` but
  every cache op is wrapped in a real Django request cycle (URL resolve,
  middleware, view dispatch, signals). Direct comparison reveals the
  per-request overhead Django adds on top of the cache call itself.

Each test runs the workload K_RUNS times and feeds aggregated metrics into a
session-scoped ``results`` sink that prints a final table at session end.
"""

import pytest

from benchmarks.configs import (
    COMPRESSOR_CONFIGS,
    DRIVER_BY_ID,
    DRIVER_CONFIGS,
    SERIALIZER_BY_ID,
    SERIALIZER_CONFIGS,
)
from benchmarks.runner import (
    format_asgi_summary,
    format_summary,
    run_asgi_benchmark,
    run_async_benchmark,
    run_benchmark,
    run_compressor_micro,
    run_request_cycle_benchmark,
)

ASYNC_CONCURRENCY = 50

# ASGI benchmark knobs — kept short by default so the suite stays runnable
# in CI; bump these manually for hero numbers.
ASGI_DURATION_S = 20
ASGI_CONCURRENCY = 100
ASGI_WORKERS = 4


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_adapters_sync(driver, server_url, results, capsys) -> None:
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_benchmark(driver, pickle_serializer, location)
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("serializer", SERIALIZER_CONFIGS, ids=lambda c: c.id)
def test_serializers(serializer, server_url, results, capsys) -> None:
    rust_driver = DRIVER_BY_ID["redis-rs"]
    location = server_url(rust_driver.server)

    result = run_benchmark(rust_driver, serializer, location)
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("compressor", COMPRESSOR_CONFIGS, ids=lambda c: c.id)
def test_compressors_macro(compressor, server_url, results, capsys) -> None:
    rust_driver = DRIVER_BY_ID["redis-rs"]
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


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_adapters_async_serial(driver, server_url, results, capsys) -> None:
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_async_benchmark(driver, pickle_serializer, location, concurrency=1)
    result.driver_id = f"{driver.id}#async"
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_adapters_async_concurrent(driver, server_url, results, capsys) -> None:
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_async_benchmark(
        driver,
        pickle_serializer,
        location,
        concurrency=ASYNC_CONCURRENCY,
    )
    result.driver_id = f"{driver.id}#async{ASYNC_CONCURRENCY}"
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_adapters_request_cycle(driver, server_url, results, capsys) -> None:
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_request_cycle_benchmark(driver, pickle_serializer, location)
    # Tag the label so the final summary distinguishes request-cycle rows
    # from direct rows when both tests run in the same session.
    result.driver_id = f"{driver.id}#req"
    results.add(result)

    with capsys.disabled():
        print()
        print(format_summary(result))


_POOL_SIZE_SWEEP = (10, 25, 50, 100, 200, 500)


@pytest.mark.parametrize("max_conns", _POOL_SIZE_SWEEP, ids=lambda n: f"max={n}")
def test_pool_size_sweep(max_conns: int, server_url, asgi_results, capsys) -> None:
    """Sweep ``max_connections`` against a pool-based driver to find the elbow.

    Picks ``redis-py`` as the representative pool backend (rust-valkey
    multiplexes and ignores the cap; django (builtin) sets its own cap via
    the thread executor). Used to validate the default we ship with.
    """

    from benchmarks.configs import DriverConfig

    base = DRIVER_BY_ID["redis-py"]
    custom = DriverConfig(
        id=f"redis-py#max{max_conns}",
        backend=base.backend,
        options={**base.options, "max_connections": max_conns},
        server=base.server,
    )
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(custom.server)

    result = run_asgi_benchmark(
        custom,
        pickle_serializer,
        location,
        duration_s=ASGI_DURATION_S,
        concurrency=ASGI_CONCURRENCY,
        workers=ASGI_WORKERS,
    )
    asgi_results.add(result)

    with capsys.disabled():
        print()
        print(format_asgi_summary(result))


@pytest.mark.parametrize("driver", DRIVER_CONFIGS, ids=lambda c: c.id)
def test_adapters_asgi(driver, server_url, asgi_results, capsys) -> None:
    """Full-stack ASGI benchmark — granian + httpx + 6 cache ops per request.

    Mirrors django-vcache's ``bench_compare.py`` shape so numbers are
    directly comparable. To reproduce vcache's connection-leak claim
    against Django's built-in ``RedisCache`` you need network latency
    (``tc qdisc add dev eth0 root netem delay 1ms``); on localhost the
    leak doesn't manifest because sync_to_async threads finish before
    the pool can grow.
    """
    pickle_serializer = SERIALIZER_BY_ID["pickle"]
    location = server_url(driver.server)

    result = run_asgi_benchmark(
        driver,
        pickle_serializer,
        location,
        duration_s=ASGI_DURATION_S,
        concurrency=ASGI_CONCURRENCY,
        workers=ASGI_WORKERS,
    )
    asgi_results.add(result)

    with capsys.disabled():
        print()
        print(format_asgi_summary(result))


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
