"""django-cachex's ORM cache against django-cachalot: correctness under races and clock skew, then speed."""

import pytest

from benchmarks.orm_runner import (
    format_latency,
    format_migrate,
    format_mixed,
    format_races,
    format_scorecard,
    format_sizes,
    format_skew,
    run_latency,
    run_migrate_noop,
    run_mixed,
    run_race,
    run_scorecard,
    run_sizes,
)

LIBRARIES = ["cachalot", "cachex"]
CACHED = [*LIBRARIES, *(f"{library}+tracking" for library in LIBRARIES)]
CONTENDERS = ["none", *CACHED]

RACE_READERS = 4
RACE_WRITES = 500
RACE_GAP_S = 0.01
RACE_SLOW_COMMIT_S = 0.002
RACES = {
    "autocommit": {"mode": "autocommit"},
    "autocommit-slow-commits": {"mode": "autocommit", "commit_s": RACE_SLOW_COMMIT_S},
    "atomic": {"mode": "atomic"},
    "atomic-slow-commits": {"mode": "atomic", "commit_s": RACE_SLOW_COMMIT_S},
    "atomic-hooks": {"mode": "atomic", "hook_s": 0.001},
    "atomic-paced": {"mode": "atomic", "pace_s": 0.001},
}

SKEW_WRITES = 500
SKEW_GAP_S = 0.02
SKEW_PACE_S = 0.001
SKEWS_S = [0.002, 0.005, 0.01, 0.02, 0.04]


def _skew_case(contender: str, *, reader_s: float = 0.0, writer_s: float = 0.0, gap_s: float = SKEW_GAP_S):
    if reader_s:
        clock = f"reader-ahead-{1000 * reader_s:g}ms"
    elif writer_s:
        clock = f"writer-ahead-{1000 * writer_s:g}ms"
    else:
        clock = "in-sync"
    return pytest.param(contender, reader_s, writer_s, gap_s, id=f"{contender}-{clock}-gap-{1000 * gap_s:g}ms")


SKEW_CASES = [
    _skew_case("cachalot"),
    *(_skew_case("cachalot", reader_s=skew) for skew in SKEWS_S),
    *(_skew_case("cachalot", writer_s=skew) for skew in SKEWS_S),
    _skew_case("cachalot", reader_s=0.015, gap_s=0.06),
    _skew_case("cachalot", reader_s=0.06, gap_s=0.06),
    _skew_case("cachex"),
    _skew_case("cachex", reader_s=SKEWS_S[-1]),
    _skew_case("cachex", writer_s=SKEWS_S[-1]),
]


@pytest.mark.parametrize("contender", ["none", *LIBRARIES])
def test_scorecard(contender, orm_env, orm_scorecard, capsys) -> None:
    result = run_scorecard(orm_env, contender)
    orm_scorecard.add(result)

    with capsys.disabled():
        print()
        print(format_scorecard([result]))


@pytest.mark.parametrize("scenario", RACES)
@pytest.mark.parametrize("contender", CACHED)
def test_races(contender, scenario, orm_env, orm_races, capsys) -> None:
    result = run_race(
        orm_env,
        contender,
        readers=RACE_READERS,
        writes=RACE_WRITES,
        gap_s=RACE_GAP_S,
        **RACES[scenario],
    )
    orm_races.add(result)

    with capsys.disabled():
        print()
        print(format_races([result]))


@pytest.mark.parametrize(("contender", "reader_s", "writer_s", "gap_s"), SKEW_CASES)
def test_clock_skew(contender, reader_s, writer_s, gap_s, orm_env, orm_skew, capsys) -> None:
    result = run_race(
        orm_env,
        contender,
        mode="atomic",
        readers=1,
        writes=SKEW_WRITES,
        gap_s=gap_s,
        pace_s=SKEW_PACE_S,
        reader_offset_s=reader_s,
        writer_offset_s=writer_s,
    )
    orm_skew.add(result)

    with capsys.disabled():
        print()
        print(format_skew([result]))


@pytest.mark.parametrize("contender", CONTENDERS)
def test_latency(contender, orm_env, orm_latency, capsys) -> None:
    result = run_latency(orm_env, contender, ops=2000, warmup=200)
    orm_latency.add(result)

    with capsys.disabled():
        print()
        print(format_latency([result]))


@pytest.mark.parametrize("contender", CONTENDERS)
def test_sizes(contender, orm_env, orm_sizes, capsys) -> None:
    result = run_sizes(orm_env, contender, sizes=[(1, 2000), (10, 2000), (100, 500), (1000, 100)], warmup=50)
    orm_sizes.add(result)

    with capsys.disabled():
        print()
        print(format_sizes([result]))


@pytest.mark.parametrize("write_share", [0.01, 0.1])
@pytest.mark.parametrize("contender", CONTENDERS)
def test_mixed(contender, write_share, orm_env, orm_mixed, capsys) -> None:
    result = run_mixed(orm_env, contender, write_share=write_share, keys=20, rows=100, ops=5000)
    orm_mixed.add(result)

    with capsys.disabled():
        print()
        print(format_mixed([result]))


@pytest.mark.parametrize("contender", LIBRARIES)
def test_migrate_noop(contender, orm_env, orm_migrate, capsys) -> None:
    result = run_migrate_noop(orm_env, contender, queries=100)
    orm_migrate.add(result)

    with capsys.disabled():
        print()
        print(format_migrate([result]))
