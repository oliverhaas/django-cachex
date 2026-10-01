"""Worker processes and reports of the ORM cache benchmark (``benchmarks/test_orm.py``)."""

import json
import math
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import Any

from benchmarks.ormbench.shared import PHASE, READY, RUN, STOP
from benchmarks.runner import _render_table

ROOT = Path(__file__).resolve().parent.parent
WORKER_TIMEOUT_S = 600
READY_TIMEOUT_S = 120


@dataclass(frozen=True)
class OrmEnv:
    """Where the worker processes find PostgreSQL and Valkey."""

    database: dict[str, str]
    valkey_url: str


class Worker:
    """A process running one role of ``benchmarks/orm_worker.py``."""

    def __init__(
        self,
        env: OrmEnv,
        contender: str,
        role: str,
        args: dict[str, Any] | None = None,
        clock_offset: float = 0.0,
    ) -> None:
        self.role = role
        self._stdout = tempfile.TemporaryFile()  # noqa: SIM115 (closed in close())
        self._stderr = tempfile.TemporaryFile()  # noqa: SIM115
        self.process = subprocess.Popen(  # noqa: S603 (sys.executable and arguments of our own)
            [sys.executable, "-m", "benchmarks.orm_worker", role, json.dumps(args or {})],
            cwd=ROOT,
            env={
                **os.environ,
                "BENCH_ORM_CONTENDER": contender,
                "BENCH_ORM_PG_JSON": json.dumps(env.database),
                "BENCH_ORM_VALKEY_URL": env.valkey_url,
                "BENCH_ORM_CLOCK_OFFSET": repr(clock_offset),
            },
            stdout=self._stdout,
            stderr=self._stderr,
        )

    def check_alive(self) -> None:
        if self.process.poll() is not None:
            raise self._failure(f"exited with {self.process.returncode} before it was ready")

    def result(self, timeout: float = WORKER_TIMEOUT_S) -> dict[str, Any]:
        try:
            code = self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            self._kill()
            raise self._failure(f"did not finish within {timeout:.0f} s") from None
        if code != 0:
            raise self._failure(f"exited with {code}")
        self._stdout.seek(0)
        return json.loads(self._stdout.read().decode().strip().splitlines()[-1])

    def close(self) -> None:
        self._kill()
        self._stdout.close()
        self._stderr.close()

    def _kill(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait()

    def _failure(self, what: str) -> RuntimeError:
        self._stderr.seek(0)
        tail = self._stderr.read().decode(errors="replace")[-4000:]
        return RuntimeError(f"ORM benchmark worker {self.role!r} {what}:\n{tail}")


def run_role(env: OrmEnv, contender: str, role: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
    worker = Worker(env, contender, role, args)
    try:
        return worker.result()
    finally:
        worker.close()


def setup(env: OrmEnv) -> None:
    """Migrate and seed the database."""
    run_role(env, "none", "setup")


def reset(env: OrmEnv) -> None:
    """Seed the tables afresh and flush Valkey."""
    run_role(env, "none", "reset")


@dataclass
class ScorecardResult:
    contender: str
    cases: list[dict[str, Any]]


def run_scorecard(env: OrmEnv, contender: str) -> ScorecardResult:
    reset(env)
    return ScorecardResult(contender, run_role(env, contender, "scorecard")["cases"])


@dataclass
class RaceResult:
    contender: str
    mode: str
    readers: int
    writes: int
    gap_s: float
    checked: int
    hidden: int
    reads: int
    stale: int
    hits: int
    seconds: float
    pace_s: float = 0.0
    commit_s: float = 0.0
    hook_s: float = 0.0
    reader_offset_s: float = 0.0
    writer_offset_s: float = 0.0

    @property
    def skew_s(self) -> float:
        return abs(self.reader_offset_s - self.writer_offset_s)

    @property
    def direction(self) -> str:
        if self.reader_offset_s > self.writer_offset_s:
            return "reader ahead"
        if self.writer_offset_s > self.reader_offset_s:
            return "writer ahead"
        return "in sync"

    @property
    def hidden_per_1k(self) -> float:
        return 1000 * self.hidden / self.checked if self.checked else 0.0

    @property
    def stale_per_1k(self) -> float:
        return 1000 * self.stale / self.reads if self.reads else 0.0

    @property
    def hit_ratio(self) -> float:
        return self.hits / self.reads if self.reads else 0.0


def _await_ready(slots: memoryview, workers: list[Worker]) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_S
    while not all(slots[READY + index] for index in range(len(workers))):
        for worker in workers:
            worker.check_alive()
        if time.monotonic() > deadline:
            msg = f"ORM benchmark workers not ready within {READY_TIMEOUT_S} s"
            raise RuntimeError(msg)
        time.sleep(0.01)


def run_race(
    env: OrmEnv,
    contender: str,
    *,
    mode: str,
    readers: int,
    writes: int,
    gap_s: float,
    pace_s: float = 0.0,
    commit_s: float = 0.0,
    hook_s: float = 0.0,
    reader_offset_s: float = 0.0,
    writer_offset_s: float = 0.0,
    seed: int = 1,
) -> RaceResult:
    """Run ``readers`` processes reading a counter while another writes it ``writes`` times, ``gap_s`` apart on average.

    Readers read every ``pace_s`` (back to back at 0), commits take ``commit_s`` longer, the on_commit() hooks after
    the publishing one take ``hook_s``, and the offsets shift the wall clocks of the readers and of the writer.
    """
    reset(env)
    shm = SharedMemory(create=True, size=8 * (READY + readers + 1))
    slots = shm.buf.cast("q")
    workers: list[Worker] = []
    try:
        workers.extend(
            Worker(env, contender, "race_reader", {"shm": shm.name, "index": index, "pace": pace_s}, reader_offset_s)
            for index in range(readers)
        )
        writer_args = {
            "shm": shm.name,
            "index": readers,
            "mode": mode,
            "writes": writes,
            "gap": gap_s,
            "commit": commit_s,
            "hook": hook_s,
            "seed": seed,
        }
        writer = Worker(env, contender, "race_writer", writer_args, writer_offset_s)
        workers.append(writer)
        _await_ready(slots, workers)
        slots[PHASE] = RUN
        written = writer.result()
        slots[PHASE] = STOP
        read = [worker.result() for worker in workers[:-1]]
    finally:
        slots[PHASE] = STOP
        for worker in workers:
            worker.close()
        slots.release()
        shm.close()
        shm.unlink()
    return RaceResult(
        contender=contender,
        mode=mode,
        readers=readers,
        writes=writes,
        gap_s=gap_s,
        checked=written["checked"],
        hidden=written["hidden"],
        reads=sum(r["reads"] for r in read),
        stale=sum(r["stale"] for r in read),
        hits=sum(r["hits"] for r in read),
        seconds=written["seconds"],
        pace_s=pace_s,
        commit_s=commit_s,
        hook_s=hook_s,
        reader_offset_s=reader_offset_s,
        writer_offset_s=writer_offset_s,
    )


@dataclass
class LatencyResult:
    contender: str
    phases: dict[str, dict[str, float]] = field(default_factory=dict)


def run_latency(env: OrmEnv, contender: str, *, ops: int, warmup: int) -> LatencyResult:
    reset(env)
    return LatencyResult(contender, run_role(env, contender, "latency", {"ops": ops, "warmup": warmup}))


def run_sizes(env: OrmEnv, contender: str, *, sizes: list[tuple[int, int]], warmup: int) -> LatencyResult:
    """Time hits on queries returning each number of rows, ``sizes`` holding (rows, ops) pairs."""
    reset(env)
    return LatencyResult(contender, run_role(env, contender, "sizes", {"sizes": sizes, "warmup": warmup}))


@dataclass
class MixedResult:
    contender: str
    write_share: float
    ops_per_s: float
    hit_ratio: float


def run_mixed(
    env: OrmEnv,
    contender: str,
    *,
    write_share: float,
    keys: int,
    rows: int,
    ops: int,
    seed: int = 1,
) -> MixedResult:
    reset(env)
    args = {"write_share": write_share, "keys": keys, "rows": rows, "ops": ops, "seed": seed}
    result = run_role(env, contender, "mixed", args)
    return MixedResult(contender, write_share, result["ops_per_s"], result["hit_ratio"])


@dataclass
class MigrateResult:
    contender: str
    queries: int
    hits: int


def run_migrate_noop(env: OrmEnv, contender: str, *, queries: int) -> MigrateResult:
    reset(env)
    result = run_role(env, contender, "migrate_noop", {"queries": queries})
    return MigrateResult(contender, result["queries"], result["hits"])


def format_scorecard(results: Iterable[ScorecardResult]) -> str:
    results = list(results)
    headers = ["Case", *(r.contender for r in results)]
    rows = []
    for index, case in enumerate(results[0].cases):
        cells = []
        for result in results:
            outcome = result.cases[index]
            if outcome["correct"] is None:
                cells.append(f"error: {outcome['error']}")
            else:
                cells.append("correct" if outcome["correct"] else outcome["failure"])
        rows.append([case["case"], *cells])
    rows.append(["Correct", *(f"{sum(bool(c['correct']) for c in r.cases)} of {len(r.cases)}" for r in results)])
    return _render_table(headers, rows)


def _race_label(result: RaceResult) -> str:
    details = [result.mode]
    if result.pace_s:
        details.append(f"a read every {1000 * result.pace_s:g} ms")
    if result.commit_s:
        details.append(f"{1000 * result.commit_s:g} ms commits")
    if result.hook_s:
        details.append(f"{1000 * result.hook_s:g} ms of hooks")
    return f"{result.contender} ({', '.join(details)})"


def format_races(results: Iterable[RaceResult]) -> str:
    headers = ["Contender", "Writes", "Checked", "Hidden writes / 1k", "Reads", "Stale reads / 1k", "Hit ratio"]
    rows = [
        [
            _race_label(r),
            f"{r.writes:,}",
            f"{r.checked:,}",
            f"{r.hidden_per_1k:.1f}",
            f"{r.reads:,}",
            f"{r.stale_per_1k:.2f}",
            f"{r.hit_ratio:.1%}",
        ]
        for r in results
    ]
    return _render_table(headers, rows)


def format_skew(results: Iterable[RaceResult]) -> str:
    """Measured results next to the model's, which only covers cachalot: cachex never reads a client's clock.

    With ``lam`` the skew over the mean gap between writes, a reader ahead hides ``lam / (1 + lam)`` of the writes,
    and a writer ahead keeps ``exp(-lam)`` of the hits it had in sync.
    """
    results = list(results)
    no_skew_hits = {(r.contender, r.gap_s): r.hit_ratio for r in results if r.skew_s == 0}
    headers = [
        "Contender",
        "Clock",
        "Skew ms",
        "Gap ms",
        "Skew / gap",
        "Checked",
        "Hidden / 1k",
        "Model",
        "Hit ratio",
        "Model",
    ]
    rows = []
    for r in results:
        lam = r.skew_s / r.gap_s
        hidden_model = hit_model = ""
        if r.contender == "cachalot":
            hidden_model = f"{1000 * lam / (1 + lam) if r.direction == 'reader ahead' else 0:.0f}"
            base = no_skew_hits.get((r.contender, r.gap_s))
            if r.direction != "reader ahead" and base is not None:
                hit_model = f"{base * math.exp(-lam):.1%}"
        rows.append(
            [
                r.contender,
                r.direction,
                f"{1000 * r.skew_s:g}",
                f"{1000 * r.gap_s:g}",
                f"{lam:.2f}",
                f"{r.checked:,}",
                f"{r.hidden_per_1k:.0f}",
                hidden_model,
                f"{r.hit_ratio:.1%}",
                hit_model,
            ],
        )
    return _render_table(headers, rows)


def format_latency(results: Iterable[LatencyResult]) -> str:
    results = list(results)
    phases = list(results[0].phases)
    headers = ["Contender", *(f"{phase} {stat}" for phase in phases for stat in ("µs", "p95"))]
    rows = [
        [
            r.contender,
            *(f"{r.phases[phase][key]:,.0f}" for phase in phases for key in ("median_us", "p95_us")),
        ]
        for r in results
    ]
    return _render_table(headers, rows)


def format_sizes(results: Iterable[LatencyResult]) -> str:
    results = list(results)
    sizes = list(results[0].phases)
    headers = ["Contender", *(f"{rows} rows µs" for rows in sizes)]
    rows = [[r.contender, *(f"{r.phases[size]['median_us']:,.0f}" for size in sizes)] for r in results]
    return _render_table(headers, rows)


def format_mixed(results: Iterable[MixedResult]) -> str:
    headers = ["Contender", "Writes", "ops/s", "Hit ratio"]
    rows = [[r.contender, f"{r.write_share:.0%}", f"{r.ops_per_s:,.0f}", f"{r.hit_ratio:.1%}"] for r in results]
    return _render_table(headers, rows)


def format_migrate(results: Iterable[MigrateResult]) -> str:
    headers = ["Contender", "Cached queries", "Still cached after migrate"]
    rows = [[r.contender, f"{r.queries}", f"{r.hits} ({r.hits / r.queries:.0%})"] for r in results]
    return _render_table(headers, rows)
