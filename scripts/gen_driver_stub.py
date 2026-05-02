"""Generate django_cachex/_driver.pyi from src/client.rs and src/async_bridge.rs.

Run from the repo root:
    python3 scripts/gen_driver_stub.py > django_cachex/_driver.pyi
    uv run ruff format django_cachex/_driver.pyi

We parse the #[pymethods] impl RedisRsDriver block — for each fn, extract
the pyo3-exposed name (honoring `#[pyo3(name = ...)]` overrides), the rust
args (skipping self/py), and the return type, then map to Python equivalents.
Sync methods get concrete types; async methods are typed as Awaitable[T] (the
driver returns RedisRsAwaitable, which is awaitable).
"""

import re
import sys
from pathlib import Path

CLIENT = Path("src/client.rs").read_text()

# Find the RedisRsDriver pymethods block
m = re.search(r"#\[pymethods\]\s*impl\s+RedisRsDriver\s*\{(.*?)^\}", CLIENT, re.DOTALL | re.MULTILINE)
if not m:
    sys.exit("RedisRsDriver pymethods block not found")
block = m.group(1)

# Match each fn (with optional preceding pyo3 sig + allow attrs)
FN_RE = re.compile(
    r"(?:#\[pyo3\(signature\s*=\s*\(([^)]*)\)\)\]\s*)?"
    r"(?:#\[pyo3\(name\s*=\s*\"(\w+)\"\)\]\s*)?"
    r"(?:#\[allow[^\]]*\]\s*)*"
    r"fn\s+(\w+)\s*\(([^{]*?)\)\s*->\s*([^{]+?)\s*\{",
    re.DOTALL,
)


def parse_pyo3_sig(s):
    """`key, value, ttl=None` -> {name: default_str_or_None}."""
    if not s:
        return {}
    out = {}
    depth = 0
    cur = ""
    parts = []
    for ch in s:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    for p in parts:
        if "=" in p:
            n, d = p.split("=", 1)
            out[n.strip()] = d.strip()
    return out


# Rust → Python type mapping (applied longest-match-first)
TYPE_MAP = [
    (r"Vec<\(String,\s*Vec<Vec<u8>>\)>", "list[tuple[str, list[bytes]]]"),
    (r"Vec<\(String,\s*Vec<u8>\)>", "list[tuple[str, bytes]]"),
    (r"Vec<\(Vec<u8>,\s*f64\)>", "list[tuple[bytes, float]]"),
    (r"Vec<Option<Vec<u8>>>", "list[bytes | None]"),
    (r"Vec<Vec<u8>>", "list[bytes]"),
    (r"Vec<String>", "list[str]"),
    (r"Vec<u8>", "bytes"),
    (r"Option<Vec<u8>>", "bytes | None"),
    (r"Option<i64>", "int | None"),
    (r"Option<u64>", "int | None"),
    (r"Option<usize>", "int | None"),
    (r"Option<f64>", "float | None"),
    (r"Option<bool>", "bool | None"),
    (r"Option<String>", "str | None"),
    (r"Option<&str>", "str | None"),
    (r"Option<\(usize,\s*usize,\s*usize\)>", "tuple[int, int, int] | None"),
    (r"&\[u8\]", "bytes"),
    (r"&str", "str"),
    (r"\bbool\b", "bool"),
    (r"\bi64\b", "int"),
    (r"\bu64\b", "int"),
    (r"\busize\b", "int"),
    (r"\bf64\b", "float"),
    (r"\bString\b", "str"),
    (r"\(\)", "None"),
]


def to_py_type(rty):
    s = rty.strip()
    for pat, repl in TYPE_MAP:
        s = re.sub(pat, repl, s)
    return s


def parse_rust_args(args_str):
    """Return [(name, py_type)] skipping self / py / Python<...>."""
    args = []
    depth = 0
    cur = ""
    segs = []
    for ch in args_str:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            segs.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        segs.append(cur)
    for raw_seg in segs:
        seg = raw_seg.strip().rstrip(",")
        if not seg or "self" in seg.split(":")[0] or seg.startswith("&self"):
            continue
        if ":" not in seg:
            continue
        name, rty = seg.split(":", 1)
        name = name.strip()
        rty = rty.strip()
        if name == "py" or "Python<" in rty:
            continue
        args.append((name, to_py_type(rty)))
    return args


def parse_return(rty, fn_name):
    rty = rty.strip()
    if rty.startswith("PyResult<"):
        inner = rty[len("PyResult<") :].strip()
        if inner.endswith(">"):
            inner = inner[:-1].strip()
    else:
        inner = rty
    is_sync = fn_name.endswith("_sync") or fn_name in {
        "connect_standard",
        "connect_cluster",
        "connect_sentinel",
        "cache_statistics",
    }
    if inner == "Py<PyAny>":
        return "Any" if is_sync else "Awaitable[Any]"
    if inner == "Self":
        return "RedisRsDriver"
    py = to_py_type(inner)
    if not is_sync:
        # Async return: wrap concrete type in Awaitable
        return f"Awaitable[{py}]"
    return py


def py_default(d):
    return {"None": "None", "true": "True", "false": "False"}.get(d, d)


methods = []
for match in FN_RE.finditer(block):
    sig_str, py_name_override, fn_name, args_str, ret_str = match.groups()
    defaults = parse_pyo3_sig(sig_str)
    args = parse_rust_args(args_str)
    ret = parse_return(ret_str, fn_name)
    exposed_name = py_name_override or fn_name
    methods.append((exposed_name, args, defaults, ret))


# Render
out = [
    "# Generated stub for the maturin-built _driver Rust extension.",
    "# Source: src/client.rs and src/async_bridge.rs. Regenerate via",
    "# scripts/gen_driver_stub.py if you modify the pyo3 surface.",
    "",
    "from collections.abc import Awaitable",
    "from typing import Any",
    "",
    "class RedisRsAwaitable:",
    "    def __await__(self) -> Any: ...",
    "    def cancel(self) -> bool: ...",
    "",
    "class RedisRsDriver:",
]

CLASSMETHODS = {"connect_standard", "connect_cluster", "connect_sentinel"}

for fn, args, defaults, ret in methods:
    arg_strs = []
    for n, t in args:
        if n in defaults:
            arg_strs.append(f"{n}: {t} = {py_default(defaults[n])}")
        else:
            arg_strs.append(f"{n}: {t}")
    if fn in CLASSMETHODS:
        out.append("    @classmethod")
        sig = "    def " + fn + "(cls" + (", " + ", ".join(arg_strs) if arg_strs else "") + ") -> " + ret + ": ..."
    else:
        sig = "    def " + fn + "(self" + (", " + ", ".join(arg_strs) if arg_strs else "") + ") -> " + ret + ": ..."
    out.append(sig)

# Module-level test helper functions (from src/test_helpers.rs)
out += [
    "",
    "def _test_resolved_bytes(value: bytes) -> RedisRsAwaitable: ...",
    "def _test_resolved_none() -> RedisRsAwaitable: ...",
    "def _test_resolved_int(value: int) -> RedisRsAwaitable: ...",
    "def _test_delayed_bytes(value: bytes, delay_ms: int) -> RedisRsAwaitable: ...",
    "def _test_pending() -> RedisRsAwaitable: ...",
    "def _test_dropped() -> RedisRsAwaitable: ...",
    "def _test_error(message: str) -> RedisRsAwaitable: ...",
    "def _test_server_error(message: str) -> RedisRsAwaitable: ...",
]

print("\n".join(out))
