"""Guard the _driver.pyi stub against drift from the actual pyo3 surface.

If a method is added or renamed in src/client.rs but the stub isn't
regenerated (`scripts/gen_driver_stub.py`), this test catches the gap.
"""

import re
from pathlib import Path

from django_cachex._driver import RedisRsDriver

STUB_PATH = Path(__file__).resolve().parents[2] / "django_cachex" / "_driver.pyi"


def _stub_methods() -> set[str]:
    text = STUB_PATH.read_text()
    # Pull `def name(` from inside the RedisRsDriver class block. The block
    # starts at `class RedisRsDriver:` and ends at the next top-level def.
    start = text.index("class RedisRsDriver:")
    end_match = re.search(r"^def ", text[start:], flags=re.MULTILINE)
    block = text[start : start + end_match.start()] if end_match else text[start:]
    return set(re.findall(r"^    def (\w+)\(", block, flags=re.MULTILINE))


def test_stub_covers_runtime_surface():
    runtime = {m for m in dir(RedisRsDriver) if not m.startswith("_")}
    missing = runtime - _stub_methods()
    assert not missing, (
        f"_driver.pyi is out of date — these runtime methods are not in the stub: "
        f"{sorted(missing)}. Run `python3 scripts/gen_driver_stub.py > "
        f"django_cachex/_driver.pyi` and re-run `ruff format` on it."
    )
