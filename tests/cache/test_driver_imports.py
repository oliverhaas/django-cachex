"""Import-time driver isolation: a backend must only pull in the driver it needs."""

import subprocess
import sys


def _drivers_loaded_by(code: str) -> str:
    probe = f"{code}; import sys; print([m for m in ('redis', 'valkey', 'glide') if m in sys.modules])"
    proc = subprocess.run(  # noqa: S603 (args are sys.executable + a constant)
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def test_importing_django_cachex_does_not_import_any_driver():
    assert _drivers_loaded_by("import django_cachex") == "[]"


def test_importing_django_cachex_cache_does_not_import_any_driver():
    """Naming a LocMem or Database BACKEND must not drag in a driver."""
    assert _drivers_loaded_by("import django_cachex.cache") == "[]"
    assert _drivers_loaded_by("from django_cachex.cache import LocMemCache") == "[]"


def test_resolving_a_resp_backend_imports_its_driver():
    assert _drivers_loaded_by("from django_cachex.cache import ValkeyCache") != "[]"
