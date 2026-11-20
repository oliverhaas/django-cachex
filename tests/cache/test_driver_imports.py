"""Import-time driver loading: importing the package pulls in no driver; resolving a backend pulls in its own."""

import subprocess
import sys

import pytest


def _drivers_loaded_by(code: str) -> str:
    probe = f"{code}; import sys; print([m for m in ('redis', 'valkey', 'glide', 'obstore') if m in sys.modules])"
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


@pytest.mark.parametrize(
    ("backend", "driver"),
    [("RedisCache", "redis"), ("ValkeyCache", "valkey"), ("S3Cache", "obstore")],
)
def test_resolving_a_backend_imports_its_driver(backend: str, driver: str):
    assert f"'{driver}'" in _drivers_loaded_by(f"from django_cachex.cache import {backend}")


def test_s3_cache_without_obstore_names_the_extra_to_install():
    code = "import sys; sys.modules['obstore'] = None; from django_cachex.cache import S3Cache; S3Cache('s3://b/p', {})"
    proc = subprocess.run(  # noqa: S603 (args are sys.executable + a constant)
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert "ImportError" in proc.stderr
    assert "django-cachex[s3]" in proc.stderr
