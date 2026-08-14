"""Tests for admin helper functions."""

from typing import Any

import pytest

from django_cachex.admin.helpers import PAGE_SIZE, _paginate
from django_cachex.admin.views.key_detail import _set_preserving_ttl
from django_cachex.exceptions import NotSupportedError

_DEFAULT = object()


class _FakeCache:
    """Records the timeout an admin edit would write.

    Subclasses model the TTL surface of one real backend family; the three
    disagree on how no-expiry is reported and on whether ``pexpire`` exists.
    """

    def __init__(self, remaining: Any):
        self._remaining = remaining
        self.set_timeout: Any = _DEFAULT
        self.pexpire_ms: int | None = None

    def set(self, key: str, value: Any, timeout: Any = _DEFAULT) -> None:
        del key, value
        self.set_timeout = timeout


class _RespCache(_FakeCache):
    """RESP adapters normalize no-expiry to ``None`` and offer ``pexpire``."""

    def pttl(self, key: str) -> int | None:
        del key
        return self._remaining

    def pexpire(self, key: str, timeout: int) -> None:
        del key
        self.pexpire_ms = timeout


class _StreamCache(_FakeCache):
    """StreamCache reports no-expiry as ``-1`` and has no ``pexpire``."""

    def pttl(self, key: str) -> int:
        del key
        return self._remaining


class _LocMemCache(_FakeCache):
    """LocMem/Database have no ``pttl``, and their ``ttl`` is whole seconds."""

    def pttl(self, key: str) -> int:
        raise NotSupportedError("pttl", type(self).__name__)

    def ttl(self, key: str) -> int:
        del key
        return self._remaining


class TestSetPreservingTtl:
    """Editing a value in the admin must not change when the key expires."""

    def test_resp_persistent_key_stays_persistent(self):
        cache = _RespCache(None)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout is None

    def test_stream_persistent_key_stays_persistent(self):
        cache = _StreamCache(-1)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout is None

    def test_locmem_persistent_key_stays_persistent(self):
        cache = _LocMemCache(-1)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout is None

    def test_missing_key_gets_the_default_timeout(self):
        cache = _RespCache(-2)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout is _DEFAULT

    def test_sub_second_precision_is_restored_via_pexpire(self):
        cache = _RespCache(1500)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout == 2
        assert cache.pexpire_ms == 1500

    def test_whole_second_ttl_needs_no_pexpire(self):
        cache = _RespCache(3600_000)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout == 3600
        assert cache.pexpire_ms is None

    def test_ttl_survives_a_backend_without_pexpire(self):
        cache = _StreamCache(3600_500)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout == 3601

    def test_locmem_seconds_ttl_is_carried_over(self):
        cache = _LocMemCache(3600)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout == 3600

    def test_expiring_key_is_not_promoted_to_the_default_timeout(self):
        # LocMem floors ttl(), so a key with under a second left reads as 0.
        cache = _LocMemCache(0)
        _set_preserving_ttl(cache, "k", "v")
        assert cache.set_timeout == 1


class TestPaginate:
    """Test _paginate helper."""

    def test_empty_collection(self):
        p = _paginate(0, 1)
        assert p["page"] == 1
        assert p["total_pages"] == 1
        assert p["total"] == 0
        assert p["has_previous"] is False
        assert p["has_next"] is False
        assert p["start_index"] == 0
        assert p["end_index"] == 0

    def test_exactly_one_page(self):
        p = _paginate(PAGE_SIZE, 1)
        assert p["page"] == 1
        assert p["total_pages"] == 1
        assert p["has_previous"] is False
        assert p["has_next"] is False
        assert p["start_index"] == 0
        assert p["end_index"] == PAGE_SIZE

    def test_just_over_one_page(self):
        p = _paginate(PAGE_SIZE + 1, 1)
        assert p["total_pages"] == 2
        assert p["has_next"] is True
        assert p["next_page"] == 2
        assert p["start_index"] == 0
        assert p["end_index"] == PAGE_SIZE

    def test_second_page(self):
        p = _paginate(PAGE_SIZE + 1, 2)
        assert p["page"] == 2
        assert p["has_previous"] is True
        assert p["has_next"] is False
        assert p["previous_page"] == 1
        assert p["start_index"] == PAGE_SIZE
        assert p["end_index"] == PAGE_SIZE + 1

    def test_middle_page(self):
        total = PAGE_SIZE * 5 + 50
        p = _paginate(total, 3)
        assert p["page"] == 3
        assert p["total_pages"] == 6
        assert p["has_previous"] is True
        assert p["has_next"] is True
        assert p["previous_page"] == 2
        assert p["next_page"] == 4
        assert p["start_index"] == PAGE_SIZE * 2
        assert p["end_index"] == PAGE_SIZE * 3

    def test_page_clamped_to_max(self):
        p = _paginate(50, 999)
        assert p["page"] == 1
        assert p["total_pages"] == 1

    def test_page_clamped_to_min(self):
        p = _paginate(50, 0)
        assert p["page"] == 1

    def test_negative_page(self):
        p = _paginate(200, -5)
        assert p["page"] == 1

    @pytest.mark.parametrize("total", [1, 50, 99, 100])
    def test_single_page_sizes(self, total: int):
        p = _paginate(total, 1)
        assert p["total_pages"] == 1
        assert p["has_next"] is False
        assert p["end_index"] == total

    def test_page_size_constant(self):
        assert PAGE_SIZE == 100
