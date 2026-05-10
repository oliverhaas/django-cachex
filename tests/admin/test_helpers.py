"""Tests for admin helper functions."""

import pytest
from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache

from django_cachex.admin.helpers import PAGE_SIZE, _paginate, wrap_for_admin
from django_cachex.cache import LocMemCache
from django_cachex.cache.compat import CachexCompat


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


class TestWrapForAdmin:
    """``wrap_for_admin`` makes non-cachex backends look like cachex backends
    for the duration of an admin request. Native cachex backends pass through
    unchanged."""

    def test_native_cachex_backend_returned_unchanged(self):
        cache = LocMemCache("native", {})
        assert wrap_for_admin(cache) is cache

    def test_stock_django_locmem_gets_wrapped(self):
        cache = DjangoLocMemCache("stock", {})
        wrapped = wrap_for_admin(cache)

        assert wrapped is not cache
        assert isinstance(wrapped, DjangoLocMemCache)
        assert isinstance(wrapped, CachexCompat)
        assert wrapped._cachex_support == "wrapped"

    def test_wrapped_shares_state_with_underlying(self):
        cache = DjangoLocMemCache("share", {})
        wrapped = wrap_for_admin(cache)

        wrapped.set("k", "v")
        assert cache.get("k") == "v"
        cache.set("k2", "v2")
        assert wrapped.get("k2") == "v2"

    def test_wrapped_supports_cachex_ext_ops(self):
        cache = DjangoLocMemCache("ext", {})
        wrapped = wrap_for_admin(cache)

        wrapped.lpush("mylist", 1, 2, 3)
        assert wrapped.lrange("mylist", 0, -1) == [3, 2, 1]
        assert wrapped.llen("mylist") == 3

    def test_wrap_class_memoized_per_underlying_class(self):
        a = DjangoLocMemCache("a", {})
        b = DjangoLocMemCache("b", {})
        wrapped_a = wrap_for_admin(a)
        wrapped_b = wrap_for_admin(b)
        assert type(wrapped_a) is type(wrapped_b)
