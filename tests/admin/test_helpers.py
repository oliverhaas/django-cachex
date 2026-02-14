"""Tests for admin helper functions."""

from __future__ import annotations

import pytest

from django_cachex.admin.helpers import PAGE_SIZE, _paginate


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
