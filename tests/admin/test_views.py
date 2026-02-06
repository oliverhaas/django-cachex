"""Tests for django_cachex.admin views.

These tests use simple fixtures from the local conftest that don't
have the parametrization of the main test suite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.test import Client
from django.urls import reverse

from django_cachex.admin.models import Key

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache


def _cache_list_url() -> str:
    """Get URL for cache list (admin changelist)."""
    return reverse("admin:django_cachex_cache_changelist")


def _cache_detail_url(cache_name: str) -> str:
    """Get URL for cache detail (admin change view)."""
    return reverse("admin:django_cachex_cache_change", args=[cache_name])


def _key_list_url(cache_name: str) -> str:
    """Get URL for key list (admin changelist with cache parameter)."""
    return reverse("admin:django_cachex_key_changelist") + f"?cache={cache_name}"


def _key_detail_url(cache_name: str, key_name: str) -> str:
    """Get URL for key detail (admin change view with composite pk)."""
    pk = Key.make_pk(cache_name, key_name)
    return reverse("admin:django_cachex_key_change", args=[pk])


def _key_add_url(cache_name: str) -> str:
    """Get URL for key add (admin add view with cache parameter)."""
    return reverse("admin:django_cachex_key_add") + f"?cache={cache_name}"


def _key_detail_create_url(cache_name: str, key_name: str, key_type: str = "string") -> str:
    """Get URL for key detail in create mode (key doesn't exist yet)."""
    from urllib.parse import urlencode

    pk = Key.make_pk(cache_name, key_name)
    base = reverse("admin:django_cachex_key_change", args=[pk])
    params = urlencode({"type": key_type})
    return f"{base}?{params}"


class TestIndexView:
    """Tests for the cache instances index view (CacheAdmin changelist)."""

    def test_index_returns_200(self, admin_client: Client, test_cache):
        """Index view should return 200 for authenticated staff."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_index_shows_configured_caches(self, admin_client: Client, test_cache):
        """Index view should list all configured caches."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        # Check that 'default' cache is shown
        assert b"default" in response.content

    def test_index_requires_staff(self, db, test_cache):
        """Index view should redirect anonymous users."""
        client = Client()
        url = _cache_list_url()
        response = client.get(url)
        # Should redirect to login
        assert response.status_code == 302

    def test_index_help_button(self, admin_client: Client, test_cache):
        """Help button should show help message."""
        url = _cache_list_url()
        response = admin_client.get(url + "?help=1")
        assert response.status_code == 200

    def test_index_page_title(self, admin_client: Client, test_cache):
        """Index view should have correct page title."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        # Title should contain "Cache" and "Instances"
        content = response.content.decode()
        assert "Cache" in content

    def test_index_shows_support_badge(self, admin_client: Client, test_cache):
        """Index view should show support level badges."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show cachex badge for django-cachex backends
        assert "cachex" in content.lower()

    def test_index_shows_backend_column(self, admin_client: Client, test_cache):
        """Index view should show backend class path."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show the full backend class path
        assert "ValkeyCache" in content or "RedisCache" in content or "django_cachex" in content

    def test_index_shows_location_column(self, admin_client: Client, test_cache):
        """Index view should show cache location."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show the location (redis:// URL or path)
        assert "redis://" in content or "localhost" in content or "127.0.0.1" in content

    def test_index_cache_links_to_keys(self, admin_client: Client, test_cache):
        """Cache list should have links to key browser."""
        url = _cache_list_url()
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should have link to key browser (List Keys link)
        assert "List Keys" in content or "key" in content.lower()

    def test_index_search_filters_caches(self, admin_client: Client, test_cache):
        """Search should filter cache list by name."""
        url = _cache_list_url()
        # Search for "default" - should show default, hide others
        response = admin_client.get(url + "?q=default")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show matching cache
        assert "default" in content
        # Should not show non-matching caches (with_prefix has different name)
        assert "with_prefix" not in content


class TestKeyListView:
    """Tests for the key search/browser view (KeyAdmin changelist)."""

    def test_key_list_returns_200(self, admin_client: Client, test_cache):
        """Key search view should return 200."""
        url = _key_list_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_key_list_requires_staff(self, db, test_cache):
        """Key search view should redirect anonymous users."""
        client = Client()
        url = _key_list_url("default")
        response = client.get(url)
        assert response.status_code == 302

    def test_key_list_page_title(self, admin_client: Client, test_cache):
        """Key search view should have correct page title."""
        url = _key_list_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        # Title should be "Keys in '<cache_name>'"
        assert b"Keys in" in response.content
        assert b"default" in response.content

    def test_key_list_has_help_and_add_links(self, admin_client: Client, test_cache):
        """Key search view should have help and add key links."""
        url = _key_list_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should have help link
        assert "help=1" in content or "Help" in content
        # Should have add key link
        assert "Add" in content

    def test_key_list_bulk_delete(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Bulk delete action should delete selected keys."""
        test_cache.set("bulk:delete:1", "value1")
        test_cache.set("bulk:delete:2", "value2")
        test_cache.set("bulk:keep", "value3")

        url = _key_list_url("default")
        response = admin_client.post(
            url,
            {
                "action": "delete_selected",
                "_selected_action": ["bulk:delete:1", "bulk:delete:2"],
            },
        )
        # Should redirect on success
        assert response.status_code == 302

        # Verify only selected keys were deleted
        assert test_cache.get("bulk:delete:1") is None
        assert test_cache.get("bulk:delete:2") is None
        assert test_cache.get("bulk:keep") == "value3"

    def test_key_list_shows_string_keys(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should show string keys."""
        test_cache.set("test:key1", "value1")
        test_cache.set("test:key2", "value2")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=test:*")
        assert response.status_code == 200
        assert b"test:key1" in response.content
        assert b"test:key2" in response.content

    def test_key_list_empty_pattern(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search with empty pattern should list all keys."""
        test_cache.set("mykey", "myvalue")

        url = _key_list_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_key_list_shows_type_column(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should show type column for keys."""
        test_cache.set("type:string:test", "value")
        test_cache.rpush("type:list:test", "item1")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=type:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show type badges
        assert "string" in content.lower() or "list" in content.lower()

    def test_key_list_shows_ttl_column(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should show TTL column."""
        test_cache.set("ttl:column:test", "value", timeout=300)

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=ttl:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show TTL value or "No expiry" - looking for table header
        assert "TTL" in content or "ttl" in content.lower()

    def test_key_list_shows_size_column(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should show size column for keys."""
        test_cache.set("size:string:test", "hello world")
        test_cache.rpush("size:list:test", "a", "b", "c")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=size:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show Size column header
        assert "Size" in content or "size" in content.lower()

    def test_key_list_wildcard_pattern(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should support wildcard patterns."""
        test_cache.set("wild:card:one", "value1")
        test_cache.set("wild:card:two", "value2")
        test_cache.set("other:key", "value3")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=wild:card:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show matching keys
        assert "wild:card:one" in content
        assert "wild:card:two" in content
        # Should not show non-matching key
        assert "other:key" not in content

    def test_key_list_contains_pattern(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search without wildcards should do contains search (Django-style)."""
        test_cache.set("session:123", "value1")
        test_cache.set("user_session", "value2")
        test_cache.set("my_session_data", "value3")
        test_cache.set("unrelated:key", "value4")

        url = _key_list_url("default")
        # Search without wildcards - should find all keys containing "session"
        response = admin_client.get(url + "&q=session")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show all keys containing "session"
        assert "session:123" in content
        assert "user_session" in content
        assert "my_session_data" in content
        # Should not show non-matching key
        assert "unrelated:key" not in content

    def test_key_list_pagination(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should paginate results."""
        # Create enough keys to trigger pagination (default is usually 100)
        for i in range(150):
            test_cache.set(f"paginate:key:{i:03d}", f"value{i}")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=paginate:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show pagination controls
        assert "paginator" in content or "page" in content.lower()

    def test_key_list_results_count(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key search should show results count."""
        test_cache.set("count:key:1", "value1")
        test_cache.set("count:key:2", "value2")
        test_cache.set("count:key:3", "value3")

        url = _key_list_url("default")
        response = admin_client.get(url + "&q=count:*")
        assert response.status_code == 200
        content = response.content.decode()
        # Should show result count somewhere (either "3 results" or "3 keys")
        assert "3" in content


class TestKeyDetailView:
    """Tests for the key detail view."""

    def test_string_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for string keys."""
        test_cache.set("string:test", "hello world")

        url = _key_detail_url("default", "string:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"hello world" in response.content

    def test_dict_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for dict/JSON keys (stored as string)."""
        test_cache.set("dict:test", {"name": "Alice", "age": 30})

        url = _key_detail_url("default", "dict:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"Alice" in response.content

    def test_list_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for Redis list keys."""
        test_cache.lpush("list:test", "item1", "item2", "item3")

        url = _key_detail_url("default", "list:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        # Should show list type
        assert b"list" in response.content.lower()

    def test_set_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for Redis set keys."""
        test_cache.sadd("set:test", "member1", "member2", "member3")

        url = _key_detail_url("default", "set:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"set" in response.content.lower()

    def test_hash_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for Redis hash keys."""
        test_cache.hset("hash:test", "field1", "value1")
        test_cache.hset("hash:test", "field2", "value2")

        url = _key_detail_url("default", "hash:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"hash" in response.content.lower()

    def test_zset_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for Redis sorted set keys."""
        test_cache.zadd("zset:test", {"member1": 1.0, "member2": 2.0})

        url = _key_detail_url("default", "zset:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"zset" in response.content.lower()

    def test_nonexistent_key_detail(self, admin_client: Client, test_cache):
        """Detail view should redirect to key list for non-existent keys."""
        url = _key_detail_url("default", "nonexistent:key")
        response = admin_client.get(url)
        # Should redirect to key list with error message
        assert response.status_code == 302
        assert "cache=default" in response.url

    def test_stream_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Detail view should work for Redis stream keys."""
        # Stream operations go directly to Redis, so we need to use the prefixed key
        raw_cache = test_cache._cache  # type: ignore[attr-defined]
        raw_key = test_cache.make_key("stream:detail:test")
        raw_cache.xadd(raw_key, {"field1": "value1"})
        raw_cache.xadd(raw_key, {"field2": "value2"})

        url = _key_detail_url("default", "stream:detail:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show stream type
        assert "stream" in content.lower()

    def test_key_detail_requires_staff(self, db, test_cache):
        """Key detail view should redirect anonymous users."""
        client = Client()
        url = _key_detail_url("default", "any:key")
        response = client.get(url)
        assert response.status_code == 302

    def test_key_detail_page_title(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail view should have correct page title."""
        test_cache.set("title:test", "value")

        url = _key_detail_url("default", "title:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        # Title should be "Key: {key}"
        assert b"Key:" in response.content
        assert b"title:test" in response.content

    def test_key_detail_help_button(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Help button on key detail should return 200."""
        test_cache.set("help:test", "value")

        url = _key_detail_url("default", "help:test")
        response = admin_client.get(url + "?help=1")
        assert response.status_code == 200

    def test_key_detail_shows_raw_key(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail should show the raw Redis key."""
        test_cache.set("rawkey:test", "value")

        url = _key_detail_url("default", "rawkey:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show "Raw Key" label or the key with prefix
        assert "Raw" in content or "rawkey:test" in content

    def test_key_detail_shows_cache_name(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail should show the cache name."""
        test_cache.set("cache:name:test", "value")

        url = _key_detail_url("default", "cache:name:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show cache name
        assert "default" in content

    def test_key_detail_shows_type_badge(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail should show type badge for the key."""
        test_cache.rpush("type:badge:test", "item1", "item2", "item3")

        url = _key_detail_url("default", "type:badge:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show list type
        assert "list" in content.lower()
        # Should show item count
        assert "3" in content

    def test_key_detail_shows_ttl(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail should show TTL for expiring keys."""
        test_cache.set("ttl:detail:test", "value", timeout=300)

        url = _key_detail_url("default", "ttl:detail:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should show TTL field
        assert "TTL" in content or "ttl" in content.lower()

    def test_key_detail_shows_no_expiry(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key detail should show 'No expiry' for persistent keys."""
        test_cache.set("noexpiry:test", "value", timeout=None)

        url = _key_detail_url("default", "noexpiry:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should indicate no expiry
        assert "No expiry" in content or "expiry" in content.lower() or "-1" in content

    def test_string_value_save_form_structure(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """String key detail view should have properly structured save form."""
        test_cache.set("form:structure:test", "original value")

        url = _key_detail_url("default", "form:structure:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # The form with id="key-form" should exist and have action="update"
        assert 'id="key-form"' in content
        assert 'name="action" value="update"' in content
        # A submit button should be present
        assert 'type="submit"' in content

    def test_string_value_save_button_updates_value(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Save button on string key detail should update the value."""
        test_cache.set("save:button:test", "original value")

        url = _key_detail_url("default", "save:button:test")

        # Simulate what the Save button form submission does
        response = admin_client.post(
            url,
            {"action": "update", "value": "updated value"},
        )
        # Should redirect after successful update
        assert response.status_code == 302

        # Verify value was updated
        assert test_cache.get("save:button:test") == "updated value"

    def test_string_ttl_input_has_its_own_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """TTL input for keys should have its own form with set_ttl action."""
        test_cache.set("ttl:form:test", "value", timeout=300)

        url = _key_detail_url("default", "ttl:form:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # TTL has its own form with set_ttl action
        assert 'name="action" value="set_ttl"' in content
        # Should have the TTL input field with name ttl_value
        assert 'name="ttl_value"' in content

    def test_string_set_ttl_action_sets_ttl(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Setting TTL via set_ttl action should set the expiry."""
        test_cache.set("ttl:save:test", "value", timeout=None)

        url = _key_detail_url("default", "ttl:save:test")

        # Set TTL to 600 seconds
        response = admin_client.post(
            url,
            {"action": "set_ttl", "ttl_value": "600"},
        )
        assert response.status_code == 302

        # Verify TTL was set
        ttl = test_cache.ttl("ttl:save:test")
        assert ttl is not None
        assert 590 <= ttl <= 600  # Allow some margin for test execution time

    def test_string_set_empty_ttl_persists(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Setting empty TTL via set_ttl action should persist (no expiry)."""
        test_cache.set("ttl:persist:test", "original", timeout=300)

        url = _key_detail_url("default", "ttl:persist:test")

        # Set empty TTL (should persist)
        response = admin_client.post(
            url,
            {"action": "set_ttl", "ttl_value": ""},
        )
        assert response.status_code == 302

        # Verify key was persisted (no expiry)
        ttl = test_cache.ttl("ttl:persist:test")
        assert ttl is None or ttl == -1  # -1 or None means no expiry

    def test_list_ttl_update_sets_ttl(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Updating TTL on list key should set the expiry."""
        test_cache.rpush("list:ttl:test", "item1", "item2")

        url = _key_detail_url("default", "list:ttl:test")

        # Update TTL to 600 seconds
        response = admin_client.post(
            url,
            {"action": "set_ttl", "ttl_value": "600"},
        )
        assert response.status_code == 302

        # Verify TTL was set
        ttl = test_cache.ttl("list:ttl:test")
        assert ttl is not None
        assert 590 <= ttl <= 600

    def test_list_empty_ttl_persists(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Submitting empty TTL on list key should persist (no expiry)."""
        test_cache.rpush("list:persist:test", "item1")
        # Set an initial TTL
        test_cache.expire("list:persist:test", 300)

        url = _key_detail_url("default", "list:persist:test")

        # Submit empty TTL (should persist)
        response = admin_client.post(
            url,
            {"action": "set_ttl", "ttl_value": ""},
        )
        assert response.status_code == 302

        # Verify key was persisted (no expiry)
        ttl = test_cache.ttl("list:persist:test")
        assert ttl is None or ttl == -1

    def test_json_serializable_string_is_editable(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """JSON-serializable string should show JSON indicator and be editable."""
        test_cache.set("json:string:test", "hello world")

        url = _key_detail_url("default", "json:string:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # Should show the value as JSON (with quotes) - HTML escaped in textarea
        assert "&quot;hello world&quot;" in content
        # Should have the save/update button visible (edit_supported)
        assert "Update" in content or "Save" in content

    def test_json_serializable_dict_is_editable(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """JSON-serializable dict should show JSON indicator and be editable."""
        test_cache.set("json:dict:test", {"name": "Alice", "age": 30})

        url = _key_detail_url("default", "json:dict:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # Should show formatted JSON - HTML escaped in textarea
        assert "&quot;name&quot;" in content
        assert "&quot;Alice&quot;" in content
        assert "&quot;age&quot;" in content
        # Should have the update button visible
        assert "Update" in content or "Save" in content

    def test_json_serializable_list_value_is_editable(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """JSON-serializable list value should be editable."""
        test_cache.set("json:list:value:test", [1, 2, 3, "four"])

        url = _key_detail_url("default", "json:list:value:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # Should show the list as JSON - HTML escaped in textarea
        assert "[" in content
        assert "1" in content
        assert "&quot;four&quot;" in content

    def test_json_indicator_shown_for_dict(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """JSON indicator should be shown for dict values."""
        test_cache.set("json:indicator:test", {"type": "example"})

        url = _key_detail_url("default", "json:indicator:test")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()

        # Should show JSON indicator somewhere
        assert "JSON" in content


class TestKeyAddView:
    """Tests for the add key view."""

    def test_add_key_get(self, admin_client: Client, test_cache):
        """Add key view GET should return form."""
        url = _key_add_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_add_key_requires_staff(self, db, test_cache):
        """Add key view should redirect anonymous users."""
        client = Client()
        url = _key_add_url("default")
        response = client.get(url)
        assert response.status_code == 302

    def test_add_key_page_title(self, admin_client: Client, test_cache):
        """Add key view should have correct page title."""
        url = _key_add_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        # Title should be "Add key to '<cache_name>'"
        assert b"Add key to" in response.content
        assert b"default" in response.content

    def test_add_key_help_button(self, admin_client: Client, test_cache):
        """Help button on add key should return 200."""
        url = _key_add_url("default")
        response = admin_client.get(url + "&help=1")
        assert response.status_code == 200

    def test_add_key_with_timeout(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key with timeout should create expiring key."""
        # Create string key via key_detail update action
        url = _key_detail_create_url("default", "timeout:test:key", "string")
        response = admin_client.post(
            url,
            {
                "action": "update",
                "value": '"expiring value"',
            },
        )
        assert response.status_code == 302

        # Set TTL separately via set_ttl action (now key exists)
        detail_url = _key_detail_url("default", "timeout:test:key")
        response = admin_client.post(
            detail_url,
            {
                "action": "set_ttl",
                "ttl_value": "300",
            },
        )
        assert response.status_code == 302

        # Verify key was created
        assert test_cache.get("timeout:test:key") == "expiring value"

        # Verify TTL was set (should be close to 300)
        ttl = test_cache.ttl("timeout:test:key")
        assert 290 <= ttl <= 300

    def test_add_key_post_string(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key view should create string keys via update action."""
        # Create string key via key_detail update action
        url = _key_detail_create_url("default", "new:string:key", "string")
        response = admin_client.post(
            url,
            {
                "action": "update",
                "value": '"test value"',
            },
        )
        # Should redirect on success
        assert response.status_code == 302

        # Verify key was created
        assert test_cache.get("new:string:key") == "test value"

    def test_add_key_post_json(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key view should parse JSON values."""
        # Create string key via key_detail update action
        url = _key_detail_create_url("default", "new:json:key", "string")
        response = admin_client.post(
            url,
            {
                "action": "update",
                "value": '{"name": "test", "count": 42}',
            },
        )
        assert response.status_code == 302

        # Verify key was created with parsed JSON
        value = test_cache.get("new:json:key")
        assert value == {"name": "test", "count": 42}

    def test_add_key_save_and_add_another(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Key add form should redirect to key_detail in create mode."""
        # POST to key_add to get redirect to key_detail
        url = _key_add_url("default")
        response = admin_client.post(
            url,
            {
                "key": "addanother:key",
                "type": "string",
            },
        )
        # Should redirect to key_detail in create mode
        assert response.status_code == 302
        # Key name is URL encoded (: -> %3A -> %253A when double encoded)
        assert "addanother" in response.url and "key" in response.url
        assert "type=string" in response.url

    def test_add_key_type_list(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key should create list keys via list_rpush action."""
        # Create list key via key_detail list_rpush action
        url = _key_detail_create_url("default", "new:list:key", "list")

        # Push items one by one (as the UI would do)
        for item in ["item1", "item2", "item3"]:
            response = admin_client.post(
                url,
                {
                    "action": "rpush",
                    "push_value": item,
                },
            )
            assert response.status_code == 302

        # Verify list was created with correct items in order
        items = test_cache.lrange("new:list:key", 0, -1)
        assert items == ["item1", "item2", "item3"]

    def test_add_key_type_set(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key should create set keys via set_sadd action."""
        # Create set key via key_detail set_sadd action
        url = _key_detail_create_url("default", "new:set:key", "set")

        # Add members one by one
        for member in ["member1", "member2", "member3"]:
            response = admin_client.post(
                url,
                {
                    "action": "sadd",
                    "member_value": member,
                },
            )
            assert response.status_code == 302

        # Verify set was created
        members = test_cache.smembers("new:set:key")
        assert members == {"member1", "member2", "member3"}

    def test_add_key_type_hash(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key should create hash keys via hash_hset action."""
        # Create hash key via key_detail hash_hset action
        url = _key_detail_create_url("default", "new:hash:key", "hash")

        # Add fields one by one
        for field, value in [("field1", "value1"), ("field2", "value2")]:
            response = admin_client.post(
                url,
                {
                    "action": "hset",
                    "field_name": field,
                    "field_value": value,
                },
            )
            assert response.status_code == 302

        # Verify hash was created
        fields = test_cache.hgetall("new:hash:key")
        assert fields == {"field1": "value1", "field2": "value2"}

    def test_add_key_type_zset(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Add key should create sorted set keys via zset_zadd action."""
        # Create sorted set key via key_detail zset_zadd action
        url = _key_detail_create_url("default", "new:zset:key", "zset")

        # Add members one by one
        for member, score in [("member1", 1.5), ("member2", 2.5)]:
            response = admin_client.post(
                url,
                {
                    "action": "zadd",
                    "member_value": member,
                    "score_value": str(score),
                },
            )
            assert response.status_code == 302

        # Verify sorted set was created
        members = test_cache.zrange("new:zset:key", 0, -1, withscores=True)
        assert members == [("member1", 1.5), ("member2", 2.5)]


class TestKeyOperations:
    """Tests for key operations (delete, edit, TTL)."""

    def test_delete_key(self, admin_client: Client, test_cache: KeyValueCache):
        """Delete action should remove key."""
        test_cache.set("delete:me", "goodbye")

        url = _key_detail_url("default", "delete:me")
        response = admin_client.post(url, {"action": "delete"})
        assert response.status_code == 302

        # Verify key was deleted
        assert test_cache.get("delete:me") is None

    def test_edit_key_value(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Edit action should update key value."""
        test_cache.set("edit:me", "old value")

        url = _key_detail_url("default", "edit:me")
        response = admin_client.post(
            url,
            {"action": "update", "value": "new value"},
        )
        assert response.status_code == 302

        # Verify value was updated
        assert test_cache.get("edit:me") == "new value"

    def test_list_lrem(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """LREM action should remove item from list."""
        test_cache.rpush("lrem:test", "item1", "item2", "item3", "item2")

        url = _key_detail_url("default", "lrem:test")
        response = admin_client.post(
            url,
            {"action": "lrem", "item_value": "item2"},
        )
        assert response.status_code == 302

        # Verify all occurrences of item2 were removed
        items = test_cache.lrange("lrem:test", 0, -1)
        assert items == ["item1", "item3"]

    def test_list_ltrim(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """LTRIM action should trim list to specified range."""
        test_cache.rpush("ltrim:test", "a", "b", "c", "d", "e")

        url = _key_detail_url("default", "ltrim:test")
        response = admin_client.post(
            url,
            {"action": "ltrim", "trim_start": "1", "trim_stop": "3"},
        )
        assert response.status_code == 302

        # Verify list was trimmed to indices 1-3 (b, c, d)
        items = test_cache.lrange("ltrim:test", 0, -1)
        assert items == ["b", "c", "d"]

    def test_hash_field_inline_edit(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """HSET action should update hash field value inline."""
        test_cache.hset("hash:edit", "name", "old_value")

        url = _key_detail_url("default", "hash:edit")
        response = admin_client.post(
            url,
            {"action": "hset", "field_name": "name", "field_value": "new_value"},
        )
        assert response.status_code == 302

        # Verify field was updated
        assert test_cache.hget("hash:edit", "name") == "new_value"

    def test_hash_hdel(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """HDEL action should delete a field from the hash."""
        test_cache.hset("hash:hdel", "field1", "value1")
        test_cache.hset("hash:hdel", "field2", "value2")
        test_cache.hset("hash:hdel", "field3", "value3")

        url = _key_detail_url("default", "hash:hdel")
        response = admin_client.post(
            url,
            {"action": "hdel", "field": "field2"},
        )
        assert response.status_code == 302

        # Verify field was deleted
        assert test_cache.hget("hash:hdel", "field2") is None
        # Verify other fields still exist
        assert test_cache.hget("hash:hdel", "field1") == "value1"
        assert test_cache.hget("hash:hdel", "field3") == "value3"

    def test_list_lpop(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """LPOP action should pop from left of list."""
        test_cache.rpush("lpop:test", "a", "b", "c")

        url = _key_detail_url("default", "lpop:test")
        response = admin_client.post(url, {"action": "lpop"})
        assert response.status_code == 302

        # Verify first element was popped
        items = test_cache.lrange("lpop:test", 0, -1)
        assert items == ["b", "c"]

    def test_list_lpop_with_count(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """LPOP action with count should pop multiple elements from left."""
        test_cache.rpush("lpop:count:test", "a", "b", "c", "d", "e")

        url = _key_detail_url("default", "lpop:count:test")
        response = admin_client.post(
            url,
            {"action": "lpop", "pop_count": "3"},
        )
        assert response.status_code == 302

        # Verify first 3 elements were popped
        items = test_cache.lrange("lpop:count:test", 0, -1)
        assert items == ["d", "e"]

    def test_list_rpop(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """RPOP action should pop from right of list."""
        test_cache.rpush("rpop:test", "a", "b", "c")

        url = _key_detail_url("default", "rpop:test")
        response = admin_client.post(url, {"action": "rpop"})
        assert response.status_code == 302

        # Verify last element was popped
        items = test_cache.lrange("rpop:test", 0, -1)
        assert items == ["a", "b"]

    def test_list_rpop_with_count(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """RPOP action with count should pop multiple elements from right."""
        test_cache.rpush("rpop:count:test", "a", "b", "c", "d", "e")

        url = _key_detail_url("default", "rpop:count:test")
        response = admin_client.post(
            url,
            {"action": "rpop", "pop_count": "3"},
        )
        assert response.status_code == 302

        # Verify last 3 elements were popped
        items = test_cache.lrange("rpop:count:test", 0, -1)
        assert items == ["a", "b"]

    def test_list_lpush(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """LPUSH action should push to left of list."""
        test_cache.rpush("lpush:test", "b", "c")

        url = _key_detail_url("default", "lpush:test")
        response = admin_client.post(
            url,
            {"action": "lpush", "push_value": "a"},
        )
        assert response.status_code == 302

        # Verify element was pushed to left
        items = test_cache.lrange("lpush:test", 0, -1)
        assert items == ["a", "b", "c"]

    def test_list_rpush(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """RPUSH action should push to right of list."""
        test_cache.rpush("rpush:test", "a", "b")

        url = _key_detail_url("default", "rpush:test")
        response = admin_client.post(
            url,
            {"action": "rpush", "push_value": "c"},
        )
        assert response.status_code == 302

        # Verify element was pushed to right
        items = test_cache.lrange("rpush:test", 0, -1)
        assert items == ["a", "b", "c"]

    def test_set_sadd(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """SADD action should add member to set."""
        test_cache.sadd("sadd:test", "a", "b")

        url = _key_detail_url("default", "sadd:test")
        response = admin_client.post(
            url,
            {"action": "sadd", "member_value": "c"},
        )
        assert response.status_code == 302

        # Verify member was added
        members = test_cache.smembers("sadd:test")
        assert members == {"a", "b", "c"}

    def test_set_srem(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """SREM action should remove member from set."""
        test_cache.sadd("srem:test", "a", "b", "c")

        url = _key_detail_url("default", "srem:test")
        response = admin_client.post(
            url,
            {"action": "srem", "member": "b"},
        )
        assert response.status_code == 302

        # Verify member was removed
        members = test_cache.smembers("srem:test")
        assert members == {"a", "c"}

    def test_set_spop(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """SPOP action should pop random member from set."""
        test_cache.sadd("spop:test", "only_member")

        url = _key_detail_url("default", "spop:test")
        response = admin_client.post(url, {"action": "spop"})
        assert response.status_code == 302

        # Verify member was popped (set should be empty)
        members = test_cache.smembers("spop:test")
        assert members == set()

    def test_set_spop_with_count(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """SPOP action with count should pop multiple random members."""
        test_cache.sadd("spop:count:test", "a", "b", "c", "d", "e")

        url = _key_detail_url("default", "spop:count:test")
        response = admin_client.post(
            url,
            {"action": "spop", "pop_count": "3"},
        )
        assert response.status_code == 302

        # Verify 3 members were popped (2 should remain)
        members = test_cache.smembers("spop:count:test")
        assert len(members) == 2

    def test_zset_zadd(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action should add member with score to sorted set."""
        test_cache.zadd("zadd:test", {"a": 1.0})

        url = _key_detail_url("default", "zadd:test")
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "b", "score_value": "2.5"},
        )
        assert response.status_code == 302

        # Verify member was added with correct score
        members = test_cache.zrange("zadd:test", 0, -1, withscores=True)
        assert members == [("a", 1.0), ("b", 2.5)]

    def test_zset_zadd_nx_flag(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action with NX flag should only add new members, not update existing."""
        test_cache.zadd("zadd:nx:test", {"a": 1.0})

        url = _key_detail_url("default", "zadd:nx:test")
        # Try to update existing member "a" with NX flag - should not update
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "99.0", "zadd_nx": "on"},
        )
        assert response.status_code == 302

        # Verify member "a" still has original score (NX prevented update)
        score = test_cache.zscore("zadd:nx:test", "a")
        assert score == 1.0

        # Try to add new member "b" with NX flag - should succeed
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "b", "score_value": "2.0", "zadd_nx": "on"},
        )
        assert response.status_code == 302

        # Verify member "b" was added
        members = test_cache.zrange("zadd:nx:test", 0, -1, withscores=True)
        assert members == [("a", 1.0), ("b", 2.0)]

    def test_zset_zadd_xx_flag(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action with XX flag should only update existing members."""
        test_cache.zadd("zadd:xx:test", {"a": 1.0})

        url = _key_detail_url("default", "zadd:xx:test")
        # Try to add new member "b" with XX flag - should not add
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "b", "score_value": "2.0", "zadd_xx": "on"},
        )
        assert response.status_code == 302

        # Verify member "b" was NOT added (XX only updates existing)
        members = test_cache.zrange("zadd:xx:test", 0, -1, withscores=True)
        assert members == [("a", 1.0)]

        # Try to update existing member "a" with XX flag - should succeed
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "99.0", "zadd_xx": "on"},
        )
        assert response.status_code == 302

        # Verify member "a" was updated
        score = test_cache.zscore("zadd:xx:test", "a")
        assert score == 99.0

    def test_zset_zadd_gt_flag(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action with GT flag should only update if new score > current."""
        test_cache.zadd("zadd:gt:test", {"a": 10.0})

        url = _key_detail_url("default", "zadd:gt:test")
        # Try to update with lower score - should not update
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "5.0", "zadd_gt": "on"},
        )
        assert response.status_code == 302

        # Verify score unchanged (GT prevented update)
        score = test_cache.zscore("zadd:gt:test", "a")
        assert score == 10.0

        # Try to update with higher score - should succeed
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "20.0", "zadd_gt": "on"},
        )
        assert response.status_code == 302

        # Verify score was updated
        score = test_cache.zscore("zadd:gt:test", "a")
        assert score == 20.0

    def test_zset_zadd_lt_flag(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action with LT flag should only update if new score < current."""
        test_cache.zadd("zadd:lt:test", {"a": 10.0})

        url = _key_detail_url("default", "zadd:lt:test")
        # Try to update with higher score - should not update
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "20.0", "zadd_lt": "on"},
        )
        assert response.status_code == 302

        # Verify score unchanged (LT prevented update)
        score = test_cache.zscore("zadd:lt:test", "a")
        assert score == 10.0

        # Try to update with lower score - should succeed
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "a", "score_value": "5.0", "zadd_lt": "on"},
        )
        assert response.status_code == 302

        # Verify score was updated
        score = test_cache.zscore("zadd:lt:test", "a")
        assert score == 5.0

    def test_zset_zrem(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZREM action should remove member from sorted set."""
        test_cache.zadd("zrem:test", {"a": 1.0, "b": 2.0, "c": 3.0})

        url = _key_detail_url("default", "zrem:test")
        response = admin_client.post(
            url,
            {"action": "zrem", "member": "b"},
        )
        assert response.status_code == 302

        # Verify member was removed
        members = test_cache.zrange("zrem:test", 0, -1, withscores=True)
        assert members == [("a", 1.0), ("c", 3.0)]

    def test_zset_zpopmin(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZPOPMIN action should pop member with lowest score."""
        test_cache.zadd("zpopmin:test", {"a": 1.0, "b": 2.0, "c": 3.0})

        url = _key_detail_url("default", "zpopmin:test")
        response = admin_client.post(url, {"action": "zpopmin"})
        assert response.status_code == 302

        # Verify lowest score member was popped
        members = test_cache.zrange("zpopmin:test", 0, -1, withscores=True)
        assert members == [("b", 2.0), ("c", 3.0)]

    def test_zset_zpopmax(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZPOPMAX action should pop member with highest score."""
        test_cache.zadd("zpopmax:test", {"a": 1.0, "b": 2.0, "c": 3.0})

        url = _key_detail_url("default", "zpopmax:test")
        response = admin_client.post(url, {"action": "zpopmax"})
        assert response.status_code == 302

        # Verify highest score member was popped
        members = test_cache.zrange("zpopmax:test", 0, -1, withscores=True)
        assert members == [("a", 1.0), ("b", 2.0)]

    def test_zset_score_inline_edit(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """ZADD action should update score for existing member."""
        test_cache.zadd("zscore:test", {"a": 1.0, "b": 2.0})

        url = _key_detail_url("default", "zscore:test")
        response = admin_client.post(
            url,
            {"action": "zadd", "member_value": "b", "score_value": "5.5"},
        )
        assert response.status_code == 302

        # Verify score was updated
        score = test_cache.zscore("zscore:test", "b")
        assert score == 5.5

    def test_set_ttl(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Set TTL action should set expiration time."""
        test_cache.set("ttl:test", "value")

        url = _key_detail_url("default", "ttl:test")
        response = admin_client.post(
            url,
            {"action": "set_ttl", "ttl_value": "300"},
        )
        assert response.status_code == 302

        # Verify TTL was set (should be close to 300)
        ttl = test_cache.ttl("ttl:test")
        assert 290 <= ttl <= 300

    def test_persist(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Persist action should remove TTL."""
        test_cache.set("persist:test", "value", timeout=300)

        url = _key_detail_url("default", "persist:test")
        response = admin_client.post(url, {"action": "persist"})
        assert response.status_code == 302

        # Verify TTL was removed (None or -1 means no expiry)
        ttl = test_cache.ttl("persist:test")
        assert ttl is None or ttl == -1

    def test_stream_xadd(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """XADD action should add an entry to the stream."""
        # Stream operations are on the underlying _cache client
        raw_cache = test_cache._cache  # type: ignore[attr-defined]
        # Create a stream with initial entry
        raw_cache.xadd("xadd:test", {"field1": "value1"})

        url = _key_detail_url("default", "xadd:test")
        response = admin_client.post(
            url,
            {"action": "xadd", "field_name": "field2", "field_value": "value2"},
        )
        assert response.status_code == 302

        # Verify a second entry was added
        entries = raw_cache.xrange("xadd:test")
        assert len(entries) == 2
        # Verify the new entry has the expected field
        last_entry = entries[-1]
        assert last_entry[1] == {"field2": "value2"}

    def test_stream_xdel(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """XDEL action should delete an entry from the stream."""
        raw_cache = test_cache._cache  # type: ignore[attr-defined]
        # Create a stream with multiple entries
        entry_id1 = raw_cache.xadd("xdel:test", {"field1": "value1"})
        entry_id2 = raw_cache.xadd("xdel:test", {"field2": "value2"})
        raw_cache.xadd("xdel:test", {"field3": "value3"})

        # Verify 3 entries exist
        assert raw_cache.xlen("xdel:test") == 3

        url = _key_detail_url("default", "xdel:test")
        response = admin_client.post(
            url,
            {"action": "xdel", "entry_id": entry_id2},
        )
        assert response.status_code == 302

        # Verify entry was deleted (2 entries remain)
        assert raw_cache.xlen("xdel:test") == 2

    def test_stream_xtrim(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """XTRIM action should execute successfully.

        Note: Redis's approximate trimming (~MAXLEN) may not immediately trim
        small streams as it optimizes for performance by trimming in batches.
        This test verifies the action executes without error.
        """
        raw_cache = test_cache._cache  # type: ignore[attr-defined]
        # Create a stream with entries
        for i in range(10):
            raw_cache.xadd("xtrim:test", {f"field{i}": f"value{i}"})

        # Verify stream exists
        assert raw_cache.xlen("xtrim:test") == 10

        url = _key_detail_url("default", "xtrim:test")
        response = admin_client.post(
            url,
            {"action": "xtrim", "maxlen": "3"},
            follow=True,
        )
        assert response.status_code == 200

        # Verify the action executed successfully (shows "Trimmed N entries" message)
        messages = [str(m.message) for m in response.context.get("messages", [])]
        assert any("Trimmed" in m for m in messages), f"Expected trim success message, got: {messages}"


class TestFlushCache:
    """Tests for flush cache functionality."""

    def test_flush_cache(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Flush action should clear all keys."""
        test_cache.set("flush:key1", "value1")
        test_cache.set("flush:key2", "value2")

        # Flush is triggered from the index view with flush_selected action
        url = _cache_list_url()
        response = admin_client.post(
            url,
            {"action": "flush_selected", "_selected_action": ["default"]},
        )
        assert response.status_code == 302

        # Verify keys were deleted
        assert test_cache.get("flush:key1") is None
        assert test_cache.get("flush:key2") is None


class TestKeyDetailCreateMode:
    """Tests for key_detail view in create mode (key doesn't exist yet)."""

    def test_create_mode_returns_200(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode should return 200, not redirect to key list."""
        # Request key_detail for non-existing key WITH type param
        url = _key_detail_create_url("default", "newkey:create", "list")
        response = admin_client.get(url)

        # Should return 200, not redirect
        assert response.status_code == 200

    def test_create_mode_shows_message(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode should show informational message about key not existing."""
        url = _key_detail_create_url("default", "newkey:message", "list")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show the create mode message
        assert "does not exist yet" in content

    def test_create_mode_shows_operations(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode should show Operations section for adding data."""
        url = _key_detail_create_url("default", "newkey:ops", "list")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show operations section
        assert "Operations" in content

    def test_create_mode_list_shows_push_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode for list should show push operations."""
        url = _key_detail_create_url("default", "newkey:list:push", "list")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show push operations
        assert "Push" in content or "LPUSH" in content or "RPUSH" in content

    def test_create_mode_set_shows_add_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode for set should show add member operation."""
        url = _key_detail_create_url("default", "newkey:set:add", "set")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show add member operation
        assert "Add" in content or "SADD" in content

    def test_create_mode_hash_shows_set_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode for hash should show set field operation."""
        url = _key_detail_create_url("default", "newkey:hash:set", "hash")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show set field operation
        assert "Set" in content or "HSET" in content or "Add" in content

    def test_create_mode_zset_shows_add_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode for zset should show add member operation."""
        url = _key_detail_create_url("default", "newkey:zset:add", "zset")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show add member operation
        assert "Add" in content or "ZADD" in content

    def test_create_mode_string_shows_value_form(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Create mode for string should show value editor."""
        url = _key_detail_create_url("default", "newkey:string:edit", "string")
        response = admin_client.get(url)

        assert response.status_code == 200
        content = response.content.decode()
        # Should show value textarea
        assert "id_value" in content or "textarea" in content.lower()

    def test_create_mode_operation_creates_key(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Performing operation in create mode should create the key."""
        # Make sure key doesn't exist
        assert test_cache.get("createkey:list") is None

        # Use create mode URL for list type
        url = _key_detail_create_url("default", "createkey:list", "list")

        # POST to push an item - this should create the key
        response = admin_client.post(
            url,
            {"action": "rpush", "push_value": "first_item"},
        )

        # Should redirect (to stay on same page after operation)
        assert response.status_code == 302

        # Verify key was created
        items = test_cache.lrange("createkey:list", 0, -1)
        assert items == ["first_item"]

    def test_create_mode_operation_redirects_to_key_detail(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """After creating key via operation, should redirect to key detail (not key list)."""
        url = _key_detail_create_url("default", "createkey:redirect", "list")

        response = admin_client.post(
            url,
            {"action": "rpush", "push_value": "item"},
        )

        # Should redirect
        assert response.status_code == 302

        # Should redirect to key detail, not key list
        # Key detail URL contains the key pk
        assert "createkey" in response.url
        # Should NOT redirect to key changelist
        assert "changelist" not in response.url

    def test_create_mode_without_type_redirects(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Non-existing key without type param should redirect to key list."""
        # Use the regular key_detail URL (without type param)
        url = _key_detail_url("default", "nonexistent:key:notype")
        response = admin_client.get(url)

        # Should redirect to key list
        assert response.status_code == 302
        assert "cache=default" in response.url


class TestCacheDetailView:
    """Tests for the cache detail view (combined info + slowlog)."""

    def test_cache_detail_returns_200(self, admin_client: Client, test_cache):
        """Cache detail view should return 200 for authenticated staff."""
        url = _cache_detail_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_cache_detail_requires_staff(self, db, test_cache):
        """Cache detail view should redirect anonymous users."""
        client = Client()
        url = _cache_detail_url("default")
        response = client.get(url)
        assert response.status_code == 302

    def test_cache_detail_shows_cache_name(self, admin_client: Client, test_cache):
        """Cache detail view should show cache name."""
        url = _cache_detail_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        assert b"default" in response.content

    def test_cache_detail_displays_configuration(
        self,
        admin_client: Client,
        test_cache: KeyValueCache,
    ):
        """Cache detail view should display cache configuration."""
        url = _cache_detail_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        # Should show the cache name
        assert b"default" in response.content
        # Should show the backend class in configuration
        assert b"django_cachex" in response.content
        # Should show configuration section
        assert b"Configuration" in response.content

    def test_cache_detail_has_keys_link(self, admin_client: Client, test_cache):
        """Cache detail view should have link to keys list."""
        url = _cache_detail_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should have link to keys list
        assert "Keys" in content or "List" in content

    def test_cache_detail_shows_slowlog_section(self, admin_client: Client, test_cache):
        """Cache detail view should show slow log section."""
        url = _cache_detail_url("default")
        response = admin_client.get(url)
        assert response.status_code == 200
        content = response.content.decode()
        # Should have slow log section
        assert "Slow Log" in content or "slow" in content.lower()

    def test_cache_detail_count_parameter(self, admin_client: Client, test_cache):
        """Cache detail view should accept count parameter for slowlog."""
        url = _cache_detail_url("default")
        # Test with different count values
        for count in [10, 25, 50]:
            response = admin_client.get(url + f"?count={count}")
            assert response.status_code == 200


class TestCacheAdmin:
    """Tests for the CacheAdmin class."""

    def test_changelist_view_returns_200(self, admin_client: Client, test_cache):
        """Changelist view should return 200 (it IS the cache list view)."""
        # Access the Django admin's changelist for the Cache model
        url = reverse("admin:django_cachex_cache_changelist")
        response = admin_client.get(url)

        # Should return 200 (changelist_view is the cache list now)
        assert response.status_code == 200

    def test_has_add_permission_returns_false(self, admin_client: Client, test_cache):
        """CacheAdmin should not allow adding new cache entries."""
        from django.contrib.admin import site
        from django.test import RequestFactory

        from django_cachex.admin.models import Cache

        # Get the registered admin instance
        cache_admin = site._registry[Cache]

        factory = RequestFactory()
        request = factory.get("/admin/")
        request.user = admin_client.session.get("_auth_user_id")

        # has_add_permission should return False
        assert cache_admin.has_add_permission(request) is False

    def test_has_delete_permission_returns_false(self, admin_client: Client, test_cache):
        """CacheAdmin should not allow deleting cache entries."""
        from django.contrib.admin import site
        from django.test import RequestFactory

        from django_cachex.admin.models import Cache

        cache_admin = site._registry[Cache]

        factory = RequestFactory()
        request = factory.get("/admin/")
        request.user = admin_client.session.get("_auth_user_id")

        assert cache_admin.has_delete_permission(request) is False

    def test_staff_only_permissions(self, db, test_cache):
        """Non-staff users should not have view/change/module permissions."""
        from django.contrib.admin import site
        from django.contrib.auth.models import User
        from django.test import RequestFactory

        from django_cachex.admin.models import Cache

        cache_admin = site._registry[Cache]

        # Create a non-staff user
        non_staff_user = User.objects.create_user(
            username="nonstaff",
            password="password",  # noqa: S106
            is_staff=False,
        )

        factory = RequestFactory()
        request = factory.get("/admin/")
        request.user = non_staff_user

        # Non-staff should not have permissions
        assert cache_admin.has_view_permission(request) is False
        assert cache_admin.has_change_permission(request) is False
        assert cache_admin.has_module_permission(request) is False

    def test_staff_has_permissions(self, admin_user, test_cache):
        """Staff users should have view/change/module permissions."""
        from django.contrib.admin import site
        from django.test import RequestFactory

        from django_cachex.admin.models import Cache

        cache_admin = site._registry[Cache]

        factory = RequestFactory()
        request = factory.get("/admin/")
        request.user = admin_user

        # Staff should have view/change/module permissions
        assert cache_admin.has_view_permission(request) is True
        assert cache_admin.has_change_permission(request) is True
        assert cache_admin.has_module_permission(request) is True
