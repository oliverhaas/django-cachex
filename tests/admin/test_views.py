"""Tests for django_cachex.admin views.

These tests use simple fixtures from the local conftest that don't
have the parametrization of the main test suite.
"""

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import django
import pytest
from bs4 import BeautifulSoup
from django.conf import settings
from django.contrib.admin import site
from django.contrib.admin.utils import quote
from django.contrib.auth.models import Group, Permission, User
from django.contrib.contenttypes.models import ContentType
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.core.management import call_command
from django.test import Client, RequestFactory, override_settings
from django.urls import reverse
from django.utils import translation

from django_cachex.adapters.pipeline import Pipeline
from django_cachex.admin.models import Cache, Key
from django_cachex.admin.views.key_detail import _MAX_STRING_BYTES
from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def _cache_list_url() -> str:
    """Get URL for cache list (admin changelist)."""
    return reverse("admin:django_cachex_cache_changelist")


def _cache_detail_url(cache_name: str) -> str:
    """Get URL for cache detail (admin change view)."""
    return reverse("admin:django_cachex_cache_change", args=[quote(cache_name)])


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


def _table_containing(content: bytes, needle: str):
    """Return the rendered table whose text contains ``needle``."""
    soup = BeautifulSoup(content, "html.parser")
    for table in soup.find_all("table"):
        if needle in table.get_text():
            return table
    pytest.fail(f"No table containing {needle!r} in the rendered page")


def _result_column(content: bytes, field: str) -> list[str]:
    """Return the ``field`` cell of every changelist result row."""
    soup = BeautifulSoup(content, "html.parser")
    table = soup.find("table", id="result_list")
    if table is None:
        return []
    return [cell.get_text(strip=True) for cell in table.select(f"tbody .field-{field}")]


def _key_detail_create_url(cache_name: str, key_name: str, key_type: str = "string") -> str:
    """Get URL for key detail in create mode (key doesn't exist yet)."""
    pk = Key.make_pk(cache_name, key_name)
    base = reverse("admin:django_cachex_key_change", args=[pk])
    params = urlencode({"type": key_type})
    return f"{base}?{params}"


def test_index_returns_200(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200


def test_index_shows_configured_caches(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"default" in response.content


def test_index_requires_staff(db, test_cache):
    """Index view should redirect anonymous users."""
    client = Client()
    url = _cache_list_url()
    response = client.get(url)
    assert response.status_code == 302


def test_index_help_button(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url + "?help=1")
    assert response.status_code == 200
    content = response.content.decode()
    assert "<strong>Caches</strong>" in content
    assert "Support Levels" in content


def test_index_page_title(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    assert "<title>Select Cache to change" in content
    assert "<h1>Select Cache to change</h1>" in content


def test_index_shows_support_badge(admin_client: Client, test_cache):
    """Index view should show support level badges in the cache table."""
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200

    names = _result_column(response.content, "name")
    badges = _result_column(response.content, "support_display")
    assert dict(zip(names, badges, strict=True))["default"] == "cachex"


def test_index_shows_backend_column(admin_client: Client, test_cache):
    """Index view should show backend class path in the cache table."""
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200

    data_table = _table_containing(response.content, "default")

    table_text = data_table.get_text()
    assert "ValkeyCache" in table_text or "RedisCache" in table_text or "django_cachex" in table_text, (
        f"Backend class not found in cache table: {table_text[:200]}"
    )


def test_index_shows_location_column(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200

    data_table = _table_containing(response.content, "default")

    table_text = data_table.get_text()
    assert (
        "redis://" in table_text or "valkey://" in table_text or "localhost" in table_text or "127.0.0.1" in table_text
    ), f"Cache location not found in cache table: {table_text[:200]}"


def test_index_cache_links_to_keys(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # The keys_link column emits an <a> pointing at the key changelist filtered by cache name.
    assert "?cache=default" in content
    assert "List Keys" in content


def test_index_search_filters_caches(admin_client: Client, test_cache):
    url = _cache_list_url()
    response = admin_client.get(url + "?q=default")
    assert response.status_code == 200
    # "local" is the other configured alias; the location column carries
    # "localhost", so only the result rows can prove it was filtered out.
    assert _result_column(response.content, "name") == ["default"]


def test_key_list_returns_200(admin_client: Client, test_cache):
    """Key search view should return 200."""
    url = _key_list_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200


def test_key_list_requires_staff(db, test_cache):
    """Key search view should redirect anonymous users."""
    client = Client()
    url = _key_list_url("default")
    response = client.get(url)
    assert response.status_code == 302


def test_key_list_page_title(admin_client: Client, test_cache):
    url = _key_list_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"Keys in" in response.content
    assert b"default" in response.content


def test_key_list_has_help_and_add_links(admin_client: Client, test_cache):
    url = _key_list_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # The change_list template renders the help link with `&help=1` and
    # the add-key link with `class="addlink"`; match the exact markup so
    # an unrelated word "Add" or "Help" elsewhere can't satisfy the test.
    assert "&amp;help=1" in content
    assert 'class="addlink"' in content


def test_key_list_bulk_delete(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("bulk:delete:1", "value1")
    test_cache.set("bulk:delete:2", "value2")
    test_cache.set("bulk:keep", "value3")

    url = _key_list_url("default")
    response = admin_client.post(
        url,
        {
            "action": "delete_selected_keys",
            "_selected_action": [
                Key.make_pk("default", "bulk:delete:1"),
                Key.make_pk("default", "bulk:delete:2"),
            ],
        },
    )
    assert response.status_code == 302

    assert test_cache.get("bulk:delete:1") is None
    assert test_cache.get("bulk:delete:2") is None
    assert test_cache.get("bulk:keep") == "value3"


def test_key_list_bulk_delete_underscore_hex_key(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: filter(pk__in) admin-unquoted the raw checkbox pks, so
    a key containing an ``_XX`` sequence was mangled (``_2F`` -> ``/``)
    and the wrong key was targeted.
    """
    test_cache.set("weird_2Fkey", "value")

    response = admin_client.post(
        _key_list_url("default"),
        {
            "action": "delete_selected_keys",
            "_selected_action": [Key.make_pk("default", "weird_2Fkey")],
        },
    )
    assert response.status_code == 302
    assert test_cache.get("weird_2Fkey") is None


def test_key_list_bulk_delete_reports_failures(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: per-key exceptions were swallowed, so a failed bulk
    delete reported nothing and looked like success.
    """
    test_cache.set("bulk:mixed:ok", "value")

    response = admin_client.post(
        _key_list_url("default"),
        {
            "action": "delete_selected_keys",
            "_selected_action": [
                Key.make_pk("default", "bulk:mixed:ok"),
                # get_cache() raises for unconfigured caches.
                Key.make_pk("no_such_cache", "bulk:mixed:broken"),
            ],
        },
        follow=True,
    )
    assert response.status_code == 200
    content = response.content.decode()
    assert "Successfully deleted 1 key(s)" in content
    assert "Failed to delete 1 key(s)" in content
    assert test_cache.get("bulk:mixed:ok") is None


def test_key_list_count_param_capped(
    admin_client: Client,
    mocker,
    test_cache: RespCache,
):
    """Regression: ``?count=`` was uncapped, letting a single changelist
    request drive one SCAN call over the whole keyspace.
    """
    from django_cachex.admin.queryset import MAX_SCAN_COUNT

    fake = mocker.MagicMock()
    fake.scan.return_value = (0, [])
    mocker.patch("django_cachex.admin.queryset.get_cache", return_value=fake)

    response = admin_client.get(_key_list_url("default") + "&count=999999")
    assert response.status_code == 200
    _, kwargs = fake.scan.call_args
    assert kwargs["count"] == MAX_SCAN_COUNT


def test_key_list_shows_string_keys(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("test:key1", "value1")
    test_cache.set("test:key2", "value2")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=test:*")
    assert response.status_code == 200
    assert b"test:key1" in response.content
    assert b"test:key2" in response.content


def test_key_list_empty_pattern(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("mykey", "myvalue")

    url = _key_list_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert "mykey" in _table_containing(response.content, "mykey").get_text()


def test_key_list_shows_type_column(
    admin_client: Client,
    test_cache: RespCache,
):
    """Key search should show type column with actual type values in table cells."""
    test_cache.set("type:string:test", "value")
    test_cache.rpush("type:list:test", "item1")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=type:*")
    assert response.status_code == 200

    names = _result_column(response.content, "key_name")
    types = _result_column(response.content, "type_display")
    assert dict(zip(names, types, strict=True)) == {"type:string:test": "string", "type:list:test": "list"}


def test_key_list_shows_ttl_column(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("ttl:column:test", "value", timeout=300)

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=ttl:*")
    assert response.status_code == 200
    content = response.content.decode()
    # ttl_display emits <code title="{seconds}s">{timeuntil}</code> for keys with a TTL.
    assert '<code title="' in content


def test_key_list_shows_size_column(
    admin_client: Client,
    test_cache: RespCache,
):
    """Key search should show size column with actual size values in table cells."""
    test_cache.rpush("size:list:test", "a", "b", "c")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=size:list*")
    assert response.status_code == 200

    data_table = _table_containing(response.content, "size:list:test")

    cells_text = [td.get_text(strip=True) for td in data_table.find_all("td")]

    assert "3" in cells_text, f"'3' (list size) not found in table cells: {cells_text}"


def test_key_list_wildcard_pattern(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("wild:card:one", "value1")
    test_cache.set("wild:card:two", "value2")
    test_cache.set("other:key", "value3")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=wild:card:*")
    assert response.status_code == 200
    content = response.content.decode()
    assert "wild:card:one" in content
    assert "wild:card:two" in content
    assert "other:key" not in content


def test_key_list_contains_pattern(
    admin_client: Client,
    test_cache: RespCache,
):
    """Key search without wildcards should do contains search (Django-style)."""
    test_cache.set("session:123", "value1")
    test_cache.set("user_session", "value2")
    test_cache.set("my_session_data", "value3")
    test_cache.set("unrelated:key", "value4")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=session")
    assert response.status_code == 200
    content = response.content.decode()
    assert "session:123" in content
    assert "user_session" in content
    assert "my_session_data" in content
    assert "unrelated:key" not in content


def test_key_list_pagination(
    admin_client: Client,
    test_cache: RespCache,
):
    # Create enough keys to trigger pagination (default is usually 100)
    for i in range(150):
        test_cache.set(f"paginate:key:{i:03d}", f"value{i}")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=paginate:*")
    assert response.status_code == 200
    content = response.content.decode()
    # Django's standard paginator wraps the controls in <p class="paginator">.
    assert 'class="paginator"' in content


def test_key_list_results_count(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("count:key:1", "value1")
    test_cache.set("count:key:2", "value2")
    test_cache.set("count:key:3", "value3")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=count:*")
    assert response.status_code == 200
    assert sorted(_result_column(response.content, "key_name")) == [
        "count:key:1",
        "count:key:2",
        "count:key:3",
    ]
    assert "3 keys shown" in response.content.decode()


def test_key_list_type_filter(
    admin_client: Client,
    test_cache: RespCache,
):
    """Type filter should only show keys of the selected type."""
    test_cache.set("typefilter:str", "value")
    test_cache.rpush("typefilter:lst", "item1")

    url = _key_list_url("default")
    response = admin_client.get(url + "&q=typefilter:*&type=string")
    assert response.status_code == 200
    content = response.content.decode()
    assert "typefilter:str" in content
    assert "typefilter:lst" not in content


def test_key_list_cache_filter_shown(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_list_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # Both cache names appear as filter options, which proves the filter rendered.
    assert "?cache=default" in content
    assert "?cache=local" in content


def test_key_list_cache_filter_switches_cache(
    admin_client: Client,
    test_cache: RespCache,
):

    caches["local"].set("localonly:key", "localvalue")
    url = _key_list_url("local")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    assert "localonly:key" in content
    assert "Keys in &#x27;local&#x27;" in content


def test_key_list_no_cache_param_defaults_to_first(
    admin_client: Client,
    test_cache: RespCache,
):
    """Key list without ?cache= should default to the first configured cache."""
    url = reverse("admin:django_cachex_key_changelist")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert "Keys in &#x27;default&#x27;" in response.content.decode()


def test_key_list_cache_links_are_admin_quoted(admin_client: Client):
    """Regression: the breadcrumb and 'Cache Details' links passed the raw
    cache name to {% url %}, so change_view's unquote() turned an ``_XX``
    name like ``shard_25`` into ``shard%`` and 404-redirected.
    """
    caches_config = {
        "default": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "quote-links-default",
        },
        "shard_25": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "quote-links-shard",
        },
    }
    with override_settings(CACHES=caches_config):
        response = admin_client.get(_key_list_url("shard_25"))
        assert response.status_code == 200
        content = response.content.decode()

        quoted_url = _cache_detail_url("shard_25")
        raw_url = reverse("admin:django_cachex_cache_change", args=["shard_25"])
        assert f'href="{quoted_url}"' in content
        assert raw_url not in content

        detail = admin_client.get(quoted_url)
        assert detail.status_code == 200
        assert "not found" not in detail.content.decode()


def test_string_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("string:test", "hello world")

    url = _key_detail_url("default", "string:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"hello world" in response.content


def test_dict_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    """Detail view should work for dict/JSON keys (stored as string)."""
    test_cache.set("dict:test", {"name": "Alice", "age": 30})

    url = _key_detail_url("default", "dict:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"Alice" in response.content


def test_list_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.lpush("list:test", "item1", "item2", "item3")

    url = _key_detail_url("default", "list:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert 'type-list">list</span>' in response.content.decode()


def test_set_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.sadd("set:test", "member1", "member2", "member3")

    url = _key_detail_url("default", "set:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert 'type-set">set</span>' in response.content.decode()


def test_hash_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.hset("hash:test", "field1", "value1")
    test_cache.hset("hash:test", "field2", "value2")

    url = _key_detail_url("default", "hash:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert 'type-hash">hash</span>' in response.content.decode()


def test_zset_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zset:test", {"member1": 1.0, "member2": 2.0})

    url = _key_detail_url("default", "zset:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert 'type-zset">zset</span>' in response.content.decode()


def test_nonexistent_key_detail(admin_client: Client, test_cache):
    """Detail view should redirect to key list for non-existent keys."""
    url = _key_detail_url("default", "nonexistent:key")
    response = admin_client.get(url)
    assert response.status_code == 302
    assert "cache=default" in response.url


def test_key_detail_unconfigured_cache_redirects(admin_client: Client):
    """Regression: change_view only checked for an empty cache name, so an
    alias missing from CACHES reached get_cache() and raised ValueError
    instead of redirecting the way add_view does.
    """
    url = _key_detail_url("no-such-cache", "somekey")
    response = admin_client.get(url)
    assert response.status_code == 302
    assert response.url == reverse("admin:django_cachex_cache_changelist")


def test_locmem_key_detail_has_no_conflict_warning(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: hasattr(cache, "eval_script") is always true (BaseCachex
    raising stub), so every LocMem key detail page warned that conflict
    detection is unavailable. Missing scripting support is the expected
    fallback, not warning-worthy.
    """
    caches["local"].set("locmem:detail", "value")

    url = _key_detail_url("local", "locmem:detail")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert "Conflict detection unavailable" not in response.content.decode()


def test_stream_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.xadd("stream:detail:test", {"field1": "value1"})
    test_cache.xadd("stream:detail:test", {"field2": "value2"})

    url = _key_detail_url("default", "stream:detail:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # change_form.html renders <span class="type-badge type-stream">stream</span>
    assert 'type-stream">stream</span>' in content


def test_key_detail_requires_staff(db, test_cache):
    """Key detail view should redirect anonymous users."""
    client = Client()
    url = _key_detail_url("default", "any:key")
    response = client.get(url)
    assert response.status_code == 302


def test_key_detail_page_title(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("title:test", "value")

    url = _key_detail_url("default", "title:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"Key:" in response.content
    assert b"title:test" in response.content


def test_key_detail_help_button(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("help:test", "value")

    url = _key_detail_url("default", "help:test")
    response = admin_client.get(url + "?help=1")
    assert response.status_code == 200
    content = response.content.decode()
    assert "<strong>String Key</strong>" in content
    assert "Value Format" in content


def test_key_detail_shows_raw_key(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("rawkey:test", "value")

    url = _key_detail_url("default", "rawkey:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    assert "rawkey:test" in content


def test_key_detail_shows_cache_name(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("cache:name:test", "value")

    url = _key_detail_url("default", "cache:name:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert "<code>default</code>" in response.content.decode()


def test_key_detail_shows_type_badge(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("type:badge:test", "item1", "item2", "item3")

    url = _key_detail_url("default", "type:badge:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # change_form.html renders <span class="type-badge type-list">list</span>
    assert 'type-list">list</span>' in content
    assert "3" in content


def test_key_detail_shows_ttl(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("ttl:detail:test", "value", timeout=300)

    url = _key_detail_url("default", "ttl:detail:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # change_form.html renders the TTL update form with id="id_ttl"
    assert 'id="id_ttl"' in content


def test_key_detail_shows_no_expiry(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("noexpiry:test", "value", timeout=None)

    url = _key_detail_url("default", "noexpiry:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # When the key has no expiry, the TTL input renders an empty value with the
    # "no expiry" placeholder.
    assert 'placeholder="no expiry"' in content


def test_string_value_save_form_structure(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("form:structure:test", "original value")

    url = _key_detail_url("default", "form:structure:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert 'id="key-form"' in content
    assert 'name="action" value="update"' in content
    assert 'type="submit"' in content


def test_string_value_save_button_updates_value(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("save:button:test", "original value")

    url = _key_detail_url("default", "save:button:test")

    response = admin_client.post(
        url,
        {"action": "update", "value": "updated value"},
    )
    assert response.status_code == 302

    assert test_cache.get("save:button:test") == "updated value"


def test_string_ttl_input_has_its_own_form(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("ttl:form:test", "value", timeout=300)

    url = _key_detail_url("default", "ttl:form:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert 'name="action" value="set_ttl"' in content
    assert 'name="ttl_value"' in content


def test_string_set_ttl_action_sets_ttl(
    admin_client: Client,
    test_cache: RespCache,
):
    """Setting TTL via set_ttl action should set the expiry."""
    test_cache.set("ttl:save:test", "value", timeout=None)

    url = _key_detail_url("default", "ttl:save:test")

    response = admin_client.post(
        url,
        {"action": "set_ttl", "ttl_value": "600"},
    )
    assert response.status_code == 302

    ttl = test_cache.ttl("ttl:save:test")
    assert ttl is not None
    assert 590 <= ttl <= 600  # Allow some margin for test execution time


def test_string_set_empty_ttl_persists(
    admin_client: Client,
    test_cache: RespCache,
):
    """Setting empty TTL via set_ttl action should persist (no expiry)."""
    test_cache.set("ttl:persist:test", "original", timeout=300)

    url = _key_detail_url("default", "ttl:persist:test")

    response = admin_client.post(
        url,
        {"action": "set_ttl", "ttl_value": ""},
    )
    assert response.status_code == 302

    ttl = test_cache.ttl("ttl:persist:test")
    assert ttl is None or ttl == -1  # -1 or None means no expiry


def test_list_ttl_update_sets_ttl(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("list:ttl:test", "item1", "item2")

    url = _key_detail_url("default", "list:ttl:test")

    response = admin_client.post(
        url,
        {"action": "set_ttl", "ttl_value": "600"},
    )
    assert response.status_code == 302

    ttl = test_cache.ttl("list:ttl:test")
    assert ttl is not None
    assert 590 <= ttl <= 600


def test_list_empty_ttl_persists(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("list:persist:test", "item1")
    test_cache.expire("list:persist:test", 300)

    url = _key_detail_url("default", "list:persist:test")

    response = admin_client.post(
        url,
        {"action": "set_ttl", "ttl_value": ""},
    )
    assert response.status_code == 302

    ttl = test_cache.ttl("list:persist:test")
    assert ttl is None or ttl == -1


def test_json_serializable_string_is_editable(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("json:string:test", "hello world")

    url = _key_detail_url("default", "json:string:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert "&quot;hello world&quot;" in content
    assert 'name="action" value="update"' in content


def test_json_serializable_dict_is_editable(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("json:dict:test", {"name": "Alice", "age": 30})

    url = _key_detail_url("default", "json:dict:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert "&quot;name&quot;" in content
    assert "&quot;Alice&quot;" in content
    assert "&quot;age&quot;" in content
    assert 'name="action" value="update"' in content


def test_json_serializable_list_value_is_editable(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("json:list:value:test", [1, 2, 3, "four"])

    url = _key_detail_url("default", "json:list:value:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert "[" in content
    assert "1" in content
    assert "&quot;four&quot;" in content


def test_json_indicator_shown_for_dict(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("json:indicator:test", {"type": "example"})

    url = _key_detail_url("default", "json:indicator:test")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert "JSON" in content


def test_read_only_warning_uses_theme_aware_styling(
    admin_client: Client,
    test_cache: RespCache,
):
    """The complex-value warning must theme with the admin, not hardcode colors.

    Hardcoded light-theme colors rendered as white-on-pale-yellow in dark
    mode, so the warning was unreadable exactly when it mattered.
    """
    test_cache.set("nonjson:datetime", datetime(2026, 8, 14, tzinfo=UTC))

    url = _key_detail_url("default", "nonjson:datetime")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    assert "Complex object detected." in content
    assert 'class="warningnote"' in content
    for hardcoded in ("#fef3cd", "#ffc107", "#c4820e"):
        assert hardcoded not in content


def test_add_key_get(admin_client: Client, test_cache):
    url = _key_add_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200


def test_add_key_requires_staff(db, test_cache):
    """Add key view should redirect anonymous users."""
    client = Client()
    url = _key_add_url("default")
    response = client.get(url)
    assert response.status_code == 302


def test_add_key_page_title(admin_client: Client, test_cache):
    url = _key_add_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"Add key to" in response.content
    assert b"default" in response.content


def test_add_key_help_button(admin_client: Client, test_cache):
    """Help button on add key should return 200."""
    url = _key_add_url("default")
    response = admin_client.get(url + "&help=1")
    assert response.status_code == 200


def test_add_key_with_timeout(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "timeout:test:key", "string")
    response = admin_client.post(
        url,
        {
            "action": "update",
            "value": '"expiring value"',
        },
    )
    assert response.status_code == 302

    detail_url = _key_detail_url("default", "timeout:test:key")
    response = admin_client.post(
        detail_url,
        {
            "action": "set_ttl",
            "ttl_value": "300",
        },
    )
    assert response.status_code == 302

    assert test_cache.get("timeout:test:key") == "expiring value"

    ttl = test_cache.ttl("timeout:test:key")
    assert 290 <= ttl <= 300


def test_add_key_post_string(
    admin_client: Client,
    test_cache: RespCache,
):
    """Add key view should create string keys via update action."""
    url = _key_detail_create_url("default", "new:string:key", "string")
    response = admin_client.post(
        url,
        {
            "action": "update",
            "value": '"test value"',
        },
    )
    assert response.status_code == 302

    assert test_cache.get("new:string:key") == "test value"


def test_add_key_post_json(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "new:json:key", "string")
    response = admin_client.post(
        url,
        {
            "action": "update",
            "value": '{"name": "test", "count": 42}',
        },
    )
    assert response.status_code == 302

    value = test_cache.get("new:json:key")
    assert value == {"name": "test", "count": 42}


def test_add_key_save_and_add_another(
    admin_client: Client,
    test_cache: RespCache,
):
    """Key add form should redirect to key_detail in create mode."""
    url = _key_add_url("default")
    response = admin_client.post(url, {"key": "addanother:key", "type": "string"})
    assert response.status_code == 302
    assert "addanother" in response.url and "key" in response.url
    assert "type=string" in response.url

    create_page = admin_client.get(response.url)
    assert create_page.status_code == 200
    assert "This key does not exist yet" in create_page.content.decode()

    admin_client.post(response.url, {"action": "update", "value": '"first"'})
    assert test_cache.get("addanother:key") == "first"

    second = admin_client.post(url, {"key": "addanother:second", "type": "list"})
    assert second.status_code == 302
    assert "type=list" in second.url


def test_add_key_type_list(
    admin_client: Client,
    test_cache: RespCache,
):
    """Add key should create list keys via list_rpush action."""
    url = _key_detail_create_url("default", "new:list:key", "list")

    for item in ["item1", "item2", "item3"]:
        response = admin_client.post(
            url,
            {
                "action": "rpush",
                "value": item,
            },
        )
        assert response.status_code == 302

    items = test_cache.lrange("new:list:key", 0, -1)
    assert items == ["item1", "item2", "item3"]


def test_add_key_type_set(
    admin_client: Client,
    test_cache: RespCache,
):
    """Add key should create set keys via set_sadd action."""
    url = _key_detail_create_url("default", "new:set:key", "set")

    for member in ["member1", "member2", "member3"]:
        response = admin_client.post(
            url,
            {
                "action": "sadd",
                "member": member,
            },
        )
        assert response.status_code == 302

    members = test_cache.smembers("new:set:key")
    assert members == {"member1", "member2", "member3"}


def test_add_key_type_hash(
    admin_client: Client,
    test_cache: RespCache,
):
    """Add key should create hash keys via hash_hset action."""
    url = _key_detail_create_url("default", "new:hash:key", "hash")

    for field, value in [("field1", "value1"), ("field2", "value2")]:
        response = admin_client.post(
            url,
            {
                "action": "hset",
                "field": field,
                "field_value": value,
            },
        )
        assert response.status_code == 302

    fields = test_cache.hgetall("new:hash:key")
    assert fields == {"field1": "value1", "field2": "value2"}


def test_add_key_type_zset(
    admin_client: Client,
    test_cache: RespCache,
):
    """Add key should create sorted set keys via zset_zadd action."""
    url = _key_detail_create_url("default", "new:zset:key", "zset")

    for member, score in [("member1", 1.5), ("member2", 2.5)]:
        response = admin_client.post(
            url,
            {
                "action": "zadd",
                "member": member,
                "score_value": str(score),
            },
        )
        assert response.status_code == 302

    members = test_cache.zrange("new:zset:key", 0, -1, withscores=True)
    assert members == [("member1", 1.5), ("member2", 2.5)]


def test_delete_key(admin_client: Client, test_cache: RespCache):
    test_cache.set("delete:me", "goodbye")

    url = _key_detail_url("default", "delete:me")
    response = admin_client.post(url, {"action": "delete"})
    assert response.status_code == 302

    assert test_cache.get("delete:me") is None


def test_edit_key_value(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("edit:me", "old value")

    url = _key_detail_url("default", "edit:me")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302

    assert test_cache.get("edit:me") == "new value"


def test_edit_key_value_on_locmem_backend(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: LocMem has no real pttl(), only BaseCachex's raising
    stub, so hasattr-gated TTL preservation blew up and the edit was
    rejected with an error instead of falling back to a plain set().
    """
    local = caches["local"]
    local.set("edit:locmem", "old value")

    url = _key_detail_url("local", "edit:locmem")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302
    assert local.get("edit:locmem") == "new value"


def test_edit_key_value_on_locmem_preserves_existing_ttl(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: LocMem raises on pttl(), so the edit fell through to a
    plain set() and silently reset the key's TTL to the default timeout.
    """
    local = caches["local"]
    local.set("edit:locmem:ttl", "old value", timeout=3600)

    url = _key_detail_url("local", "edit:locmem:ttl")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302
    assert local.get("edit:locmem:ttl") == "new value"
    assert local.ttl("edit:locmem:ttl") > 3000


def test_edit_key_value_on_locmem_preserves_persistent_key(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: without pttl(), a persistent LocMem key was stamped with
    the default timeout and started expiring.
    """
    local = caches["local"]
    local.set("edit:locmem:persistent", "old value", timeout=None)

    url = _key_detail_url("local", "edit:locmem:persistent")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302
    assert local.get("edit:locmem:persistent") == "new value"
    assert local.ttl("edit:locmem:persistent") is None


def test_edit_key_value_preserves_persistent_key(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: editing a key without TTL stamped it with the default
    timeout, silently making a persistent key expire.
    """
    test_cache.set("edit:persistent", "old value", timeout=None)

    url = _key_detail_url("default", "edit:persistent")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302
    assert test_cache.get("edit:persistent") == "new value"
    ttl = test_cache.ttl("edit:persistent")
    assert ttl is None or ttl == -1


def test_edit_key_value_preserves_existing_ttl(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("edit:with:ttl", "old value", timeout=600)

    url = _key_detail_url("default", "edit:with:ttl")
    response = admin_client.post(
        url,
        {"action": "update", "value": "new value"},
    )
    assert response.status_code == 302
    assert test_cache.get("edit:with:ttl") == "new value"
    ttl = test_cache.ttl("edit:with:ttl")
    assert ttl is not None
    assert 590 <= ttl <= 600


def test_list_lrem(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("lrem:test", "item1", "item2", "item3", "item2")

    url = _key_detail_url("default", "lrem:test")
    response = admin_client.post(
        url,
        {"action": "lrem", "value": "item2", "lrem_count": "0"},
    )
    assert response.status_code == 302

    # count=0 is LREM's "remove every occurrence"
    items = test_cache.lrange("lrem:test", 0, -1)
    assert items == ["item1", "item3"]


def test_list_lrem_defaults_to_a_single_occurrence(
    admin_client: Client,
    test_cache: RespCache,
):
    """The per-row Remove button says "remove this item", so an absent
    ``lrem_count`` must not fall through to LREM's "remove all" mode.
    """
    test_cache.rpush("lrem:once", "item1", "item2", "item3", "item2")

    response = admin_client.post(
        _key_detail_url("default", "lrem:once"),
        {"action": "lrem", "value": "item2"},
    )
    assert response.status_code == 302

    assert test_cache.lrange("lrem:once", 0, -1) == ["item1", "item3", "item2"]


def test_list_ltrim(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("ltrim:test", "a", "b", "c", "d", "e")

    url = _key_detail_url("default", "ltrim:test")
    response = admin_client.post(
        url,
        {"action": "ltrim", "trim_start": "1", "trim_stop": "3"},
    )
    assert response.status_code == 302

    items = test_cache.lrange("ltrim:test", 0, -1)
    assert items == ["b", "c", "d"]


def test_hash_field_inline_edit(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.hset("hash:edit", "name", "old_value")

    url = _key_detail_url("default", "hash:edit")
    response = admin_client.post(
        url,
        {"action": "hset", "field": "name", "field_value": "new_value"},
    )
    assert response.status_code == 302

    assert test_cache.hget("hash:edit", "name") == "new_value"


def test_hash_hdel(
    admin_client: Client,
    test_cache: RespCache,
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

    assert test_cache.hget("hash:hdel", "field2") is None
    assert test_cache.hget("hash:hdel", "field1") == "value1"
    assert test_cache.hget("hash:hdel", "field3") == "value3"


def test_list_lpop(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("lpop:test", "a", "b", "c")

    url = _key_detail_url("default", "lpop:test")
    response = admin_client.post(url, {"action": "lpop"})
    assert response.status_code == 302

    items = test_cache.lrange("lpop:test", 0, -1)
    assert items == ["b", "c"]


def test_list_lpop_with_count(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("lpop:count:test", "a", "b", "c", "d", "e")

    url = _key_detail_url("default", "lpop:count:test")
    response = admin_client.post(
        url,
        {"action": "lpop", "pop_count": "3"},
    )
    assert response.status_code == 302

    items = test_cache.lrange("lpop:count:test", 0, -1)
    assert items == ["d", "e"]


def test_list_rpop(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("rpop:test", "a", "b", "c")

    url = _key_detail_url("default", "rpop:test")
    response = admin_client.post(url, {"action": "rpop"})
    assert response.status_code == 302

    items = test_cache.lrange("rpop:test", 0, -1)
    assert items == ["a", "b"]


def test_list_rpop_with_count(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("rpop:count:test", "a", "b", "c", "d", "e")

    url = _key_detail_url("default", "rpop:count:test")
    response = admin_client.post(
        url,
        {"action": "rpop", "pop_count": "3"},
    )
    assert response.status_code == 302

    items = test_cache.lrange("rpop:count:test", 0, -1)
    assert items == ["a", "b"]


def test_list_lpush(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("lpush:test", "b", "c")

    url = _key_detail_url("default", "lpush:test")
    response = admin_client.post(
        url,
        {"action": "lpush", "value": "a"},
    )
    assert response.status_code == 302

    items = test_cache.lrange("lpush:test", 0, -1)
    assert items == ["a", "b", "c"]


def test_list_rpush(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.rpush("rpush:test", "a", "b")

    url = _key_detail_url("default", "rpush:test")
    response = admin_client.post(
        url,
        {"action": "rpush", "value": "c"},
    )
    assert response.status_code == 302

    items = test_cache.lrange("rpush:test", 0, -1)
    assert items == ["a", "b", "c"]


def test_set_sadd(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.sadd("sadd:test", "a", "b")

    url = _key_detail_url("default", "sadd:test")
    response = admin_client.post(
        url,
        {"action": "sadd", "member": "c"},
    )
    assert response.status_code == 302

    members = test_cache.smembers("sadd:test")
    assert members == {"a", "b", "c"}


def test_set_srem(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.sadd("srem:test", "a", "b", "c")

    url = _key_detail_url("default", "srem:test")
    response = admin_client.post(
        url,
        {"action": "srem", "member": "b"},
    )
    assert response.status_code == 302

    members = test_cache.smembers("srem:test")
    assert members == {"a", "c"}


def test_set_spop(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.sadd("spop:test", "only_member")

    url = _key_detail_url("default", "spop:test")
    response = admin_client.post(url, {"action": "spop"})
    assert response.status_code == 302

    members = test_cache.smembers("spop:test")
    assert members == set()


def test_popping_the_last_member_lands_in_create_mode(
    admin_client: Client,
    test_cache: RespCache,
):
    """Emptying a key keeps the user on it instead of bouncing to the key list."""
    test_cache.sadd("spop:last", "only_member")

    response = admin_client.post(_key_detail_url("default", "spop:last"), {"action": "spop"}, follow=True)

    assert response.status_code == 200
    assert response.redirect_chain[-1][0].endswith("?type=set")
    content = response.content.decode()
    assert "Popped" in content
    assert "This key does not exist yet" in content
    assert "does not exist in cache" not in content


def test_removing_the_last_member_of_a_zset_lands_in_create_mode(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zrem:last", {"only": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "zrem:last"),
        {"action": "zrem", "member": "only"},
        follow=True,
    )

    assert response.status_code == 200
    assert response.redirect_chain[-1][0].endswith("?type=zset")
    assert "This key does not exist yet" in response.content.decode()


def test_set_spop_with_count(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.sadd("spop:count:test", "a", "b", "c", "d", "e")

    url = _key_detail_url("default", "spop:count:test")
    response = admin_client.post(
        url,
        {"action": "spop", "pop_count": "3"},
    )
    assert response.status_code == 302

    members = test_cache.smembers("spop:count:test")
    assert len(members) == 2


def test_zset_zadd(
    admin_client: Client,
    test_cache: RespCache,
):
    """ZADD action should add member with score to sorted set."""
    test_cache.zadd("zadd:test", {"a": 1.0})

    url = _key_detail_url("default", "zadd:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "b", "score_value": "2.5"},
    )
    assert response.status_code == 302

    members = test_cache.zrange("zadd:test", 0, -1, withscores=True)
    assert members == [("a", 1.0), ("b", 2.5)]


def test_zset_zadd_nx_flag(
    admin_client: Client,
    test_cache: RespCache,
):
    """ZADD action with NX flag should only add new members, not update existing."""
    test_cache.zadd("zadd:nx:test", {"a": 1.0})

    url = _key_detail_url("default", "zadd:nx:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "99.0", "zadd_nx": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:nx:test", "a")
    assert score == 1.0

    response = admin_client.post(
        url,
        {"action": "zadd", "member": "b", "score_value": "2.0", "zadd_nx": "on"},
    )
    assert response.status_code == 302

    members = test_cache.zrange("zadd:nx:test", 0, -1, withscores=True)
    assert members == [("a", 1.0), ("b", 2.0)]


def test_zset_zadd_xx_flag(
    admin_client: Client,
    test_cache: RespCache,
):
    """ZADD action with XX flag should only update existing members."""
    test_cache.zadd("zadd:xx:test", {"a": 1.0})

    url = _key_detail_url("default", "zadd:xx:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "b", "score_value": "2.0", "zadd_xx": "on"},
    )
    assert response.status_code == 302

    members = test_cache.zrange("zadd:xx:test", 0, -1, withscores=True)
    assert members == [("a", 1.0)]

    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "99.0", "zadd_xx": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:xx:test", "a")
    assert score == 99.0


def test_zset_zadd_gt_flag(
    admin_client: Client,
    test_cache: RespCache,
):
    """ZADD action with GT flag should only update if new score > current."""
    test_cache.zadd("zadd:gt:test", {"a": 10.0})

    url = _key_detail_url("default", "zadd:gt:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "5.0", "zadd_gt": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:gt:test", "a")
    assert score == 10.0

    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "20.0", "zadd_gt": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:gt:test", "a")
    assert score == 20.0


def test_zset_zadd_lt_flag(
    admin_client: Client,
    test_cache: RespCache,
):
    """ZADD action with LT flag should only update if new score < current."""
    test_cache.zadd("zadd:lt:test", {"a": 10.0})

    url = _key_detail_url("default", "zadd:lt:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "20.0", "zadd_lt": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:lt:test", "a")
    assert score == 10.0

    response = admin_client.post(
        url,
        {"action": "zadd", "member": "a", "score_value": "5.0", "zadd_lt": "on"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zadd:lt:test", "a")
    assert score == 5.0


def test_zset_zrem(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zrem:test", {"a": 1.0, "b": 2.0, "c": 3.0})

    url = _key_detail_url("default", "zrem:test")
    response = admin_client.post(
        url,
        {"action": "zrem", "member": "b"},
    )
    assert response.status_code == 302

    members = test_cache.zrange("zrem:test", 0, -1, withscores=True)
    assert members == [("a", 1.0), ("c", 3.0)]


def test_zset_zpopmin(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zpopmin:test", {"a": 1.0, "b": 2.0, "c": 3.0})

    url = _key_detail_url("default", "zpopmin:test")
    response = admin_client.post(url, {"action": "zpopmin"})
    assert response.status_code == 302

    members = test_cache.zrange("zpopmin:test", 0, -1, withscores=True)
    assert members == [("b", 2.0), ("c", 3.0)]


def test_zset_zpopmax(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zpopmax:test", {"a": 1.0, "b": 2.0, "c": 3.0})

    url = _key_detail_url("default", "zpopmax:test")
    response = admin_client.post(url, {"action": "zpopmax"})
    assert response.status_code == 302

    members = test_cache.zrange("zpopmax:test", 0, -1, withscores=True)
    assert members == [("a", 1.0), ("b", 2.0)]


def test_zset_score_inline_edit(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.zadd("zscore:test", {"a": 1.0, "b": 2.0})

    url = _key_detail_url("default", "zscore:test")
    response = admin_client.post(
        url,
        {"action": "zadd", "member": "b", "score_value": "5.5"},
    )
    assert response.status_code == 302

    score = test_cache.zscore("zscore:test", "b")
    assert score == 5.5


def test_unknown_action_is_rejected(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("persist:test", "value")

    url = _key_detail_url("default", "persist:test")
    response = admin_client.post(url, {"action": "persist"}, follow=True)
    assert response.status_code == 200
    assert "Unknown action: &#x27;persist&#x27;." in response.content.decode()


def test_stream_xadd(
    admin_client: Client,
    test_cache: RespCache,
):
    """XADD action should add an entry to the stream."""
    test_cache.xadd("xadd:test", {"field1": "value1"})

    url = _key_detail_url("default", "xadd:test")
    response = admin_client.post(
        url,
        {"action": "xadd", "field": "field2", "field_value": "value2"},
    )
    assert response.status_code == 302

    entries = test_cache.xrange("xadd:test")
    assert len(entries) == 2
    last_entry = entries[-1]
    assert last_entry[1] == {"field2": "value2"}


def test_stream_xdel(
    admin_client: Client,
    test_cache: RespCache,
):
    """XDEL action should delete an entry from the stream."""
    test_cache.xadd("xdel:test", {"field1": "value1"})
    entry_id2 = test_cache.xadd("xdel:test", {"field2": "value2"})
    test_cache.xadd("xdel:test", {"field3": "value3"})

    assert test_cache.xlen("xdel:test") == 3

    url = _key_detail_url("default", "xdel:test")
    response = admin_client.post(
        url,
        {"action": "xdel", "entry_id": entry_id2},
    )
    assert response.status_code == 302

    assert test_cache.xlen("xdel:test") == 2


def test_stream_xtrim(
    admin_client: Client,
    test_cache: RespCache,
):
    """XTRIM must trim to exactly the requested length.

    The backend defaults to ``MAXLEN ~``, which Redis is free to leave
    longer than asked; the confirm dialog promises an exact trim, so the
    admin passes ``approximate=False``.
    """
    for i in range(10):
        test_cache.xadd("xtrim:test", {f"field{i}": f"value{i}"})

    assert test_cache.xlen("xtrim:test") == 10

    url = _key_detail_url("default", "xtrim:test")
    response = admin_client.post(
        url,
        {"action": "xtrim", "maxlen": "3"},
        follow=True,
    )
    assert response.status_code == 200

    assert test_cache.xlen("xtrim:test") == 3

    messages = [str(m.message) for m in response.context.get("messages", [])]
    assert any("Trimmed 7 entries" in m for m in messages), f"Expected trim success message, got: {messages}"


# Every value editor shares one textarea partial and one parsing contract.
def test_operations_forms_use_the_shared_textarea(admin_client: Client, test_cache: RespCache):
    """Push/add inputs were single-line text fields, so multi-line JSON was unenterable."""
    test_cache.lpush("inputs:list", "a")

    response = admin_client.get(_key_detail_url("default", "inputs:list"))
    content = response.content.decode()

    for field_id in ("id_lpush_value", "id_rpush_value"):
        assert f'id="{field_id}" name="value" rows="4" class="cachex-value-input"' in content


def test_item_rows_use_the_shared_textarea(admin_client: Client, test_cache: RespCache):
    test_cache.rpush("inputs:rows", "a", "b")

    response = admin_client.get(_key_detail_url("default", "inputs:rows"))
    content = response.content.decode()

    assert 'name="value" rows="2" class="cachex-value-input" form="list-update-0"' in content
    assert 'name="value" rows="2" class="cachex-value-input" form="list-update-1"' in content


def test_zset_member_is_editable(admin_client: Client, test_cache: RespCache):
    """Members used to render as static <code>, so only the score could be changed."""
    test_cache.zadd("inputs:zset", {"alpha": 1.0})

    response = admin_client.get(_key_detail_url("default", "inputs:zset"))
    content = response.content.decode()

    assert 'name="new_member" rows="2" class="cachex-value-input" form="zset-update-0"' in content
    assert 'name="action" value="zupdate"' in content


def test_non_serializable_entry_is_read_only(admin_client: Client, test_cache: RespCache):
    """A repr() rendering cannot be submitted back: it would store the repr text."""
    test_cache.rpush("inputs:repr", datetime(2026, 8, 21, tzinfo=UTC))

    response = admin_client.get(_key_detail_url("default", "inputs:repr"))
    content = response.content.decode()

    assert "readonly" in content
    assert "datetime.datetime(2026, 8, 21" in content
    assert content.count("disabled") >= 2


def test_zset_member_can_be_renamed(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("inputs:rename", {"alpha": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "inputs:rename"),
        {"action": "zupdate", "member": '"alpha"', "new_member": '"beta"', "score_value": "3.5"},
    )
    assert response.status_code == 302
    assert test_cache.zrange("inputs:rename", 0, -1, withscores=True) == [("beta", 3.5)]


def test_zset_score_only_edit_keeps_the_member(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("inputs:score", {"alpha": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "inputs:score"),
        {"action": "zupdate", "member": '"alpha"', "new_member": '"alpha"', "score_value": "9"},
    )
    assert response.status_code == 302
    assert test_cache.zrange("inputs:score", 0, -1, withscores=True) == [("alpha", 9.0)]


def test_set_member_can_be_replaced(admin_client: Client, test_cache: RespCache):
    test_cache.sadd("inputs:set", "alpha")

    response = admin_client.post(
        _key_detail_url("default", "inputs:set"),
        {"action": "supdate", "member": '"alpha"', "new_member": '"beta"'},
    )
    assert response.status_code == 302
    assert test_cache.smembers("inputs:set") == {"beta"}


def test_xadd_parses_json_like_every_other_value(admin_client: Client, test_cache: RespCache):
    """xadd was the one handler that stored its value as a raw string."""
    response = admin_client.post(
        _key_detail_create_url("default", "inputs:stream", "string"),
        {"action": "xadd", "field": "payload", "field_value": "42"},
    )
    assert response.status_code == 302

    entries = test_cache.xrange("inputs:stream")
    assert entries[0][1] == {"payload": 42}


def test_hash_field_is_renameable(admin_client: Client, test_cache: RespCache):
    """Field names used to render as static <code>, so only the value could be changed."""
    test_cache.hset("inputs:hash", "field1", "a")

    response = admin_client.get(_key_detail_url("default", "inputs:hash"))
    content = response.content.decode()

    assert 'name="new_field" value="field1" class="cachex-value-input" form="hash-update-0"' in content
    assert 'name="action" value="hupdate"' in content


def test_hash_field_name_is_read_only_for_a_viewer(db, test_cache: RespCache):
    """Regression: the field-name input followed only the JSON gate, so a view-only user could type into it."""
    test_cache.hset("inputs:hash_viewer", "field1", "a")
    client = _staff_client(["view_key"])

    response = client.get(_key_detail_url("default", "inputs:hash_viewer"))
    content = response.content.decode()

    assert response.status_code == 200
    assert re.search(r'name="new_field" value="field1"[^>]*\sreadonly\s', content)
    assert re.search(r'name="field_value"[^>]*\sreadonly\s', content)


def test_hash_field_can_be_renamed(admin_client: Client, test_cache: RespCache):
    test_cache.hset("inputs:hrename", "old", "a")

    response = admin_client.post(
        _key_detail_url("default", "inputs:hrename"),
        {"action": "hupdate", "field": "old", "new_field": "new", "field_value": '"b"'},
    )
    assert response.status_code == 302
    assert test_cache.hgetall("inputs:hrename") == {"new": "b"}


def test_hash_rename_refuses_to_clobber_an_existing_field(admin_client: Client, test_cache: RespCache):
    test_cache.hset("inputs:hclobber", "old", "a")
    test_cache.hset("inputs:hclobber", "taken", "b")

    response = admin_client.post(
        _key_detail_url("default", "inputs:hclobber"),
        {"action": "hupdate", "field": "old", "new_field": "taken", "field_value": '"c"'},
    )
    assert response.status_code == 302
    assert test_cache.hgetall("inputs:hclobber") == {"old": "a", "taken": "b"}


def test_hash_value_only_edit_keeps_the_field(admin_client: Client, test_cache: RespCache):
    test_cache.hset("inputs:hvalue", "field1", "a")

    response = admin_client.post(
        _key_detail_url("default", "inputs:hvalue"),
        {"action": "hupdate", "field": "field1", "new_field": "field1", "field_value": '"b"'},
    )
    assert response.status_code == 302
    assert test_cache.hgetall("inputs:hvalue") == {"field1": "b"}


def test_string_update_strips_surrounding_whitespace(admin_client: Client, test_cache: RespCache):
    """Every container handler stripped; the string editor did not."""
    test_cache.set("inputs:string", "old")

    response = admin_client.post(
        _key_detail_url("default", "inputs:string"),
        {"action": "update", "value": '  "new"  '},
    )
    assert response.status_code == 302
    assert test_cache.get("inputs:string") == "new"


@pytest.mark.usefixtures("_allow_flush")
def test_flush_cache(
    admin_client: Client,
    test_cache: RespCache,
):
    test_cache.set("flush:key1", "value1")
    test_cache.set("flush:key2", "value2")

    url = _cache_list_url()
    response = admin_client.post(
        url,
        {"action": "flush_selected", "_selected_action": ["default"]},
    )
    assert response.status_code == 302

    assert test_cache.get("flush:key1") is None
    assert test_cache.get("flush:key2") is None


def test_create_mode_returns_200(
    admin_client: Client,
    test_cache: RespCache,
):
    """Create mode should return 200, not redirect to key list."""
    url = _key_detail_create_url("default", "newkey:create", "list")
    response = admin_client.get(url)

    assert response.status_code == 200


def test_create_mode_shows_message(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:message", "list")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert "does not exist yet" in content


def test_create_mode_shows_operations(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:ops", "list")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert "Operations" in content


def test_create_mode_list_shows_push_form(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:list:push", "list")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="action" value="lpush"' in content
    assert 'name="action" value="rpush"' in content


def test_create_mode_set_shows_add_form(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:set:add", "set")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="action" value="sadd"' in content


def test_create_mode_hash_shows_set_form(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:hash:set", "hash")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="action" value="hset"' in content


def test_create_mode_zset_shows_add_form(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:zset:add", "zset")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="action" value="zadd"' in content


def test_create_mode_string_shows_value_form(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _key_detail_create_url("default", "newkey:string:edit", "string")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode()
    # The string-value form has a textarea with id="id_value".
    assert 'id="id_value"' in content


def test_create_mode_operation_creates_key(
    admin_client: Client,
    test_cache: RespCache,
):
    assert test_cache.get("createkey:list") is None

    url = _key_detail_create_url("default", "createkey:list", "list")

    response = admin_client.post(
        url,
        {"action": "rpush", "value": "first_item"},
    )

    assert response.status_code == 302

    items = test_cache.lrange("createkey:list", 0, -1)
    assert items == ["first_item"]


def test_create_mode_operation_redirects_to_key_detail(
    admin_client: Client,
    test_cache: RespCache,
):
    """After creating key via operation, should redirect to key detail (not key list)."""
    url = _key_detail_create_url("default", "createkey:redirect", "list")

    response = admin_client.post(
        url,
        {"action": "rpush", "value": "item"},
    )

    assert response.status_code == 302

    assert "createkey" in response.url
    assert "changelist" not in response.url


def test_create_mode_without_type_redirects(
    admin_client: Client,
    test_cache: RespCache,
):
    """Non-existing key without type param should redirect to key list."""
    url = _key_detail_url("default", "nonexistent:key:notype")
    response = admin_client.get(url)

    assert response.status_code == 302
    assert "cache=default" in response.url


def test_cache_detail_returns_200(admin_client: Client, test_cache):
    """Cache detail view should return 200 for authenticated staff."""
    url = _cache_detail_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200


def test_cache_detail_requires_staff(db, test_cache):
    """Cache detail view should redirect anonymous users."""
    client = Client()
    url = _cache_detail_url("default")
    response = client.get(url)
    assert response.status_code == 302


def test_cache_detail_follows_changelist_link_for_underscore_alias(
    admin_client: Client,
):
    """Regression: admin quoting escapes ``_`` as ``_5F``, so the changelist
    row links to ``my_5Fcache``. change_view used to pass that through raw
    and report the alias as missing.
    """
    caches_config = {
        "my_cache": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "underscore-alias",
        },
    }
    with override_settings(CACHES=caches_config):
        changelist = admin_client.get(
            reverse("admin:django_cachex_cache_changelist"),
        )
        assert changelist.status_code == 200

        url = _cache_detail_url("my_cache")
        assert "my_5Fcache" in url
        assert f'href="{url}"' in changelist.content.decode()

        detail = admin_client.get(url)
        assert detail.status_code == 200
        assert "not found" not in detail.content.decode()


def test_cache_detail_shows_cache_name(admin_client: Client, test_cache):
    url = _cache_detail_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"default" in response.content


def test_cache_detail_displays_configuration(
    admin_client: Client,
    test_cache: RespCache,
):
    url = _cache_detail_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"default" in response.content
    assert b"django_cachex" in response.content
    assert b"Configuration" in response.content


def test_cache_detail_has_keys_link(admin_client: Client, test_cache):
    url = _cache_detail_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # change_form.html renders <a href="...?cache=default">List Keys</a>
    assert "?cache=default" in content
    assert "List Keys" in content


def test_cache_detail_shows_slowlog_section(admin_client: Client, test_cache):
    url = _cache_detail_url("default")
    response = admin_client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    # change_form.html renders <h2>Slow Log</h2> when slowlog_data is truthy
    # (always: get_slowlog returns a dict even when empty).
    assert "<h2>Slow Log</h2>" in content


def test_cache_detail_count_parameter(admin_client: Client, test_cache):
    url = _cache_detail_url("default")
    for count in [10, 25, 50]:
        response = admin_client.get(url + f"?count={count}")
        assert response.status_code == 200


def test_make_pk_parse_pk_roundtrip_plain():
    assert Key.parse_pk(Key.make_pk("default", "user:1")) == ("default", "user:1")


def test_make_pk_parse_pk_roundtrip_colon_in_cache_name():
    """Regression: parse_pk split on the first colon, so a cache name
    containing ':' donated its tail to the key name.
    """
    assert Key.parse_pk(Key.make_pk("tier:hot", "user:1")) == ("tier:hot", "user:1")


# The space is on purpose, and Django warns that memcached would reject it.
@pytest.mark.filterwarnings("ignore::django.core.cache.CacheKeyWarning")
def test_key_detail_url_roundtrips_special_characters(
    admin_client: Client,
    test_cache: RespCache,
):
    """Regression: key names went into detail URLs without the admin's pk
    quoting, so keys with '/', ' ', '%', ':' or ``_XX`` sequences broke
    the link or resolved to a different key.
    """
    from django_cachex.admin.views.base import key_detail_url

    for key in ("a/b c%d:e", "literal_2Funderscore"):
        test_cache.set(key, "value")
        response = admin_client.get(key_detail_url("default", key))
        assert response.status_code == 200, key
        assert response.context["key"] == key
        assert response.context["cache_name"] == "default"


def test_querysets_declare_total_ordering():
    """Regression: Django 6.1 ChangeList reads queryset.totally_ordered.

    Both duck-typed querysets must expose it or every changelist view
    crashes with AttributeError in django/contrib/admin/views/main.py.
    """
    from django_cachex.admin.queryset import CacheQuerySet, KeyQuerySet

    assert CacheQuerySet([]).totally_ordered is True
    assert KeyQuerySet([], "default").totally_ordered is True


def test_cache_admin_has_add_permission_returns_false(admin_user, test_cache):
    """CacheAdmin should not allow adding new cache entries, even for a superuser."""
    cache_admin = site._registry[Cache]

    request = RequestFactory().get("/admin/")
    request.user = admin_user

    assert cache_admin.has_add_permission(request) is False


def test_cache_admin_has_delete_permission_returns_false(admin_user, test_cache):
    """CacheAdmin should not allow deleting cache entries, even for a superuser."""
    cache_admin = site._registry[Cache]

    request = RequestFactory().get("/admin/")
    request.user = admin_user

    assert cache_admin.has_delete_permission(request) is False


def test_cache_admin_non_staff_has_no_permissions(db, test_cache):
    cache_admin = site._registry[Cache]

    non_staff_user = User.objects.create_user(
        username="nonstaff",
        password="password",  # noqa: S106
        is_staff=False,
    )

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = non_staff_user

    assert cache_admin.has_view_permission(request) is False
    assert cache_admin.has_change_permission(request) is False
    assert cache_admin.has_module_permission(request) is False


def test_cache_admin_superuser_has_all_permissions(admin_user, test_cache):
    cache_admin = site._registry[Cache]

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = admin_user

    assert cache_admin.has_view_permission(request) is True
    assert cache_admin.has_change_permission(request) is True
    assert cache_admin.has_module_permission(request) is True


def test_cache_admin_staff_without_perms_has_no_permissions(db, test_cache):
    """Staff users without explicit permissions should be denied."""
    cache_admin = site._registry[Cache]

    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = staff_user

    assert cache_admin.has_view_permission(request) is False
    assert cache_admin.has_change_permission(request) is False


def test_cache_admin_staff_with_view_perm_can_view(db, test_cache):
    cache_admin = site._registry[Cache]

    staff_user = User.objects.create_user(
        username="staff_viewer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    perm = Permission.objects.get(
        codename="view_cache",
        content_type__app_label="django_cachex",
    )
    staff_user.user_permissions.add(perm)
    # Refetch to clear permission cache
    staff_user = User.objects.get(pk=staff_user.pk)

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = staff_user

    assert cache_admin.has_view_permission(request) is True
    assert cache_admin.has_change_permission(request) is False


def test_cache_admin_staff_with_change_perm_can_change(db, test_cache):
    cache_admin = site._registry[Cache]

    staff_user = User.objects.create_user(
        username="staff_changer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    perm = Permission.objects.get(
        codename="change_cache",
        content_type__app_label="django_cachex",
    )
    staff_user.user_permissions.add(perm)
    staff_user = User.objects.get(pk=staff_user.pk)

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = staff_user

    assert cache_admin.has_change_permission(request) is True
    # change_cache also grants view (Django default behavior)
    assert cache_admin.has_view_permission(request) is True


def test_keys_link_urlencodes_cache_name(test_cache):
    """Regression: the ?cache= query parameter was string-concatenated,
    so cache names containing '&' or spaces produced broken links.
    """
    weird = "we ird&name"
    caches_config = {
        **settings.CACHES,
        weird: {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "keys-link-urlencode-test",
        },
    }
    with override_settings(CACHES=caches_config):
        cache_obj = Cache.get_by_name(weird)
        assert cache_obj is not None
        html = site._registry[Cache].keys_link(cache_obj)
    assert "cache=we+ird%26name" in html


def test_get_actions_emits_no_deprecation_warning(rf, admin_user, test_cache):
    """Regression: the override lacked the ``action_location`` parameter,
    so Django 6.1's shim emitted RemovedInDjango70Warning on every
    changelist request.
    """
    import warnings

    from django.utils.deprecation import RemovedInDjango70Warning

    key_admin = site._registry[Key]
    request = rf.get(reverse("admin:django_cachex_key_changelist"))
    request.user = admin_user

    # Django 6.1 routes through a shim that warns when get_actions()
    # lacks the action_location parameter; Django 6.0 has no shim.
    shim = getattr(key_admin, "_get_actions_with_action_location", key_admin.get_actions)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RemovedInDjango70Warning)
        actions = shim(request)

    assert "delete_selected" not in actions
    assert "delete_selected_keys" in actions


def test_key_admin_module_permission_true(admin_user, test_cache):
    key_admin = site._registry[Key]

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = admin_user

    assert key_admin.has_module_permission(request) is True


def test_superuser_has_all_key_permissions(admin_user, test_cache):
    key_admin = site._registry[Key]

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = admin_user

    assert key_admin.has_view_permission(request) is True
    assert key_admin.has_add_permission(request) is True
    assert key_admin.has_change_permission(request) is True
    assert key_admin.has_delete_permission(request) is True


def test_key_admin_staff_without_perms_denied(db, test_cache):
    """Staff users without permissions should be denied."""
    key_admin = site._registry[Key]

    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = staff_user

    assert key_admin.has_view_permission(request) is False
    assert key_admin.has_add_permission(request) is False
    assert key_admin.has_change_permission(request) is False
    assert key_admin.has_delete_permission(request) is False


def test_staff_with_view_key_perm(db, test_cache):
    key_admin = site._registry[Key]

    staff_user = User.objects.create_user(
        username="staff_viewer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    perm = Permission.objects.get(
        codename="view_key",
        content_type__app_label="django_cachex",
    )
    staff_user.user_permissions.add(perm)
    staff_user = User.objects.get(pk=staff_user.pk)

    factory = RequestFactory()
    request = factory.get("/admin/")
    request.user = staff_user

    assert key_admin.has_view_permission(request) is True
    assert key_admin.has_add_permission(request) is False
    assert key_admin.has_change_permission(request) is False
    assert key_admin.has_delete_permission(request) is False


def test_staff_without_perms_denied_cache_list(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )
    client = Client()
    client.force_login(staff_user)

    response = client.get(_cache_list_url())
    assert response.status_code == 403


def test_staff_without_perms_denied_cache_detail(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )
    client = Client()
    client.force_login(staff_user)

    response = client.get(_cache_detail_url("default"))
    assert response.status_code == 403


def test_staff_without_perms_denied_key_list(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )
    client = Client()
    client.force_login(staff_user)

    response = client.get(_key_list_url("default"))
    assert response.status_code == 403


def test_staff_without_perms_denied_key_add(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_no_perms",
        password="password",  # noqa: S106
        is_staff=True,
    )
    client = Client()
    client.force_login(staff_user)

    response = client.get(_key_add_url("default"))
    assert response.status_code == 403


def test_staff_with_view_perm_can_access_cache_list(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_viewer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    perm = Permission.objects.get(
        codename="view_cache",
        content_type__app_label="django_cachex",
    )
    staff_user.user_permissions.add(perm)

    client = Client()
    client.force_login(staff_user)

    response = client.get(_cache_list_url())
    assert response.status_code == 200


@pytest.mark.usefixtures("_allow_flush")
def test_view_only_user_cannot_flush_cache(db, test_cache):
    """Staff user with only view_cache perm cannot flush via action.

    Django's standard admin action handling rejects the action silently
    (the action isn't in the user's available choices) and redirects.
    """
    staff_user = User.objects.create_user(
        username="staff_viewer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    perm = Permission.objects.get(
        codename="view_cache",
        content_type__app_label="django_cachex",
    )
    staff_user.user_permissions.add(perm)

    client = Client()
    client.force_login(staff_user)

    test_cache.set("flush_test_key", "value")

    response = client.post(
        _cache_list_url(),
        {"action": "flush_selected", "_selected_action": ["default"]},
    )
    # Django standard admin re-renders changelist (action not available)
    assert response.status_code == 200
    assert test_cache.get("flush_test_key") == "value"


def test_view_only_user_cannot_delete_keys(db, test_cache):
    """Staff user with view_key but not delete_key cannot bulk-delete keys."""
    staff_user = User.objects.create_user(
        username="staff_viewer",
        password="password",  # noqa: S106
        is_staff=True,
    )
    for codename in ("view_key", "access_default"):
        staff_user.user_permissions.add(
            Permission.objects.get(codename=codename, content_type__app_label="django_cachex"),
        )

    client = Client()
    client.force_login(staff_user)

    test_cache.set("test_key", "value")
    client.post(
        _key_list_url("default"),
        {
            "action": "delete_selected_keys",
            "_selected_action": [Key.make_pk("default", "test_key")],
        },
    )
    # Django admin silently ignores unauthorized actions; verify key not deleted
    assert test_cache.get("test_key") == "value"


def test_superuser_can_access_all_views(admin_client, test_cache):
    assert admin_client.get(_cache_list_url()).status_code == 200
    assert admin_client.get(_cache_detail_url("default")).status_code == 200
    assert admin_client.get(_key_list_url("default")).status_code == 200
    assert admin_client.get(_key_add_url("default")).status_code == 200


def test_flush_action_is_refused_without_allow_flush(admin_client: Client, test_cache: RespCache):
    test_cache.set("survivor", "value")

    response = admin_client.post(_cache_list_url(), {"action": "flush_selected", "_selected_action": ["default"]})

    assert response.status_code == 200
    assert test_cache.get("survivor") == "value"


def test_key_list_has_no_clear_tool_without_allow_flush(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_key_list_url("default"))

    assert response.status_code == 200
    assert 'value="clear_cache"' not in response.content.decode()


def test_clear_is_refused_without_allow_flush(admin_client: Client, test_cache: RespCache):
    test_cache.set("survivor", "value")

    response = admin_client.post(_key_list_url("default"), {"action": "clear_cache", "cache_name": "default"})

    assert response.status_code == 403
    assert test_cache.get("survivor") == "value"


def test_danger_zone_hidden_without_allow_flush(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    assert 'name="action" value="flush_db"' not in response.content.decode()


@pytest.mark.parametrize("action", ["flush_db", "clear_all_versions"])
def test_cache_detail_actions_need_allow_flush(admin_client: Client, test_cache: RespCache, action: str):
    test_cache.set("survivor", "value")

    response = admin_client.post(_cache_detail_url("default"), {"action": action})

    assert response.status_code == 403
    assert test_cache.get("survivor") == "value"


# clear_cache and the danger-zone actions have the same blast radius, so both need change_cache, not change_key.
@pytest.mark.usefixtures("_allow_flush")
def test_change_key_only_cannot_clear_cache(db, test_cache):
    """Regression: ``change_key`` alone must NOT permit ``clear_cache``."""
    staff_user = User.objects.create_user(
        username="staff_change_key_only",
        password="password",  # noqa: S106
        is_staff=True,
    )
    for codename in ("change_key", "access_default"):
        staff_user.user_permissions.add(
            Permission.objects.get(codename=codename, content_type__app_label="django_cachex"),
        )
    staff_user = User.objects.get(pk=staff_user.pk)

    client = Client()
    client.force_login(staff_user)

    test_cache.set("preserved_key", "value")

    response = client.post(
        _key_list_url("default"),
        {"action": "clear_cache", "cache_name": "default"},
    )

    assert response.status_code == 403
    assert test_cache.get("preserved_key") == "value"


@pytest.mark.usefixtures("_allow_flush")
def test_change_cache_permits_clear_cache(db, test_cache):
    """Sanity check: ``change_cache`` is the right gate."""
    staff_user = User.objects.create_user(
        username="staff_change_cache",
        password="password",  # noqa: S106
        is_staff=True,
    )
    for codename in ("view_key", "change_cache", "access_default"):
        perm = Permission.objects.get(
            codename=codename,
            content_type__app_label="django_cachex",
        )
        staff_user.user_permissions.add(perm)
    staff_user = User.objects.get(pk=staff_user.pk)

    client = Client()
    client.force_login(staff_user)

    test_cache.set("doomed_key", "value")

    response = client.post(
        _key_list_url("default"),
        {"action": "clear_cache", "cache_name": "default"},
    )

    assert response.status_code == 302
    assert test_cache.get("doomed_key") is None


@pytest.mark.usefixtures("_allow_flush")
@pytest.mark.parametrize(
    ("backend", "confirm_word", "message_word"),
    [
        pytest.param("django_cachex.cache.LocMemCache", "every version", "all versions", id="locmem"),
        pytest.param("django_cachex.cache.DatabaseCache", "database table", "whole table", id="database"),
    ],
)
def test_clear_says_what_the_backend_removes(
    admin_client: Client,
    test_cache,
    backend: str,
    confirm_word: str,
    message_word: str,
):
    call_command("createcachetable", "admin_test_clear")
    with _extra_cache("cleared", {"BACKEND": backend, "LOCATION": "admin_test_clear"}):
        caches["cleared"].set("other:version", "value", version=2)

        page = admin_client.get(_key_list_url("cleared"))
        response = admin_client.post(
            _key_list_url("cleared"),
            {"action": "clear_cache", "cache_name": "cleared"},
            follow=True,
        )

        assert caches["cleared"].get("other:version", version=2) is None
    clear_link = BeautifulSoup(page.content, "html.parser").select_one("#clear-cache-form a")
    assert clear_link is not None
    assert confirm_word in clear_link["onclick"]
    assert response.status_code == 200
    assert message_word in response.content.decode()


# A user without add_key must not see the add form, fill it in, and only then hit PermissionDenied on submit.
def test_staff_without_add_perm_denied_on_get(db, test_cache):
    staff_user = User.objects.create_user(
        username="staff_view_key_only",
        password="password",  # noqa: S106
        is_staff=True,
    )
    for codename in ("view_key", "access_default"):
        staff_user.user_permissions.add(
            Permission.objects.get(codename=codename, content_type__app_label="django_cachex"),
        )
    staff_user = User.objects.get(pk=staff_user.pk)

    client = Client()
    client.force_login(staff_user)

    response = client.get(_key_add_url("default"))
    assert response.status_code == 403


# Cluster SCAN cursors are per-node dicts and the adapter raises NotSupportedError, so the listing short-circuits.
def test_cluster_cache_renders_empty_with_info_message(rf, mocker, test_cache):
    del test_cache  # only needed for the CACHES override side effect

    from django.contrib.messages.storage.base import BaseStorage

    from django_cachex.cache.resp import RespClusterCache

    fake_cluster = mocker.MagicMock(spec=RespClusterCache)
    mocker.patch("django_cachex.admin.queryset.get_cache", return_value=fake_cluster)

    # Message storage that works without the session middleware.
    class _InMemoryStorage(BaseStorage):
        def __init__(self, request):
            super().__init__(request)
            self._queued: list = []

        def _get(self, *args, **kwargs):
            return self._queued, True

        def _store(self, messages, response, *args, **kwargs):
            self._queued.extend(messages)
            return []

        def add(self, level, message, extra_tags=""):
            from django.contrib.messages.storage.base import Message

            self._queued.append(Message(level, message, extra_tags=extra_tags))

    key_admin = site._registry[Key]

    request = rf.get(_key_list_url("default"))
    request.user = User(is_superuser=True, is_staff=True)
    request._messages = _InMemoryStorage(request)
    request._cachex_cursor = 0
    request._cachex_count = 100

    qs = key_admin.get_queryset(request)

    assert len(qs) == 0
    fake_cluster.scan.assert_not_called()

    msgs = [str(m.message) for m in request._messages._queued]
    assert any("cluster" in m.lower() for m in msgs), msgs


BROKEN_KEY = "broken:undecodable"


def _inject_broken_value(test_cache: RespCache) -> None:
    """Write garbage bytes directly to Redis under the cache's key prefix.

    Bypasses the cache's serializer/compressor pipeline so reading via
    cache.get() will raise SerializerError.
    """
    client = test_cache.get_client(write=True)
    full_key = test_cache.make_key(BROKEN_KEY)
    # Bytes that aren't valid pickle and aren't valid for any compressor
    # in the default chain. The client's _decompress / _deserialize must
    # raise on read.
    client.set(full_key, b"\x00not-valid-pickle-or-compressed-data")


# A compressor or serializer change can leave undecodable bytes; an operator must still open and delete the key.
def test_key_detail_loads_for_broken_value(admin_client, test_cache):
    """Key detail view returns 200 (not 500) for an undecodable value."""
    _inject_broken_value(test_cache)

    response = admin_client.get(_key_detail_url("default", BROKEN_KEY))

    assert response.status_code == 200
    content = response.content.decode()
    assert "cannot be decoded" in content


def test_key_detail_shows_warning_message_for_broken_value(admin_client, test_cache):
    _inject_broken_value(test_cache)

    response = admin_client.get(_key_detail_url("default", BROKEN_KEY))

    # Django messages framework renders into the page.
    content = response.content.decode()
    assert "different compressor or serializer" in content


def test_delete_works_for_broken_value(admin_client, test_cache):
    _inject_broken_value(test_cache)

    client = test_cache.get_client(write=True)
    full_key = test_cache.make_key(BROKEN_KEY)
    assert client.exists(full_key) == 1

    response = admin_client.post(
        _key_detail_url("default", BROKEN_KEY),
        {"action": "delete"},
    )

    # 302 redirect to key list = success (matches the success branch in key_detail.py).
    assert response.status_code == 302
    assert client.exists(full_key) == 0


def test_key_list_renders_with_broken_value(admin_client, test_cache):
    """The key list shouldn't crash either; size column degrades gracefully."""
    _inject_broken_value(test_cache)

    response = admin_client.get(_key_list_url("default"))

    assert response.status_code == 200
    # Broken key still appears in the list; operator needs to see it to delete it.
    assert BROKEN_KEY in response.content.decode()


def test_key_list_keeps_other_keys_next_to_an_undecodable_name(admin_client: Client, test_cache: RespCache):
    test_cache.set("plain:key", "value")
    test_cache.get_client(write=True).set(test_cache.make_key("bad:").encode() + b"\xff", b"raw")

    response = admin_client.get(_key_list_url("default"))

    assert response.status_code == 200
    assert set(_result_column(response.content, "key_name")) == {"plain:key", "bad:\\xff"}


def test_oversized_string_is_neither_read_nor_rendered(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("big:string", "x" * (_MAX_STRING_BYTES + 1))
    read = mocker.patch.object(test_cache, "eval_script", wraps=test_cache.eval_script)

    response = admin_client.get(_key_detail_url("default", "big:string"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "too large" in content
    assert "x" * 100 not in content
    read.assert_not_called()
    assert 'name="ttl_value"' in content
    assert 'id="delete-form"' in content


def _assert_breadcrumbs(content: str, *, trail: list[str]) -> None:
    if django.VERSION >= (6, 1):
        open_tag, close_tag = '<ol class="breadcrumbs">', "</ol>"
        assert '<div class="breadcrumbs">' not in content
    else:
        open_tag, close_tag = '<div class="breadcrumbs">', "</div>"
        assert '<ol class="breadcrumbs">' not in content
    assert open_tag in content
    crumbs = content.split(open_tag)[1].split(close_tag, maxsplit=1)[0]
    for label in trail:
        assert label in crumbs
    if django.VERSION >= (6, 1):
        assert crumbs.count("<li") == len(trail)
        assert 'aria-current="page"' in crumbs
    else:
        assert crumbs.count("<a ") == len(trail) - 1
        assert crumbs.count("&rsaquo;") == len(trail) - 1


# Django 6.1 styles ol.breadcrumbs and 6.0 styles div.breadcrumbs; the wrong one renders completely unstyled.
def test_breadcrumbs_match_a_stock_admin_page(admin_client: Client, test_cache):
    """The trail must use the element the running admin's own pages emit."""
    response = admin_client.get(reverse("admin:auth_user_changelist"))
    assert response.status_code == 200
    stock = response.content.decode()
    expected = '<ol class="breadcrumbs">' if django.VERSION >= (6, 1) else '<div class="breadcrumbs">'
    assert expected in stock


def test_key_list_breadcrumbs(admin_client: Client, test_cache):
    response = admin_client.get(_key_list_url("default"))
    assert response.status_code == 200
    _assert_breadcrumbs(
        response.content.decode(),
        trail=["Home", "Caches", "default", "Keys"],
    )


def test_cache_detail_breadcrumbs(admin_client: Client, test_cache):
    response = admin_client.get(_cache_detail_url("default"))
    assert response.status_code == 200
    _assert_breadcrumbs(
        response.content.decode(),
        trail=["Home", "Caches", "default"],
    )


def test_key_detail_breadcrumbs(admin_client: Client, test_cache: RespCache):
    test_cache.set("crumb-key", "value")
    response = admin_client.get(_key_detail_url("default", "crumb-key"))
    assert response.status_code == 200
    _assert_breadcrumbs(
        response.content.decode(),
        trail=["Home", "Caches", "default", "crumb-key"],
    )


def test_key_add_breadcrumbs(admin_client: Client, test_cache):
    response = admin_client.get(_key_add_url("default"))
    assert response.status_code == 200
    _assert_breadcrumbs(
        response.content.decode(),
        trail=["Home", "Caches", "default", "Add Key"],
    )


def _assert_tools(content: str, *, labels: list[str]) -> None:
    assert content.count('<ul class="object-tools">') == 1
    start = content.index('<ul class="object-tools">')
    tools = content[start:].split("</ul>", maxsplit=1)[0]
    for label in labels:
        assert f">{label}</a>" in tools
    wrapper = '<div class="titles-and-tools">' if django.VERSION >= (6, 1) else '<div id="content"'
    assert content.index(wrapper) < start


# Django 6.0 nests object-tools inside content, which our own content block replaces, so both blocks carry the links.
def test_tools_match_a_stock_admin_page(admin_client: Client, admin_user, test_cache):
    """The links must sit in the slot the running admin's own pages use."""
    response = admin_client.get(reverse("admin:auth_user_change", args=[admin_user.pk]))
    assert response.status_code == 200
    _assert_tools(response.content.decode(), labels=["History"])


def test_cache_detail_tools(admin_client: Client, test_cache):
    response = admin_client.get(_cache_detail_url("default"))
    assert response.status_code == 200
    _assert_tools(response.content.decode(), labels=["List Keys", "Help"])


def test_key_detail_tools(admin_client: Client, test_cache: RespCache):
    test_cache.set("tools-key", "value")
    response = admin_client.get(_key_detail_url("default", "tools-key"))
    assert response.status_code == 200
    _assert_tools(response.content.decode(), labels=["Help"])


def test_key_add_tools(admin_client: Client, test_cache):
    response = admin_client.get(_key_add_url("default"))
    assert response.status_code == 200
    _assert_tools(response.content.decode(), labels=["Help"])


def _access_permission(alias: str) -> Permission:
    """Return the ``access_<alias>`` permission, creating it for an alias only a test adds."""
    permission, _created = Permission.objects.get_or_create(
        codename=f"access_{alias}",
        content_type=ContentType.objects.get_for_model(Cache),
        defaults={"name": f"Can access keys in cache '{alias}'"},
    )
    return permission


def _staff_client(perms: list[str], *, aliases: tuple[str, ...] = ("default",)) -> Client:
    """Log in a staff user holding exactly ``perms`` on ``django_cachex`` and access to ``aliases``."""
    user = User.objects.create_user(
        username="staff_" + "_".join([*perms, *aliases]),
        password="password",  # noqa: S106
        is_staff=True,
    )
    for codename in perms:
        user.user_permissions.add(
            Permission.objects.get(codename=codename, content_type__app_label="django_cachex"),
        )
    for alias in aliases:
        user.user_permissions.add(_access_permission(alias))
    user = User.objects.get(pk=user.pk)
    client = Client()
    client.force_login(user)
    return client


# Numbers that travel back to the server must render unlocalized: 1,5 or 1,000 breaks float()/int() on the POST.
def test_zset_scores_render_unlocalized(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("l10n:zset", {"member-a": 1.5})

    with translation.override("de"):
        response = admin_client.get(_key_detail_url("default", "l10n:zset"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'value="1.5"' in content
    assert 'value="1,5"' not in content


def test_zset_score_edit_round_trips_under_comma_locale(
    admin_client: Client,
    test_cache: RespCache,
):
    """The CAS check compares the posted ``original_score`` with the stored
    one, so a localized score silently fails every edit.
    """
    test_cache.zadd("l10n:zset:edit", {"member-a": 1.5})
    url = _key_detail_url("default", "l10n:zset:edit")

    with translation.override("de"):
        content = admin_client.get(url).content.decode()
        match = re.search(r'name="original_score" value="([^"]+)"', content)
        assert match is not None
        admin_client.post(
            url,
            {
                "action": "zupdate",
                "member": "member-a",
                "new_member": "member-a",
                "original_score": match.group(1),
                "score_value": "2.5",
            },
        )

    assert test_cache.zscore("l10n:zset:edit", "member-a") == 2.5


@override_settings(USE_THOUSAND_SEPARATOR=True)
def test_ttl_input_renders_unlocalized(admin_client: Client, test_cache: RespCache):
    test_cache.set("l10n:ttl", "value", timeout=3600)

    response = admin_client.get(_key_detail_url("default", "l10n:ttl"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="ttl_value"' in content
    assert 'value="3,600"' not in content
    assert re.search(r'name="ttl_value"[^>]*value="3[56]\d\d"', content)


@override_settings(USE_THOUSAND_SEPARATOR=True)
def test_list_row_index_renders_unlocalized(admin_client: Client, test_cache: RespCache):
    """The hidden ``index`` field feeds LSET, which needs a plain integer."""
    test_cache.rpush("l10n:list", *[f"item{i}" for i in range(1001)])

    response = admin_client.get(_key_detail_url("default", "l10n:list") + "?page=11")

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="index" value="1000"' in content
    assert 'name="index" value="1,000"' not in content


@override_settings(USE_THOUSAND_SEPARATOR=True)
def test_key_list_cursor_and_count_render_unlocalized(
    admin_client: Client,
    mocker,
    test_cache: RespCache,
):
    """``cursor`` and ``count`` go back into SCAN as integers."""
    fake = mocker.MagicMock()
    fake.scan.return_value = (123456, [])
    mocker.patch("django_cachex.admin.queryset.get_cache", return_value=fake)

    response = admin_client.get(_key_list_url("default") + "&count=1000")

    assert response.status_code == 200
    content = response.content.decode()
    assert "count=1000" in content
    assert "count=1,000" not in content
    assert "cursor=123456" in content
    assert "cursor=123,456" not in content


CREATE_KEY = "perm:create:key"


# Materializing a missing key is an add, so change_key alone must not get around the add_key gate.
def test_create_mode_get_denied_without_add_perm(db, test_cache: RespCache):
    client = _staff_client(["view_key", "change_key"])

    response = client.get(_key_detail_create_url("default", CREATE_KEY, "hash"))

    assert response.status_code == 302
    assert response.url == _key_list_url("default")


def test_create_mode_get_allowed_with_add_perm(db, test_cache: RespCache):
    client = _staff_client(["view_key", "change_key", "add_key"])

    response = client.get(_key_detail_create_url("default", CREATE_KEY, "hash"))

    assert response.status_code == 200


def test_materializing_post_denied_without_add_perm(db, test_cache: RespCache):
    client = _staff_client(["view_key", "change_key"])

    response = client.post(
        _key_detail_url("default", CREATE_KEY),
        {"action": "hset", "field": "f", "field_value": "v"},
    )

    assert response.status_code == 403
    assert not test_cache.has_key(CREATE_KEY)


def test_materializing_post_allowed_with_add_perm(db, test_cache: RespCache):
    client = _staff_client(["view_key", "change_key", "add_key"])

    response = client.post(
        _key_detail_url("default", CREATE_KEY),
        {"action": "hset", "field": "f", "field_value": "v"},
    )

    assert response.status_code == 302
    assert test_cache.hget(CREATE_KEY, "f") == "v"


def test_existing_key_still_editable_without_add_perm(db, test_cache: RespCache):
    """``change_key`` alone must keep working on keys that already exist."""
    test_cache.hset("perm:existing:key", "f", "old")
    client = _staff_client(["view_key", "change_key"])

    response = client.post(
        _key_detail_url("default", "perm:existing:key"),
        {"action": "hset", "field": "f", "field_value": "new"},
    )

    assert response.status_code == 302
    assert test_cache.hget("perm:existing:key", "f") == "new"


def test_add_perm_alone_offers_only_the_creating_operations(db, test_cache: RespCache):
    client = _staff_client(["view_key", "add_key"])

    response = client.get(_key_detail_create_url("default", CREATE_KEY, "list"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'value="lpush"' in content
    assert 'value="lpop"' not in content
    assert 'name="ttl_value"' not in content


def test_add_perm_alone_creates_a_missing_key(db, test_cache: RespCache):
    client = _staff_client(["view_key", "add_key"])

    response = client.post(
        _key_detail_url("default", CREATE_KEY),
        {"action": "hset", "field": "f", "field_value": "v"},
    )

    assert response.status_code == 302
    assert test_cache.hget(CREATE_KEY, "f") == "v"


def test_add_perm_alone_cannot_edit_an_existing_key(db, test_cache: RespCache):
    test_cache.hset("perm:existing:key", "f", "old")
    client = _staff_client(["view_key", "add_key"])

    response = client.post(
        _key_detail_url("default", "perm:existing:key"),
        {"action": "hset", "field": "f", "field_value": "new"},
    )

    assert response.status_code == 403
    assert test_cache.hget("perm:existing:key", "f") == "old"


def test_delete_button_hidden_in_create_mode(admin_client: Client, test_cache: RespCache):
    """The Delete link submits ``#delete-form``, which only exists once the
    key does. Rendering it in create mode gives a JS TypeError.
    """
    response = admin_client.get(_key_detail_create_url("default", "nodelete:key", "hash"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'id="delete-form"' not in content
    assert 'class="deletelink"' not in content


def test_delete_button_shown_for_existing_key(admin_client: Client, test_cache: RespCache):
    test_cache.set("hasdelete:key", "value")

    response = admin_client.get(_key_detail_url("default", "hasdelete:key"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'id="delete-form"' in content
    assert 'class="deletelink"' in content


def test_unexpected_read_error_still_renders(
    admin_client: Client,
    mocker,
    test_cache: RespCache,
):
    """Only decode errors used to be caught, so any other read failure
    turned the detail page into a 500 with no way to delete the key.
    """
    test_cache.set("unreadable:key", "value")
    mocker.patch.object(test_cache, "eval_script", side_effect=RuntimeError("boom"))
    mocker.patch.object(test_cache, "get", side_effect=RuntimeError("boom"))

    response = admin_client.get(_key_detail_url("default", "unreadable:key"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "could not be read" in content


def test_type_filter_drops_rows_the_backend_did_not_filter(
    admin_client: Client,
    mocker,
    test_cache: RespCache,
):
    """``key_type`` is only a hint to ``scan()``. A backend that ignores it
    used to make the Type filter silently list every key.
    """
    fake = mocker.MagicMock()
    fake.scan.return_value = (0, ["typefilter:str", "typefilter:lst"])
    fake.type.side_effect = lambda k: "string" if k.endswith("str") else "list"
    fake.ttl.return_value = None
    fake.pipeline.side_effect = NotSupportedError("pipeline", "fake")
    mocker.patch("django_cachex.admin.queryset.get_cache", return_value=fake)
    mocker.patch("django_cachex.admin.queryset.get_size", return_value=10)

    response = admin_client.get(_key_list_url("default") + "&type=string")

    assert response.status_code == 200
    content = response.content.decode()
    assert "typefilter:str" in content
    assert "typefilter:lst" not in content


# get_urls registers history/delete routes that start with get_queryset().get(), which the fake querysets lack.
@pytest.mark.parametrize(
    ("url_name", "arg"),
    [
        ("admin:django_cachex_cache_history", "default"),
        ("admin:django_cachex_cache_delete", "default"),
        ("admin:django_cachex_key_history", "default::somekey"),
        ("admin:django_cachex_key_delete", "default::somekey"),
    ],
)
def test_per_object_routes_404(admin_client: Client, test_cache, url_name, arg):
    response = admin_client.get(reverse(url_name, args=[quote(arg)]))
    assert response.status_code == 404


@pytest.mark.parametrize("ttl_value", ["0", "00", "+0", "-0"])
def test_zero_ttl_spellings_persist_instead_of_deleting(
    admin_client: Client,
    test_cache: RespCache,
    ttl_value: str,
):
    """``expire(key, 0)`` deletes the key. Only the string ``"0"`` used to be
    recognised as "no expiry", so ``"00"`` reported success and dropped the key.
    """
    test_cache.set("ttl:zero", "value", timeout=300)

    response = admin_client.post(
        _key_detail_url("default", "ttl:zero"),
        {"action": "set_ttl", "ttl_value": ttl_value},
    )

    assert response.status_code == 302
    assert test_cache.get("ttl:zero") == "value"
    assert test_cache.ttl("ttl:zero") in (None, -1)


def test_negative_ttl_rejected(admin_client: Client, test_cache: RespCache):
    test_cache.set("ttl:negative", "value", timeout=300)

    response = admin_client.post(
        _key_detail_url("default", "ttl:negative"),
        {"action": "set_ttl", "ttl_value": "-5"},
        follow=True,
    )

    content = response.content.decode()
    assert "TTL must be non-negative." in content
    assert test_cache.get("ttl:negative") == "value"


def test_zadd_flag_noop_reported_as_no_change(admin_client: Client, test_cache: RespCache):
    """ZADD returns 0 both for "existing member, score rewritten" and for
    "flags blocked the write". Asking for CH makes the two distinguishable.
    """
    test_cache.zadd("zadd:noop", {"a": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "zadd:noop"),
        {"action": "zadd", "member": "a", "score_value": "99.0", "zadd_nx": "on"},
        follow=True,
    )

    content = response.content.decode()
    assert "No change" in content
    assert test_cache.zscore("zadd:noop", "a") == 1.0


def test_zadd_flagged_write_reported_as_success(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("zadd:applied", {"a": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "zadd:applied"),
        {"action": "zadd", "member": "a", "score_value": "99.0", "zadd_xx": "on"},
        follow=True,
    )

    content = response.content.decode()
    assert "No change" not in content
    assert test_cache.zscore("zadd:applied", "a") == 99.0


def test_zadd_unhashable_member_rejected(admin_client: Client, test_cache: RespCache):
    """ZADD passes members as dict keys, so a JSON array would raise a bare
    ``TypeError: unhashable type``.
    """
    response = admin_client.post(
        _key_detail_url("default", "zadd:unhashable"),
        {"action": "zadd", "member": "[1, 2]", "score_value": "1.0"},
        follow=True,
    )

    content = response.content.decode()
    assert "cannot be used as sorted set members" in content


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        ("srem", "Member is required."),
        ("zrem", "Member is required."),
        ("hdel", "Field name is required."),
    ],
)
def test_blank_input_reports_an_error(
    admin_client: Client,
    test_cache: RespCache,
    action: str,
    expected: str,
):
    """A blank field used to redirect with no message at all, which reads as
    a successful no-op.
    """
    if action == "srem":
        test_cache.sadd("blank:set", "a")
    elif action == "zrem":
        test_cache.zadd("blank:set", {"a": 1.0})
    else:
        test_cache.hset("blank:set", "a", "1")

    response = admin_client.post(
        _key_detail_url("default", "blank:set"),
        {"action": action, "member": "", "field": ""},
        follow=True,
    )

    assert expected in response.content.decode()


def _help_href(content: str) -> str:
    match = re.search(r'<a href="([^"]*)">Help</a>', content)
    assert match is not None
    return match.group(1)


def test_help_link_keeps_page(admin_client: Client, test_cache: RespCache):
    test_cache.rpush("help:list", "a", "b")

    content = admin_client.get(_key_detail_url("default", "help:list") + "?page=1").content.decode()

    href = _help_href(content)
    assert "page=1" in href
    assert "help=1" in href


def test_help_link_keeps_type_in_create_mode(admin_client: Client, test_cache: RespCache):
    """Dropping ``type`` points Help at a key that does not exist, which
    bounces straight back to the key list.
    """
    content = admin_client.get(_key_detail_create_url("default", "help:create", "hash")).content.decode()

    href = _help_href(content)
    assert "type=hash" in href
    assert "help=1" in href


def test_unknown_create_type_is_rejected(admin_client: Client, test_cache: RespCache):
    """``create_type`` is interpolated into badge classes and the help key,
    so an arbitrary value renders an undefined-state page.
    """
    response = admin_client.get(
        _key_detail_create_url("default", "bogus:type:key", "notatype"),
        follow=True,
    )

    assert "Unknown key type" in response.content.decode()
    assert response.redirect_chain[0][1] == 302


# LocMem cannot run the CAS scripts, so score edits fall back to ZADD.
def test_zset_page_on_locmem_omits_the_score_fingerprint(admin_client: Client, test_cache: RespCache):
    local = caches["local"]
    local.zadd("zset:locmem:render", {"member-a": 1.0})

    response = admin_client.get(_key_detail_url("local", "zset:locmem:render"))

    assert response.status_code == 200
    assert 'name="original_score"' not in response.content.decode()


def test_zset_score_edit_on_locmem_succeeds(admin_client: Client, test_cache: RespCache):
    local = caches["local"]
    local.zadd("zset:locmem:edit", {"member-a": 1.0})

    response = admin_client.post(
        _key_detail_url("local", "zset:locmem:edit"),
        {
            "action": "zupdate",
            "member": '"member-a"',
            "new_member": '"member-a"',
            "score_value": "9.5",
        },
        follow=True,
    )

    assert response.status_code == 200
    assert local.zscore("zset:locmem:edit", "member-a") == 9.5
    assert "Updated &#x27;member-a&#x27; score to 9.5." in response.content.decode()


def test_score_edit_succeeds_with_conflict_detection(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("zset:resp:edit", {"member-a": 1.0})
    page = admin_client.get(_key_detail_url("default", "zset:resp:edit"))
    assert 'name="original_score" value="1.0"' in page.content.decode()

    response = admin_client.post(
        _key_detail_url("default", "zset:resp:edit"),
        {
            "action": "zupdate",
            "member": '"member-a"',
            "new_member": '"member-a"',
            "score_value": "9.5",
            "original_score": "1.0",
        },
        follow=True,
    )

    assert response.status_code == 200
    assert test_cache.zscore("zset:resp:edit", "member-a") == 9.5


def test_stream_whose_entries_were_all_deleted_renders_as_empty(
    admin_client: Client,
    test_cache: RespCache,
):
    entry_id = test_cache.xadd("stream:drained", {"field": "value"})
    test_cache.xdel("stream:drained", entry_id)
    assert test_cache.xlen("stream:drained") == 0

    response = admin_client.get(_key_detail_url("default", "stream:drained"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Stream is empty." in content
    assert "Could not load value" not in content


@pytest.fixture(params=["unimportable", "rejected_options"])
def broken(request: pytest.FixtureRequest, test_cache: RespCache) -> str:
    if request.param == "unimportable":
        backend = {"BACKEND": "django_cachex.cache.NoSuchBackend", "LOCATION": _SECRET_LOCATION}
    else:
        backend = {
            "BACKEND": "django_cachex.cache.ValkeyCache",
            "LOCATION": _SECRET_LOCATION,
            "OPTIONS": {"decode_responses": True},
        }
    with override_settings(CACHES={**settings.CACHES, "broken": backend}):
        yield "broken"


# A raising constructor used to escape Cache._get_cache and 500 the changelist every broken alias redirects to.
def test_unbuildable_backend_cache_list_still_renders(admin_client: Client, broken: str):
    response = admin_client.get(_cache_list_url())

    assert response.status_code == 200
    row = _table_containing(response.content, broken).get_text()
    assert broken in row
    assert "could not be loaded" in row
    assert "redis://cachexuser:***@cache.example.test:6379/1" in row
    assert "s3cr3t-pw" not in response.content.decode()


def test_unbuildable_backend_model_falls_back_to_the_settings_location(broken: str):
    cache = Cache.get_by_name(broken)

    assert cache is not None
    assert cache.location == "redis://cachexuser:***@cache.example.test:6379/1"
    assert cache.support_level == "limited"


def test_unbuildable_backend_cache_detail_redirects_with_a_message(admin_client: Client, broken: str):
    response = admin_client.get(_cache_detail_url(broken), follow=True)

    assert response.status_code == 200
    assert response.redirect_chain[-1][0] == _cache_list_url()
    assert "could not be loaded" in response.content.decode()


def test_unbuildable_backend_key_list_renders_empty_with_a_message(admin_client: Client, broken: str):
    response = admin_client.get(_key_list_url(broken))

    assert response.status_code == 200
    assert "could not be loaded" in response.content.decode()


def test_unbuildable_backend_key_detail_redirects_with_a_message(admin_client: Client, broken: str):
    response = admin_client.get(_key_detail_url(broken, "any:key"), follow=True)

    assert response.status_code == 200
    assert response.redirect_chain[-1][0] == _cache_list_url()
    assert "could not be loaded" in response.content.decode()


def test_unbuildable_backend_key_add_redirects_with_a_message(admin_client: Client, broken: str):
    response = admin_client.get(_key_add_url(broken), follow=True)

    assert response.status_code == 200
    assert response.redirect_chain[-1][0] == _cache_list_url()
    assert "could not be loaded" in response.content.decode()


@pytest.mark.usefixtures("_allow_flush")
def test_danger_zone_hidden_for_a_backend_without_the_operations(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_cache_detail_url("local"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Danger Zone" not in content
    assert 'name="action" value="flush_db"' not in content


@pytest.mark.usefixtures("_allow_flush")
def test_danger_zone_shown_for_a_resp_backend(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Danger Zone" in content
    assert 'name="action" value="flush_db"' in content


@pytest.mark.usefixtures("_allow_flush")
def test_danger_zone_hand_crafted_post_is_refused(admin_client: Client, test_cache: RespCache):
    response = admin_client.post(_cache_detail_url("local"), {"action": "flush_db"}, follow=True)

    assert response.status_code == 200
    assert "Destructive operations are not supported by this cache backend." in response.content.decode()


def test_failed_first_operation_stays_in_create_mode(admin_client: Client, test_cache: RespCache):
    url = _key_detail_create_url("default", "create:error:list", "list")

    response = admin_client.post(url, {"action": "rpush", "value": ""})

    assert response.status_code == 302
    assert response["Location"].endswith("?type=list")


def test_failed_first_operation_redirect_still_offers_the_create_page(
    admin_client: Client,
    test_cache: RespCache,
):
    response = admin_client.post(
        _key_detail_create_url("default", "create:error:hash", "hash"),
        {"action": "hset", "field": "", "field_value": ""},
        follow=True,
    )

    assert response.status_code == 200
    content = response.content.decode()
    assert "This key does not exist yet" in content
    assert "does not exist in cache" not in content


def test_unknown_type_renders_read_only(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("opaque:key", "unmistakable-payload")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)

    response = admin_client.get(_key_detail_url("default", "opaque:key"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "which the cache admin cannot display or edit" in content
    assert "unmistakable-payload" not in content
    assert 'name="action" value="update"' not in content


def test_unknown_type_keeps_the_delete_control(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("opaque:deletable", "value")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)

    response = admin_client.get(_key_detail_url("default", "opaque:deletable"))

    assert 'name="action" value="delete"' in response.content.decode()


def test_unknown_type_rejects_a_hand_crafted_update(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("opaque:posted", "unmistakable-payload")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)

    response = admin_client.post(
        _key_detail_url("default", "opaque:posted"),
        {"action": "update", "value": "overwritten"},
        follow=True,
    )

    assert response.status_code == 200
    assert "cannot edit" in response.content.decode()
    assert test_cache.get("opaque:posted") == "unmistakable-payload"


def test_unknown_type_still_deletes(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("opaque:doomed", "value")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)

    response = admin_client.post(_key_detail_url("default", "opaque:doomed"), {"action": "delete"}, follow=True)

    assert response.status_code == 200
    assert test_cache.get("opaque:doomed") is None


def test_add_form_offers_no_unknown_type(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_key_add_url("default"))

    assert response.status_code == 200
    content = response.content.decode()
    assert '<option value="string"' in content
    assert '<option value="stream"' in content
    assert f'<option value="{KeyType.UNKNOWN.value}"' not in content


def test_unknown_type_is_rejected_in_create_mode(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(
        _key_detail_create_url("default", "opaque:create", KeyType.UNKNOWN.value),
        follow=True,
    )

    assert "Unknown key type" in response.content.decode()


def test_add_form_offers_no_stream_on_a_backend_without_streams(admin_client: Client, test_cache: RespCache):
    """LocMem and Database have no xadd(); offering "Stream" led to an AttributeError message."""
    response = admin_client.get(_key_add_url("local"))

    assert response.status_code == 200
    content = response.content.decode()
    assert '<option value="zset"' in content
    assert '<option value="stream"' not in content


def test_stream_type_is_rejected_on_a_backend_without_streams(admin_client: Client, test_cache: RespCache):
    add_response = admin_client.post(_key_add_url("local"), {"key": "nostream:add", "type": "stream"})
    assert add_response.status_code == 200
    assert "does not support stream keys" in add_response.content.decode()

    detail_response = admin_client.get(_key_detail_create_url("local", "nostream:create", "stream"), follow=True)
    content = detail_response.content.decode()
    assert "does not support stream keys" in content
    assert "does not exist yet" not in content


def test_string_key_page_has_no_mutation_controls_for_a_viewer(db, test_cache: RespCache):
    test_cache.set("viewonly:string", "value")
    client = _staff_client(["view_key"])

    response = client.get(_key_detail_url("default", "viewonly:string"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "<textarea" in content
    assert ">Update</button>" not in content
    assert 'name="ttl_value"' not in content
    assert 'id="delete-form"' not in content


def test_list_key_page_has_no_mutation_controls_for_a_viewer(db, test_cache: RespCache):
    test_cache.rpush("viewonly:list", "a")
    client = _staff_client(["view_key"])

    response = client.get(_key_detail_url("default", "viewonly:list"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Operations" not in content
    assert 'name="action" value="rpush"' not in content
    assert 'name="action" value="lrem"' not in content


def test_change_permission_restores_the_mutation_controls(db, test_cache: RespCache):
    test_cache.set("viewonly:changeable", "value")
    client = _staff_client(["view_key", "change_key"])

    response = client.get(_key_detail_url("default", "viewonly:changeable"))

    assert response.status_code == 200
    content = response.content.decode()
    assert ">Update</button>" in content
    assert 'name="ttl_value"' in content


_SECRET_LOCATION = "redis://cachexuser:s3cr3t-pw@cache.example.test:6379/1"


def _extra_cache(alias: str, config: dict) -> override_settings:
    """Add one more alias to the caches the running test has configured."""
    return override_settings(CACHES={**settings.CACHES, alias: config})


# view_cache is the weakest cache permission, and both cache pages render LOCATION, which can carry a password.
def test_cache_list_masks_the_password(db, test_cache):
    client = _staff_client(["view_cache"])

    with _extra_cache(
        "secret",
        {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": _SECRET_LOCATION},
    ):
        response = client.get(_cache_list_url())

    assert response.status_code == 200
    content = response.content.decode()
    assert "s3cr3t-pw" not in content
    assert "redis://cachexuser:***@cache.example.test:6379/1" in content


def test_cache_detail_masks_the_password(db, test_cache):
    client = _staff_client(["view_cache"])

    with _extra_cache(
        "secret",
        {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": _SECRET_LOCATION},
    ):
        response = client.get(_cache_detail_url("secret"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "s3cr3t-pw" not in content
    assert "redis://cachexuser:***@cache.example.test:6379/1" in content


def test_every_entry_of_a_location_list_is_masked(db, test_cache):
    client = _staff_client(["view_cache"])
    location = [
        "redis://cachexuser:s3cr3t-pw@replica-a.example.test:6379/1",
        "redis://cachexuser:s3cr3t-pw@replica-b.example.test:6379/1",
    ]
    # An unimportable backend keeps ``Cache.location`` on its settings
    # fallback, which is the branch that renders a sequence.
    with _extra_cache(
        "secret",
        {"BACKEND": "django_cachex.cache.NoSuchCache", "LOCATION": location},
    ):
        response = client.get(_cache_list_url())

    assert response.status_code == 200
    content = response.content.decode()
    assert "s3cr3t-pw" not in content
    assert "redis://cachexuser:***@replica-a.example.test:6379/1" in content
    assert "redis://cachexuser:***@replica-b.example.test:6379/1" in content


def test_a_unix_socket_location_is_left_alone(db, test_cache):
    client = _staff_client(["view_cache"])
    location = "unix:///var/run/redis/redis.sock?db=1"

    with _extra_cache(
        "secret",
        {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": location},
    ):
        response = client.get(_cache_detail_url("secret"))

    assert response.status_code == 200
    assert location in response.content.decode()


def test_a_connection_error_quoting_the_url_is_masked(db, test_cache, mocker):
    client = _staff_client(["view_cache"])
    mocker.patch(
        "django_cachex.admin.queryset.get_cache",
        side_effect=OSError(f"Error 111 connecting to {_SECRET_LOCATION}. Connection refused."),
    )

    response = client.get(_cache_list_url())

    assert response.status_code == 200
    content = response.content.decode()
    assert "s3cr3t-pw" not in content
    assert "***@cache.example.test" in content


def test_a_backend_that_fails_to_build_is_masked(db, test_cache, mocker):
    """Building the backend parses the URL, so the failure can quote it."""
    client = _staff_client(["view_cache"])

    def build(name: str):
        if name == "secret":
            msg = f"Sentinel URL {_SECRET_LOCATION} has no hostname (service name)."
            raise ImproperlyConfigured(msg)
        return caches[name]

    handler = mocker.patch("django_cachex.admin.helpers.caches")
    handler.__getitem__.side_effect = build

    with _extra_cache(
        "secret",
        {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": _SECRET_LOCATION},
    ):
        response = client.get(_cache_detail_url("secret"), follow=True)

    assert response.status_code == 200
    assert response.redirect_chain[-1][0] == _cache_list_url()
    content = response.content.decode()
    assert "s3cr3t-pw" not in content
    assert "could not be loaded: Sentinel URL redis://cachexuser:***@cache.example.test:6379/1" in content


def _stock_alias() -> override_settings:
    return _extra_cache(
        "stock",
        {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "admin-test-stock-locmem",
        },
    )


# Stock Django backends have no type(), so a type-specific write would have to guess the value's editor.
def test_stock_backend_cache_detail_renders(admin_client: Client, test_cache):
    """No ``info()`` and no slow log, so both sections drop out quietly."""
    with _stock_alias():
        response = admin_client.get(_cache_detail_url("stock"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Cache: stock" in content
    assert "Slow Log" not in content
    assert "could not be read" not in content


def test_stock_backend_key_page_still_renders(admin_client: Client, test_cache):
    with _stock_alias():
        caches["stock"].set("stock:readable", "stock-only-payload")

        response = admin_client.get(_key_detail_url("stock", "stock:readable"))

    assert response.status_code == 200
    assert "stock-only-payload" in response.content.decode()


def test_stock_backend_update_is_refused(admin_client: Client, test_cache):
    with _stock_alias():
        cache = caches["stock"]
        cache.set("stock:frozen", "original")

        response = admin_client.post(
            _key_detail_url("stock", "stock:frozen"),
            {"action": "update", "value": '"replacement"'},
            follow=True,
        )

        assert cache.get("stock:frozen") == "original"

    assert response.status_code == 200
    assert "does not report key types" in response.content.decode()


def test_stock_backend_delete_still_works(admin_client: Client, test_cache):
    with _stock_alias():
        cache = caches["stock"]
        cache.set("stock:doomed", "value")

        response = admin_client.post(
            _key_detail_url("stock", "stock:doomed"),
            {"action": "delete"},
            follow=True,
        )

        assert cache.get("stock:doomed") is None

    assert response.status_code == 200


def test_stock_backend_add_key_is_not_offered(admin_client: Client, test_cache):
    """No ``type()`` means no create action can run, so the link and the form both go."""
    with _stock_alias():
        listing = admin_client.get(_key_list_url("stock"))
        add_page = admin_client.get(_key_add_url("stock"), follow=True)
        create_page = admin_client.get(_key_detail_create_url("stock", "stock:new", "string"), follow=True)

    assert listing.status_code == 200
    assert "Add key" not in listing.content.decode()
    assert add_page.redirect_chain[-1][0].endswith("?cache=stock")
    assert "does not support adding keys from the admin" in add_page.content.decode()
    assert "does not support adding keys from the admin" in create_page.content.decode()


# get() returns None once XFetch fires; the admin must show the real value or the next update writes None back.
def test_value_survives_a_recompute_signal(admin_client: Client, test_cache):
    config = {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": settings.CACHES["default"]["LOCATION"],
        "OPTIONS": {"stampede_prevention": True},
    }
    with _extra_cache("stampede", config):
        cache = caches["stampede"]
        cache.set("stampede:key", "unmistakable-payload", timeout=300)
        # The default buffer is 60s, so a raw 50s TTL puts the logical
        # remaining lifetime below zero and every get() asks to recompute.
        cache.expire("stampede:key", 50, stampede_prevention=False)
        assert cache.get("stampede:key") is None

        response = admin_client.get(_key_detail_url("stampede", "stampede:key"))
        caches.close_all()

    assert response.status_code == 200
    content = response.content.decode()
    assert "unmistakable-payload" in content
    assert ">null</textarea>" not in content


@pytest.fixture
def buffered_key(test_cache: RespCache):
    config = {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": settings.CACHES["default"]["LOCATION"],
        "OPTIONS": {"stampede_prevention": True},
    }
    with _extra_cache("stampede", config):
        cache = caches["stampede"]
        cache.set("stampede:ttl", "value", timeout=300)
        cache.expire("stampede:ttl", 50, stampede_prevention=False)
        yield cache, _key_detail_url("stampede", "stampede:ttl")
        caches.close_all()


def _ttl_form_data(content: bytes) -> dict[str, str]:
    soup = BeautifulSoup(content, "html.parser")
    action = soup.find("input", attrs={"name": "action", "value": "set_ttl"})
    assert action is not None
    form = action.find_parent("form")
    assert form is not None
    return {
        field["name"]: field.get("value", "")
        for field in form.find_all("input", attrs={"name": True})
        if field["name"] != "csrfmiddlewaretoken"
    }


def test_untouched_ttl_form_keeps_the_expiry_of_a_key_in_the_stampede_buffer(admin_client: Client, buffered_key):
    cache, url = buffered_key
    form = _ttl_form_data(admin_client.get(url).content)

    response = admin_client.post(url, form, follow=True)

    assert form["ttl_value"] == "0"
    assert cache.ttl("stampede:ttl", stampede_prevention=False) in range(1, 51)
    assert response.status_code == 200
    assert "unchanged" in response.content.decode()


def test_emptied_ttl_form_removes_the_expiry_of_a_key_in_the_stampede_buffer(admin_client: Client, buffered_key):
    cache, url = buffered_key
    form = _ttl_form_data(admin_client.get(url).content)

    response = admin_client.post(url, {**form, "ttl_value": ""})

    assert response.status_code == 302
    assert cache.ttl("stampede:ttl", stampede_prevention=False) is None


# unknown is this package's label, not a server-side type name, so SCAN ... TYPE unknown matched nothing.
def test_unknown_type_is_not_pushed_into_scan(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("scan:plain", "value")
    spy = mocker.spy(type(test_cache), "scan")

    response = admin_client.get(_key_list_url("default") + "&type=unknown")

    assert response.status_code == 200
    assert spy.call_args.kwargs["key_type"] is None


def test_a_modelled_type_is_still_pushed_into_scan(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("scan:plain", "value")
    spy = mocker.spy(type(test_cache), "scan")

    response = admin_client.get(_key_list_url("default") + "&type=string")

    assert response.status_code == 200
    assert spy.call_args.kwargs["key_type"] == "string"


def test_unknown_typed_keys_are_listed(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("scan:opaque", "value")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)
    mocker.patch.object(Pipeline, "_decode_type", return_value=KeyType.UNKNOWN)

    response = admin_client.get(_key_list_url("default") + "&type=unknown")

    assert response.status_code == 200
    assert _result_column(response.content, "key_name") == ["scan:opaque"]


# Clear wipes the whole cache and Add key writes one, so neither belongs on the page of a view_key-only user.
@pytest.mark.usefixtures("_allow_flush")
def test_view_only_user_gets_no_key_list_tools(db, test_cache):
    client = _staff_client(["view_key"])

    response = client.get(_key_list_url("default"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'value="clear_cache"' not in content
    assert 'class="addlink"' not in content


@pytest.mark.usefixtures("_allow_flush")
def test_the_permissions_bring_the_key_list_tools_back(db, test_cache):
    client = _staff_client(["view_key", "add_key", "view_cache", "change_cache"])

    response = client.get(_key_list_url("default"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'value="clear_cache"' in content
    assert 'class="addlink"' in content


@pytest.mark.usefixtures("_allow_flush")
def test_clear_is_refused_without_the_cache_permission(db, test_cache: RespCache):
    test_cache.set("survivor", "value")
    client = _staff_client(["view_key"])

    response = client.post(_key_list_url("default"), {"action": "clear_cache", "cache_name": "default"})

    assert response.status_code == 403
    assert test_cache.get("survivor") == "value"


# delete() returns False when nothing was removed; reporting a success read as the wrong key being targeted.
def test_key_page_delete_of_a_missing_key_warns(admin_client: Client, test_cache):
    response = admin_client.post(
        _key_detail_url("default", "never:existed"),
        {"action": "delete"},
        follow=True,
    )

    assert response.status_code == 200
    assert "Key not found, nothing was deleted." in response.content.decode()


def test_bulk_delete_counts_a_missing_key_separately(admin_client: Client, test_cache: RespCache):
    test_cache.set("bulk:present", "value")

    response = admin_client.post(
        _key_list_url("default"),
        {
            "action": "delete_selected_keys",
            "_selected_action": [
                Key.make_pk("default", "bulk:present"),
                Key.make_pk("default", "bulk:never:existed"),
            ],
        },
        follow=True,
    )

    assert response.status_code == 200
    content = response.content.decode()
    assert "Successfully deleted 1 key(s)." in content
    assert "1 key(s) were already gone." in content


# A missing slow log is not a failure, so the page drops the section instead of quoting NotSupportedError.
def test_locmem_detail_hides_the_slow_log_section(admin_client: Client, test_cache, mocker):
    mocker.patch.object(
        type(caches["local"]),
        "slowlog_get",
        side_effect=NotSupportedError("slowlog_get", "LocMemCache"),
    )

    response = admin_client.get(_cache_detail_url("local"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Slow Log" not in content
    assert "could not be read" not in content


_DOWN = OSError(f"Error 111 connecting to {_SECRET_LOCATION}. Connection refused.")


def _assert_redirected_with_masked_error(response) -> None:
    assert response.status_code == 200
    assert response.redirect_chain[-1][0] == _cache_list_url()
    content = response.content.decode()
    assert "is unreachable" in content
    assert "s3cr3t-pw" not in content
    assert "***@cache.example.test" in content


# get_cache() only builds the backend; has_key is the first command to reach the server, and its failure used to 500.
def test_key_detail_get_unreachable_cache_redirects(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "type", side_effect=_DOWN)
    mocker.patch.object(type(test_cache), "has_key", side_effect=_DOWN)

    response = admin_client.get(_key_detail_url("default", "down:key"), follow=True)

    _assert_redirected_with_masked_error(response)


def test_key_detail_post_unreachable_cache_redirects(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "has_key", side_effect=_DOWN)

    response = admin_client.post(
        _key_detail_url("default", "down:key"),
        {"action": "update", "value": '"x"'},
        follow=True,
    )

    _assert_redirected_with_masked_error(response)


def test_key_add_post_unreachable_cache_redirects(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "has_key", side_effect=_DOWN)

    response = admin_client.post(_key_add_url("default"), {"key": "down:key", "type": "string"}, follow=True)

    _assert_redirected_with_masked_error(response)


# A form rendered for one type must not write through to a key that has since been recreated as another type.
def test_string_update_does_not_overwrite_a_hash(admin_client: Client, test_cache: RespCache):
    test_cache.hset("retyped:key", "field", "kept")

    response = admin_client.post(
        _key_detail_url("default", "retyped:key"),
        {"action": "update", "value": '"replacement"'},
        follow=True,
    )

    assert response.status_code == 200
    assert test_cache.type("retyped:key") == KeyType.HASH
    assert test_cache.hget("retyped:key", "field") == "kept"
    assert "is now a hash, not a string" in response.content.decode()


def test_list_push_does_not_touch_a_set(admin_client: Client, test_cache: RespCache):
    test_cache.sadd("retyped:set", "a")

    response = admin_client.post(
        _key_detail_url("default", "retyped:set"),
        {"action": "rpush", "value": '"b"'},
        follow=True,
    )

    assert response.status_code == 200
    assert test_cache.smembers("retyped:set") == {"a"}
    assert "is now a set, not a list" in response.content.decode()


def test_delete_and_ttl_are_type_agnostic(admin_client: Client, test_cache: RespCache):
    test_cache.hset("retyped:ttl", "field", "v")

    admin_client.post(_key_detail_url("default", "retyped:ttl"), {"action": "set_ttl", "ttl_value": "120"})
    assert test_cache.ttl("retyped:ttl") > 0

    admin_client.post(_key_detail_url("default", "retyped:ttl"), {"action": "delete"})
    assert not test_cache.has_key("retyped:ttl")


# Hash and set pages used to HGETALL/SMEMBERS the whole key; now only the requested page crosses the wire.
def test_hash_second_page_reads_only_its_fields(admin_client: Client, test_cache: RespCache, mocker):
    from django_cachex.admin.helpers import PAGE_SIZE

    test_cache.hset("paged:hash", mapping={f"f{i:04d}": f"v{i}" for i in range(PAGE_SIZE + 5)})
    mocker.patch.object(type(test_cache), "hgetall", side_effect=AssertionError("whole hash fetched"))
    mocker.patch.object(type(test_cache), "hkeys", side_effect=AssertionError("every field name fetched"))
    script_results: list[list] = []
    eval_script = test_cache.eval_script

    def record(*args, **kwargs):
        script_results.append(eval_script(*args, **kwargs))
        return script_results[-1]

    mocker.patch.object(test_cache, "eval_script", side_effect=record)

    response = admin_client.get(_key_detail_url("default", "paged:hash") + "?page=2")

    assert response.status_code == 200
    assert [len(rows) for _cursor, _skip, rows in script_results] == [5]
    content = response.content.decode()
    assert set(re.findall(r"&quot;v(\d+)&quot;", content)) == {str(i) for i in range(PAGE_SIZE, PAGE_SIZE + 5)}


def test_hash_page_emptied_between_hkeys_and_fetch_is_not_reported_as_empty(
    admin_client: Client,
    test_cache: RespCache,
    mocker,
):
    """Regression: the page's fields vanishing after HKEYS rendered "Hash is empty." under a non-zero count."""
    test_cache.hset("racing:hash", mapping={"f1": "v1", "f2": "v2"})
    mocker.patch("django_cachex.admin.helpers._hash_entries", return_value=[])

    response = admin_client.get(_key_detail_url("default", "racing:hash"))

    assert response.status_code == 200
    content = response.content.decode()
    assert "Hash is empty." not in content
    assert "No fields on this page." in content
    assert "(2 fields)" in content


def test_hash_page_on_locmem_uses_hmget(admin_client: Client, test_cache: RespCache, mocker):
    from django_cachex.admin.helpers import PAGE_SIZE

    cache = caches["local"]
    cache.hset("paged:hash", mapping={f"f{i:04d}": f"v{i}" for i in range(PAGE_SIZE + 5)})
    hmget = mocker.patch.object(cache, "hmget", wraps=cache.hmget)

    response = admin_client.get(_key_detail_url("local", "paged:hash") + "?page=2")

    assert response.status_code == 200
    assert len(hmget.call_args.args) - 1 == 5  # (key, *fields)
    assert "&quot;v104&quot;" in response.content.decode()


def test_set_page_uses_sscan(admin_client: Client, test_cache: RespCache, mocker):
    from django_cachex.admin.helpers import PAGE_SIZE

    members = [f"m{i:04d}" for i in range(PAGE_SIZE + 5)]
    test_cache.sadd("paged:set", *members)
    mocker.patch.object(type(test_cache), "smembers", side_effect=AssertionError("whole set fetched"))

    first = admin_client.get(_key_detail_url("default", "paged:set"))
    second = admin_client.get(_key_detail_url("default", "paged:set") + "?page=2")

    assert first.status_code == second.status_code == 200
    shown = sorted(set(re.findall(r"&quot;(m\d{4})&quot;", first.content.decode())))
    assert len(shown) == PAGE_SIZE
    shown += set(re.findall(r"&quot;(m\d{4})&quot;", second.content.decode()))
    assert sorted(shown) == members
    assert f"({PAGE_SIZE + 5} members)" in first.content.decode()


def test_set_page_on_locmem_falls_back_to_smembers(admin_client: Client, test_cache: RespCache):
    from django_cachex.admin.helpers import PAGE_SIZE

    cache = caches["local"]
    cache.sadd("paged:set", *(f"m{i:04d}" for i in range(PAGE_SIZE + 5)))

    response = admin_client.get(_key_detail_url("local", "paged:set") + "?page=2")

    assert response.status_code == 200
    shown = dict.fromkeys(re.findall(r"&quot;(m\d{4})&quot;", response.content.decode()))
    assert list(shown) == [f"m{i:04d}" for i in range(PAGE_SIZE, PAGE_SIZE + 5)]


def test_ttl_type_and_size_are_batched(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("batched:s", "value")
    test_cache.rpush("batched:l", "a", "b")
    test_cache.hset("batched:h", "f", "v")
    for per_key in ("ttl", "type", "llen", "hlen"):
        mocker.patch.object(type(test_cache), per_key, side_effect=AssertionError(f"{per_key} called per key"))
    pipeline = mocker.patch.object(test_cache, "pipeline", wraps=test_cache.pipeline)

    response = admin_client.get(_key_list_url("default") + "&q=batched:*")

    assert response.status_code == 200
    assert pipeline.call_count == 2  # ttl/type, then collection sizes
    rows = dict(
        zip(
            _result_column(response.content, "key_name"),
            _result_column(response.content, "size_display"),
            strict=True,
        ),
    )
    assert rows["batched:l"].startswith("2")
    assert rows["batched:h"].startswith("1")
    assert rows["batched:s"].startswith(str(len(test_cache.encode("value"))))


def test_key_list_still_lists_on_locmem(admin_client: Client, test_cache: RespCache):
    cache = caches["local"]
    cache.set("plain:s", "value")
    cache.rpush("plain:l", "a", "b")

    response = admin_client.get(_key_list_url("local") + "&q=plain:*")

    assert response.status_code == 200
    assert sorted(_result_column(response.content, "key_name")) == ["plain:l", "plain:s"]


def test_size_pipeline_failure_falls_back_per_key(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.rpush("fallback:l", "a", "b", "c")
    mocker.patch("django_cachex.admin.queryset._pipelined_sizes", side_effect=RuntimeError("boom"))

    response = admin_client.get(_key_list_url("default") + "&q=fallback:*")

    assert response.status_code == 200
    assert _result_column(response.content, "size_display")[0].startswith("3")


# Stock Django backends have neither expire nor persist, so the TTL form promised what the handler could not do.
def test_stock_backend_ttl_form_is_hidden(admin_client: Client, test_cache):
    with _stock_alias():
        caches["stock"].set("stock:ttl", "value")
        response = admin_client.get(_key_detail_url("stock", "stock:ttl"))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'name="ttl_value"' not in content
    assert 'id="delete-form"' in content


def test_stock_backend_hand_crafted_set_ttl_is_refused(admin_client: Client, test_cache):
    with _stock_alias():
        caches["stock"].set("stock:ttl", "value")
        response = admin_client.post(
            _key_detail_url("stock", "stock:ttl"),
            {"action": "set_ttl", "ttl": "60"},
            follow=True,
        )
        assert caches["stock"].get("stock:ttl") == "value"

    assert response.status_code == 200
    assert "does not support changing a key" in response.content.decode()


def test_ttl_form_is_shown_on_cachex_locmem(admin_client: Client, test_cache):
    caches["local"].set("local:ttl", "value")

    response = admin_client.get(_key_detail_url("local", "local:ttl"))

    assert 'name="ttl_value"' in response.content.decode()


@pytest.mark.parametrize("bad_type", ["unknown", "bitmap", "<script>"])
def test_unknown_type_is_a_form_error(admin_client: Client, test_cache, bad_type: str):
    response = admin_client.post(_key_add_url("default"), {"key": "add:odd", "type": bad_type})

    assert response.status_code == 200
    content = response.content.decode()
    assert "Unknown key type" in content
    assert 'name="key"' in content
    assert not test_cache.has_key("add:odd")


def test_unknown_type_gets_the_generic_help(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("help:opaque", "value")
    mocker.patch.object(type(test_cache), "type", return_value=KeyType.UNKNOWN)

    response = admin_client.get(_key_detail_url("default", "help:opaque") + "?help=1")

    assert response.status_code == 200
    content = response.content.decode()
    assert "<strong>Key Details</strong>" in content


_ERROR = OSError(f"Error 111 connecting to {_SECRET_LOCATION}. Connection refused.")


def _assert_masked(content: str) -> None:
    assert "s3cr3t-pw" not in content
    assert "***@cache.example.test" in content


# Any message that quotes a driver exception can leak the connection URL.
def test_key_detail_action_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.rpush("masked:list", "a")
    mocker.patch.object(type(test_cache), "lpop", side_effect=_ERROR)

    response = admin_client.post(_key_detail_url("default", "masked:list"), {"action": "lpop"}, follow=True)

    assert response.status_code == 200
    _assert_masked(response.content.decode())


def test_key_detail_value_read_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("masked:str", "value")
    mocker.patch.object(type(test_cache), "eval_script", side_effect=_ERROR)
    mocker.patch.object(type(test_cache), "get", side_effect=_ERROR)

    response = admin_client.get(_key_detail_url("default", "masked:str"))

    assert response.status_code == 200
    _assert_masked(response.content.decode())


def test_container_page_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.rpush("masked:page", "a")
    mocker.patch.object(type(test_cache), "llen", side_effect=_ERROR)

    response = admin_client.get(_key_detail_url("default", "masked:page"))

    assert response.status_code == 200
    _assert_masked(response.content.decode())


@pytest.mark.usefixtures("_allow_flush")
def test_cache_detail_flush_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "flush_db", side_effect=_ERROR)

    response = admin_client.post(_cache_detail_url("default"), {"action": "flush_db"}, follow=True)

    assert response.status_code == 200
    _assert_masked(response.content.decode())


def test_cache_detail_info_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "info", side_effect=_ERROR)

    response = admin_client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    _assert_masked(response.content.decode())


def test_key_list_query_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    mocker.patch.object(type(test_cache), "scan", side_effect=_ERROR)

    response = admin_client.get(_key_list_url("default"))

    assert response.status_code == 200
    _assert_masked(response.content.decode())


def test_bulk_delete_error_is_masked(admin_client: Client, test_cache: RespCache, mocker):
    test_cache.set("masked:bulk", "value")
    mocker.patch.object(type(test_cache), "delete", side_effect=_ERROR)

    response = admin_client.post(
        reverse("admin:django_cachex_key_changelist") + "?cache=default",
        {"action": "delete_selected_keys", "_selected_action": [Key.make_pk("default", "masked:bulk")]},
        follow=True,
    )

    assert response.status_code == 200
    _assert_masked(response.content.decode())


@pytest.mark.parametrize("action", ["flush_db", "clear_all_versions"])
@pytest.mark.usefixtures("_allow_flush")
def test_cache_detail_actions_need_change_cache(db, test_cache: RespCache, action: str):
    client = _staff_client(["view_cache"])
    test_cache.set("gated:cache", "value")

    response = client.post(_cache_detail_url("default"), {"action": action})

    assert response.status_code == 403
    assert test_cache.get("gated:cache") == "value"


def test_key_delete_needs_delete_key(db, test_cache: RespCache):
    client = _staff_client(["view_key", "change_key"])
    test_cache.set("gated:key", "value")

    response = client.post(_key_detail_url("default", "gated:key"), {"action": "delete"})

    assert response.status_code == 403
    assert test_cache.get("gated:key") == "value"


def test_stream_entry_with_a_field_named_items_renders(admin_client: Client, test_cache: RespCache):
    # Regression: the template looped over ``fields.items``, which resolved the "items" field.
    test_cache.xadd("fields:stream", {"items": "3", "name": "a"})

    response = admin_client.get(_key_detail_url("default", "fields:stream"))

    assert response.status_code == 200
    assert "items: 3, name: a" in response.content.decode()


def test_key_list_pagination_keeps_a_param_named_items(admin_client: Client, test_cache: RespCache):
    # Regression: ``cl.params.items`` in the template resolved the ``items`` param.
    for i in range(20):
        test_cache.set(f"paging:key{i}", i)

    response = admin_client.get(_key_list_url("default") + "&count=1&items=x")

    assert response.status_code == 200
    next_link = BeautifulSoup(response.content, "html.parser").find("a", string="Next")
    assert next_link is not None
    assert "&items=x&" in next_link["href"]


@pytest.mark.parametrize("original_score", ["", "1"], ids=["without_cas", "with_cas"])
def test_zset_rename_refuses_to_overwrite_an_existing_member(
    admin_client: Client,
    test_cache: RespCache,
    original_score: str,
):
    test_cache.zadd("inputs:zclobber", {"alpha": 1.0, "beta": 2.0})

    response = admin_client.post(
        _key_detail_url("default", "inputs:zclobber"),
        {
            "action": "zupdate",
            "member": '"alpha"',
            "new_member": '"beta"',
            "score_value": "5",
            "original_score": original_score,
        },
        follow=True,
    )

    assert test_cache.zrange("inputs:zclobber", 0, -1, withscores=True) == [("alpha", 1.0), ("beta", 2.0)]
    assert "Member &#x27;beta&#x27; already exists." in response.content.decode()


def test_zset_rename_with_the_loaded_score_moves_the_member(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("inputs:zcas", {"alpha": 1.0})

    response = admin_client.post(
        _key_detail_url("default", "inputs:zcas"),
        {"action": "zupdate", "member": '"alpha"', "new_member": '"beta"', "score_value": "3.5", "original_score": "1"},
    )

    assert response.status_code == 302
    assert test_cache.zrange("inputs:zcas", 0, -1, withscores=True) == [("beta", 3.5)]


def test_zset_rename_after_a_concurrent_score_change_is_refused(admin_client: Client, test_cache: RespCache):
    test_cache.zadd("inputs:zstale", {"alpha": 2.0})

    admin_client.post(
        _key_detail_url("default", "inputs:zstale"),
        {"action": "zupdate", "member": '"alpha"', "new_member": '"beta"', "score_value": "3.5", "original_score": "1"},
    )

    assert test_cache.zrange("inputs:zstale", 0, -1, withscores=True) == [("alpha", 2.0)]


def test_hash_field_with_surrounding_spaces_can_be_edited(admin_client: Client, test_cache: RespCache):
    test_cache.hset("inputs:hpadded", " padded ", "a")

    response = admin_client.post(
        _key_detail_url("default", "inputs:hpadded"),
        {"action": "hupdate", "field": " padded ", "new_field": " padded ", "field_value": '"b"'},
    )

    assert response.status_code == 302
    assert test_cache.hgetall("inputs:hpadded") == {" padded ": "b"}


def test_hash_field_with_surrounding_spaces_can_be_deleted(admin_client: Client, test_cache: RespCache):
    test_cache.hset("inputs:hpadded", mapping={" padded ": "a", "padded": "b"})

    response = admin_client.post(_key_detail_url("default", "inputs:hpadded"), {"action": "hdel", "field": " padded "})

    assert response.status_code == 302
    assert test_cache.hgetall("inputs:hpadded") == {"padded": "b"}


@pytest.mark.usefixtures("_allow_flush")
def test_flush_database_warns_that_a_cluster_loses_every_primary(admin_client: Client, test_cache: RespCache):
    response = admin_client.get(_cache_detail_url("default"))

    content = response.content.decode()
    assert "On a cluster, every primary is flushed." in content
    assert "targeted primary" not in content


@pytest.mark.parametrize("key_type", ["list", "set", "hash", "zset", "stream"])
def test_confirm_dialogs_escape_translated_text(admin_client: Client, test_cache: RespCache, mocker, key_type: str):
    # A translation with an apostrophe used to end the JavaScript string early.
    {
        "list": lambda: test_cache.rpush("confirm:key", "a"),
        "set": lambda: test_cache.sadd("confirm:key", "a"),
        "hash": lambda: test_cache.hset("confirm:key", "f", "a"),
        "zset": lambda: test_cache.zadd("confirm:key", {"a": 1.0}),
        "stream": lambda: test_cache.xadd("confirm:key", {"f": "a"}),
    }[key_type]()
    mocker.patch("django.template.base.gettext_lazy", side_effect=lambda msgid: msgid.replace("?", " l'élément ?"))

    response = admin_client.get(_key_detail_url("default", "confirm:key"))

    confirms = re.findall(r"confirm\('([^)]*)'\)", response.content.decode())
    assert len(confirms) > 1
    assert all("&#x27;" not in text for text in confirms)


def test_migrate_creates_an_access_permission_per_alias(db):
    access = Permission.objects.filter(content_type__app_label="django_cachex", codename__startswith="access_")

    assert {permission.codename for permission in access} == {f"access_{alias}" for alias in settings.CACHES}
    assert "'default'" in access.get(codename="access_default").name


def _cache_filter_choices(content: bytes) -> list[str]:
    """Return the aliases the key list's cache filter offers."""
    soup = BeautifulSoup(content, "html.parser")
    return [link.get_text(strip=True) for link in soup.select('details[data-filter-title="cache"] a')]


def test_key_list_is_forbidden_without_the_alias_permission(db, test_cache):
    client = _staff_client(["view_key"])

    response = client.get(_key_list_url("local"))

    assert response.status_code == 403


def test_key_list_defaults_to_the_first_granted_alias(db, test_cache: RespCache):
    test_cache.set("default:hidden", "value")
    caches["local"].set("local:shown", "value")
    client = _staff_client(["view_key"], aliases=("local",))

    response = client.get(reverse("admin:django_cachex_key_changelist"))

    assert response.status_code == 200
    keys = _result_column(response.content, "key_name")
    assert "local:shown" in keys
    assert "default:hidden" not in keys
    assert _cache_filter_choices(response.content) == []


def test_key_list_filter_offers_only_granted_aliases(db, test_cache):
    client = _staff_client(["view_key"], aliases=("default", "local"))

    with _extra_cache("other", {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": "admin-test-other"}):
        response = client.get(_key_list_url("default"))

    assert response.status_code == 200
    assert _cache_filter_choices(response.content) == ["default", "local"]


def test_key_list_without_any_alias_permission_is_forbidden(db, test_cache):
    client = _staff_client(["view_key"], aliases=())

    response = client.get(reverse("admin:django_cachex_key_changelist"))

    assert response.status_code == 403


def test_key_list_of_an_unknown_alias_still_says_not_found(db, test_cache):
    client = _staff_client(["view_key"])

    response = client.get(_key_list_url("nonexistent"))

    assert response.status_code == 302
    assert response.url == _cache_list_url()


def test_key_queryset_refuses_an_alias_without_permission(db, test_cache, rf):
    request = rf.get(_key_list_url("local"))
    request.user = User.objects.create_user(username="staff_queryset", is_staff=True)

    with pytest.raises(PermissionDenied):
        site._registry[Key].get_queryset(request)


@pytest.mark.usefixtures("_allow_flush")
def test_clear_tool_is_forbidden_without_the_alias_permission(db, test_cache: RespCache):
    test_cache.set("guarded:clear", "value")
    client = _staff_client(["view_key", "view_cache", "change_cache"], aliases=("local",))

    response = client.post(_key_list_url("default"), {"action": "clear_cache", "cache_name": "default"})

    assert response.status_code == 403
    assert test_cache.get("guarded:clear") == "value"


def test_a_group_grant_opens_the_key_list(db, test_cache):
    caches["local"].set("group:granted", "value")
    group = Group.objects.create(name="cache readers")
    group.permissions.add(
        Permission.objects.get(codename="view_key", content_type__app_label="django_cachex"),
        _access_permission("local"),
    )
    user = User.objects.create_user(username="staff_in_group", is_staff=True)
    user.groups.add(group)
    client = Client()
    client.force_login(user)

    response = client.get(reverse("admin:django_cachex_key_changelist"))

    assert response.status_code == 200
    assert "group:granted" in _result_column(response.content, "key_name")


def test_key_detail_is_forbidden_without_the_alias_permission(db, test_cache):
    caches["local"].set("forbidden:detail", "classified-value")
    client = _staff_client(["view_key"])

    response = client.get(_key_detail_url("local", "forbidden:detail"))

    assert response.status_code == 403
    assert "classified-value" not in response.content.decode()


def test_add_key_is_forbidden_without_the_alias_permission(db, test_cache):
    client = _staff_client(["view_key", "add_key"])

    response = client.get(_key_add_url("local"))

    assert response.status_code == 403


def test_add_key_defaults_to_the_first_granted_alias(db, test_cache):
    client = _staff_client(["view_key", "add_key"], aliases=("local",))

    response = client.get(reverse("admin:django_cachex_key_add"))

    assert response.status_code == 200
    heading = BeautifulSoup(response.content, "html.parser").select_one("#content h1")
    assert heading is not None
    assert "local" in heading.get_text()


def test_bulk_delete_spanning_a_forbidden_alias_deletes_nothing(db, test_cache: RespCache):
    test_cache.set("allowed:bulk", "value")
    caches["local"].set("forbidden:span", "value")
    client = _staff_client(["view_key", "delete_key"])

    response = client.post(
        _key_list_url("default"),
        {
            "action": "delete_selected_keys",
            "_selected_action": [Key.make_pk("default", "allowed:bulk"), Key.make_pk("local", "forbidden:span")],
        },
    )

    assert response.status_code == 403
    assert test_cache.get("allowed:bulk") == "value"
    assert caches["local"].get("forbidden:span") == "value"


@pytest.mark.usefixtures("_allow_flush")
def test_flush_spanning_a_forbidden_cache_flushes_nothing(db, test_cache: RespCache):
    test_cache.set("allowed:flush", "value")
    caches["local"].set("forbidden:flush", "value")
    client = _staff_client(["view_cache", "change_cache"])

    response = client.post(
        _cache_list_url(),
        {"action": "flush_selected", "_selected_action": ["default", "local"]},
        follow=True,
    )

    soup = BeautifulSoup(response.content, "html.parser")
    errors = [item.get_text() for item in soup.select("ul.messagelist li.error")]
    assert any("'local'" in error for error in errors)
    assert test_cache.get("allowed:flush") == "value"
    assert caches["local"].get("forbidden:flush") == "value"


def test_cache_list_links_only_the_keys_of_granted_caches(db, test_cache):
    client = _staff_client(["view_cache"])

    response = client.get(_cache_list_url())

    assert response.status_code == 200
    names = _result_column(response.content, "name")
    links = _result_column(response.content, "keys_link")
    assert dict(zip(names, links, strict=True)) == {"default": "List Keys", "local": "-"}


@pytest.mark.usefixtures("_allow_flush")
@pytest.mark.parametrize(
    ("action", "aliases"),
    [
        pytest.param("clear_all_versions", ("local",), id="clear-without-the-page-alias"),
        pytest.param("flush_db", ("default",), id="flushdb-without-every-alias"),
    ],
)
def test_cache_detail_actions_need_their_aliases(db, test_cache: RespCache, action: str, aliases: tuple[str, ...]):
    client = _staff_client(["view_cache", "change_cache"], aliases=aliases)
    test_cache.set("gated:aliases", "value")

    response = client.post(_cache_detail_url("default"), {"action": action})

    assert response.status_code == 403
    assert test_cache.get("gated:aliases") == "value"


@pytest.mark.usefixtures("_allow_flush")
@pytest.mark.parametrize(
    ("aliases", "buttons"),
    [
        pytest.param((), set(), id="no-alias"),
        pytest.param(("default",), {"clear_all_versions"}, id="page-alias"),
        pytest.param(("default", "local"), {"clear_all_versions", "flush_db"}, id="every-alias"),
    ],
)
def test_danger_zone_offers_what_the_aliases_allow(db, test_cache, aliases, buttons):
    client = _staff_client(["view_cache", "change_cache"], aliases=aliases)

    response = client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    soup = BeautifulSoup(response.content, "html.parser")
    assert {field["value"] for field in soup.select('.danger-zone input[name="action"]')} == buttons


@pytest.mark.parametrize(
    ("aliases", "command", "shown"),
    [
        pytest.param(("default",), ["SET :1:session:abc123 secret"], "SET", id="valkey-py-without-every-alias"),
        pytest.param(("default",), ["SET", ":1:session:abc123", "secret"], "SET", id="glide-without-every-alias"),
        pytest.param(
            ("default", "local"),
            ["SET", ":1:session:abc123", "secret"],
            "SET :1:session:abc123 secret",
            id="glide-with-every-alias",
        ),
    ],
)
def test_slow_log_shows_arguments_only_with_every_alias(db, test_cache, mocker, aliases, command, shown):
    mocker.patch.object(type(test_cache), "slowlog_get", return_value=[{"command": command}])
    client = _staff_client(["view_cache"], aliases=aliases)

    response = client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    soup = BeautifulSoup(response.content, "html.parser")
    assert [cell.get_text(strip=True) for cell in soup.select("#result_list .command-cell")] == [shown]


@pytest.mark.parametrize(
    ("aliases", "tools"),
    [
        pytest.param((), ["Help"], id="no-alias"),
        pytest.param(("default",), ["List Keys", "Help"], id="page-alias"),
    ],
)
def test_cache_detail_links_the_keys_only_with_the_alias_permission(db, test_cache, aliases, tools):
    client = _staff_client(["view_cache"], aliases=aliases)

    response = client.get(_cache_detail_url("default"))

    assert response.status_code == 200
    soup = BeautifulSoup(response.content, "html.parser")
    assert [link.get_text(strip=True) for link in soup.select("ul.object-tools a")] == tools


def test_a_tracking_cache_needs_the_permission_of_its_transport(db, test_cache):
    client = _staff_client(["view_key"], aliases=("tracking",))

    with _extra_cache(
        "tracking",
        {"BACKEND": "django_cachex.cache.TrackingCache", "OPTIONS": {"transport": "default"}},
    ):
        response = client.get(_key_list_url("tracking"))

    assert response.status_code == 403


@pytest.mark.usefixtures("_allow_flush")
@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("django.core.cache.backends.redis.RedisCache", id="redis-flushdb"),
        pytest.param("django.core.cache.backends.memcached.PyMemcacheCache", id="memcached-flush-all"),
    ],
)
def test_clear_tool_on_a_server_wide_backend_needs_every_alias(db, test_cache: RespCache, backend: str):
    test_cache.set("shared:clear", "value")
    client = _staff_client(["view_key", "change_cache"], aliases=("stock",))

    with _extra_cache("stock", {"BACKEND": backend, "LOCATION": settings.CACHES["default"]["LOCATION"]}):
        response = client.post(_key_list_url("stock"), {"action": "clear_cache", "cache_name": "stock"})

    assert response.status_code == 403
    assert test_cache.get("shared:clear") == "value"


@pytest.mark.usefixtures("_allow_flush")
def test_flush_action_on_a_server_wide_backend_needs_every_alias(db, test_cache: RespCache):
    test_cache.set("shared:flush", "value")
    client = _staff_client(["view_cache", "change_cache"], aliases=("stock",))

    with _extra_cache(
        "stock",
        {"BACKEND": "django.core.cache.backends.redis.RedisCache", "LOCATION": settings.CACHES["default"]["LOCATION"]},
    ):
        response = client.post(
            _cache_list_url(),
            {"action": "flush_selected", "_selected_action": ["stock"]},
            follow=True,
        )

    soup = BeautifulSoup(response.content, "html.parser")
    errors = [item.get_text() for item in soup.select("ul.messagelist li.error")]
    assert any("'stock'" in error for error in errors)
    assert test_cache.get("shared:flush") == "value"
