"""Tests for basic cache operations: set, get, add, delete, get_many, set_many."""

import datetime

from pytest_mock import MockerFixture

from django_cachex.cache import RedisCache
from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.msgpack import MSGPackSerializer
from tests.settings_wrapper import SettingsWrapper


class TestBasicCacheOperations:
    def test_setnx(self, cache: RedisCache):
        # we should ensure there is no test_key_nx in redis
        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

        res = cache.set("test_key_nx", 1, nx=True)
        assert bool(res) is True
        # test that second set will have
        res = cache.set("test_key_nx", 2, nx=True)
        assert res is False
        res = cache.get("test_key_nx")
        assert res == 1

        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

    def test_unicode_keys(self, cache: RedisCache):
        cache.set("ключ", "value")
        res = cache.get("ключ")
        assert res == "value"

    def test_save_and_integer(self, cache: RedisCache):
        cache.set("test_key", 2)
        res = cache.get("test_key", "Foo")

        assert isinstance(res, int)
        assert res == 2

    def test_save_string(self, cache: RedisCache):
        cache.set("test_key", "hello" * 1000)
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "hello" * 1000

        cache.set("test_key", "2")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "2"

    def test_save_unicode(self, cache: RedisCache):
        cache.set("test_key", "heló")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "heló"

    def test_save_dict(self, cache: RedisCache):
        if isinstance(cache._serializers[0], JSONSerializer | MSGPackSerializer):
            # JSONSerializer and MSGPackSerializer use the isoformat for
            # datetimes.
            now_dt: str | datetime.datetime = datetime.datetime.now().isoformat()
        else:
            now_dt = datetime.datetime.now()

        test_dict = {"id": 1, "date": now_dt, "name": "Foo"}

        cache.set("test_key", test_dict)
        res = cache.get("test_key")

        assert isinstance(res, dict)
        assert res["id"] == 1
        assert res["name"] == "Foo"
        assert res["date"] == now_dt

    def test_save_float(self, cache: RedisCache):
        float_val = 1.345620002

        cache.set("test_key", float_val)
        res = cache.get("test_key")

        assert isinstance(res, float)
        assert res == float_val

    def test_get_set_bool(self, cache: RedisCache):
        cache.set("bool", True)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is True

        cache.set("bool", False)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is False

    def test_set_add(self, cache: RedisCache):
        cache.set("add_key", "Initial value")
        res = cache.add("add_key", "New value")
        assert res is False

        res = cache.get("add_key")
        assert res == "Initial value"
        res = cache.add("other_key", "New value")
        assert res is True

    def test_get_many(self, cache: RedisCache):
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_get_many_unicode(self, cache: RedisCache):
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")

        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": "1", "b": "2", "c": "3"}

    def test_set_many(self, cache: RedisCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_set_via_pipeline(
        self,
        cache: RedisCache,
        mocker: MockerFixture,
    ):
        """Test that pipeline.set() works correctly."""
        key = "pipeline_key"
        value = "pipeline_value"

        # Use the new architecture's pipeline API
        with cache.pipeline() as pipe:
            pipe.set(key, value)
            results = pipe.execute()

        assert results == [True]
        assert cache.get(key) == value

    def test_delete(self, cache: RedisCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete("a")
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"b": 2, "c": 3}

        res = cache.delete("a")
        assert bool(res) is False

    def test_delete_return_value_type(self, cache: RedisCache):
        """delete() returns a boolean (Django 3.1+)."""
        cache.set("a", 1)
        res = cache.delete("a")
        assert isinstance(res, bool)
        assert res is True
        res = cache.delete("b")
        assert isinstance(res, bool)
        assert res is False

    def test_delete_many(self, cache: RedisCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete_many(["a", "b"])
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = cache.delete_many(["a", "b"])
        assert bool(res) is False

    def test_delete_many_generator(self, cache: RedisCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete_many(key for key in ["a", "b"])
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = cache.delete_many(["a", "b"])
        assert bool(res) is False

    def test_delete_many_empty_generator(self, cache: RedisCache):
        from typing import cast

        res = cache.delete_many(key for key in cast("list[str]", []))
        assert bool(res) is False

    def test_clear(self, cache: RedisCache):
        cache.set("foo", "bar")
        value_from_cache = cache.get("foo")
        assert value_from_cache == "bar"
        cache.clear()
        value_from_cache_after_clear = cache.get("foo")
        assert value_from_cache_after_clear is None

    def test_close(self, cache: RedisCache, settings: SettingsWrapper):
        settings.DJANGO_REDIS_CLOSE_CONNECTION = True
        cache.set("f", "1")
        cache.close()

    def test_close_client(self, cache: RedisCache, mocker: MockerFixture):
        # In the new architecture, cache IS the client, so close() is called directly
        # Just verify close() can be called without error
        cache.set("test_close_key", "value")
        cache.close()
        # Verify we can still use the cache after close (new connection should be created)
        cache.set("test_close_key2", "value2")
        assert cache.get("test_close_key2") == "value2"
