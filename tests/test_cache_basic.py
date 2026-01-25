"""Tests for basic cache operations: set, get, add, delete, get_many, set_many."""

import datetime

from pytest_mock import MockerFixture

from django_cachex.cache import KeyValueCache
from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.msgpack import MessagePackSerializer
from tests.settings_wrapper import SettingsWrapper


class TestSetIfNotExists:
    """Tests for set with nx (not exists) flag."""

    def test_set_nx_creates_new_key(self, cache: KeyValueCache):
        cache.delete("nx_key")
        assert cache.get("nx_key") is None

        result = cache.set("nx_key", 42, nx=True)
        assert result is True
        assert cache.get("nx_key") == 42

    def test_set_nx_does_not_overwrite(self, cache: KeyValueCache):
        cache.set("nx_existing", "original")
        result = cache.set("nx_existing", "changed", nx=True)
        assert result is False
        assert cache.get("nx_existing") == "original"

    def test_set_nx_cleanup(self, cache: KeyValueCache):
        cache.set("nx_cleanup", "temp", nx=True)
        cache.delete("nx_cleanup")
        assert cache.get("nx_cleanup") is None


class TestUnicodeSupport:
    """Tests for unicode key and value handling."""

    def test_cyrillic_key(self, cache: KeyValueCache):
        cache.set("ключ", "данные")
        assert cache.get("ключ") == "данные"

    def test_emoji_value(self, cache: KeyValueCache):
        cache.set("emoji_test", "Hello 🌍")
        assert cache.get("emoji_test") == "Hello 🌍"

    def test_chinese_characters(self, cache: KeyValueCache):
        cache.set("chinese", "你好世界")
        assert cache.get("chinese") == "你好世界"


class TestDataTypePersistence:
    """Tests verifying different data types are preserved through serialization."""

    def test_integer_storage(self, cache: KeyValueCache):
        cache.set("int_val", 99)
        result = cache.get("int_val", "fallback")
        assert isinstance(result, int)
        assert result == 99

    def test_large_string_storage(self, cache: KeyValueCache):
        large_content = "x" * 5000
        cache.set("large_str", large_content)
        result = cache.get("large_str")
        assert isinstance(result, str)
        assert len(result) == 5000
        assert result == large_content

    def test_numeric_string_stays_string(self, cache: KeyValueCache):
        cache.set("num_str", "12345")
        result = cache.get("num_str")
        assert isinstance(result, str)
        assert result == "12345"

    def test_accented_string(self, cache: KeyValueCache):
        cache.set("accented", "café résumé")
        result = cache.get("accented")
        assert isinstance(result, str)
        assert result == "café résumé"

    def test_dictionary_with_datetime(self, cache: KeyValueCache):
        if isinstance(cache._cache._serializers[0], JSONSerializer | MessagePackSerializer):
            timestamp: str | datetime.datetime = datetime.datetime.now().isoformat()
        else:
            timestamp = datetime.datetime.now()

        data = {"user_id": 42, "created": timestamp, "label": "Test"}
        cache.set("dict_data", data)
        result = cache.get("dict_data")

        assert isinstance(result, dict)
        assert result["user_id"] == 42
        assert result["label"] == "Test"
        assert result["created"] == timestamp

    def test_float_precision(self, cache: KeyValueCache):
        precise_val = 3.141592653589793
        cache.set("pi", precise_val)
        result = cache.get("pi")
        assert isinstance(result, float)
        assert result == precise_val

    def test_boolean_true(self, cache: KeyValueCache):
        cache.set("flag_on", True)
        result = cache.get("flag_on")
        assert isinstance(result, bool)
        assert result is True

    def test_boolean_false(self, cache: KeyValueCache):
        cache.set("flag_off", False)
        result = cache.get("flag_off")
        assert isinstance(result, bool)
        assert result is False


class TestAddOperation:
    """Tests for the add() method (set only if key doesn't exist)."""

    def test_add_fails_for_existing_key(self, cache: KeyValueCache):
        cache.set("preexisting", "first")
        result = cache.add("preexisting", "second")
        assert result is False
        assert cache.get("preexisting") == "first"

    def test_add_succeeds_for_new_key(self, cache: KeyValueCache):
        cache.delete("fresh_key")
        result = cache.add("fresh_key", "new_value")
        assert result is True
        assert cache.get("fresh_key") == "new_value"


class TestBulkGetOperations:
    """Tests for get_many() method."""

    def test_get_many_integers(self, cache: KeyValueCache):
        cache.set("x", 10)
        cache.set("y", 20)
        cache.set("z", 30)
        result = cache.get_many(["x", "y", "z"])
        assert result == {"x": 10, "y": 20, "z": 30}

    def test_get_many_strings(self, cache: KeyValueCache):
        cache.set("s1", "alpha")
        cache.set("s2", "beta")
        cache.set("s3", "gamma")
        result = cache.get_many(["s1", "s2", "s3"])
        assert result == {"s1": "alpha", "s2": "beta", "s3": "gamma"}

    def test_get_many_partial_match(self, cache: KeyValueCache):
        cache.set("found", "yes")
        cache.delete("missing")
        result = cache.get_many(["found", "missing"])
        assert result == {"found": "yes"}


class TestBulkSetOperations:
    """Tests for set_many() method."""

    def test_set_many_and_retrieve(self, cache: KeyValueCache):
        cache.set_many({"m1": 100, "m2": 200, "m3": 300})
        result = cache.get_many(["m1", "m2", "m3"])
        assert result == {"m1": 100, "m2": 200, "m3": 300}


class TestPipelineSetOperation:
    """Tests for pipeline.set() functionality."""

    def test_pipeline_set_returns_true(
        self,
        cache: KeyValueCache,
        mocker: MockerFixture,
    ):
        with cache.pipeline() as pipe:
            pipe.set("pipe_test", "pipe_val")
            results = pipe.execute()

        assert results == [True]
        assert cache.get("pipe_test") == "pipe_val"


class TestDeleteOperations:
    """Tests for delete() and delete_many() methods."""

    def test_delete_existing_key(self, cache: KeyValueCache):
        cache.set_many({"d1": 1, "d2": 2, "d3": 3})
        result = cache.delete("d1")
        assert result is True
        remaining = cache.get_many(["d1", "d2", "d3"])
        assert remaining == {"d2": 2, "d3": 3}

    def test_delete_nonexistent_key(self, cache: KeyValueCache):
        cache.delete("surely_missing")
        result = cache.delete("surely_missing")
        assert result is False

    def test_delete_returns_boolean(self, cache: KeyValueCache):
        cache.set("bool_del", "value")
        result = cache.delete("bool_del")
        assert isinstance(result, bool)
        assert result is True
        result = cache.delete("bool_del")
        assert isinstance(result, bool)
        assert result is False

    def test_delete_many_removes_multiple(self, cache: KeyValueCache):
        cache.set_many({"dm1": 1, "dm2": 2, "dm3": 3})
        result = cache.delete_many(["dm1", "dm2"])
        assert bool(result) is True
        remaining = cache.get_many(["dm1", "dm2", "dm3"])
        assert remaining == {"dm3": 3}

    def test_delete_many_already_deleted(self, cache: KeyValueCache):
        cache.delete_many(["gone1", "gone2"])
        result = cache.delete_many(["gone1", "gone2"])
        assert bool(result) is False

    def test_delete_many_with_generator(self, cache: KeyValueCache):
        cache.set_many({"gen1": 1, "gen2": 2, "gen3": 3})
        result = cache.delete_many(k for k in ["gen1", "gen2"])
        assert bool(result) is True
        remaining = cache.get_many(["gen1", "gen2", "gen3"])
        assert remaining == {"gen3": 3}

    def test_delete_many_empty_generator(self, cache: KeyValueCache):
        from typing import cast

        result = cache.delete_many(k for k in cast("list[str]", []))
        assert bool(result) is False


class TestClearOperation:
    """Tests for clear() method."""

    def test_clear_removes_all(self, cache: KeyValueCache):
        cache.set("to_clear", "exists")
        assert cache.get("to_clear") == "exists"
        cache.clear()
        assert cache.get("to_clear") is None


class TestCloseOperation:
    """Tests for close() method."""

    def test_close_with_setting(self, cache: KeyValueCache, settings: SettingsWrapper):
        settings.DJANGO_REDIS_CLOSE_CONNECTION = True
        cache.set("pre_close", "value")
        cache.close()

    def test_close_and_reconnect(self, cache: KeyValueCache, mocker: MockerFixture):
        cache.set("reconnect_test", "before")
        cache.close()
        cache.set("reconnect_test2", "after")
        assert cache.get("reconnect_test2") == "after"
