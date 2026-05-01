# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

from typing import Any

from django_cachex.exceptions import SerializerError


class BaseSerializer:
    """Base class for cache value serializers.

    Subclasses implement ``_dumps`` and ``_loads``. Plain ints pass through
    ``loads`` unchanged so Redis ``INCR`` results don't need re-decoding.
    """

    def dumps(self, obj: Any) -> bytes:
        try:
            return self._dumps(obj)
        except Exception as e:
            raise SerializerError from e

    def loads(self, data: bytes | int) -> Any:
        if isinstance(data, int):
            return data
        try:
            return self._loads(data)
        except Exception as e:
            raise SerializerError from e

    def _dumps(self, obj: Any) -> bytes:
        raise NotImplementedError

    def _loads(self, data: bytes) -> Any:
        raise NotImplementedError
