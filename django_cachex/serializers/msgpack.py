# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project. The code similarity
# is somewhat coincidental given the minimal nature of wrapping msgpack.

from typing import Any

import msgpack

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class MessagePackSerializer(BaseSerializer):
    """MessagePack-based serializer for efficient binary serialization.

    MessagePack is a binary format that is more compact and faster than JSON,
    while supporting similar data types. It's a good choice when performance
    and storage efficiency are priorities.

    Requires the ``msgpack`` package to be installed::

        pip install msgpack

    Note:
        MessagePack has different type support than pickle or JSON:
        - Supports: None, bool, int, float, str, bytes, list, dict
        - Does NOT support: datetime, Decimal, custom objects (without extension)
        - For complex types, consider using pickle or a custom serializer

    Example:
        Configure in Django settings::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": "redis://localhost:6379/1",
                    "OPTIONS": {
                        "serializer": "django_cachex.serializers.msgpack.MessagePackSerializer",
                    }
                }
            }
    """

    def dumps(self, obj: Any) -> bytes | int:
        return msgpack.dumps(obj)

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return msgpack.loads(data, raw=False)
        except Exception as e:
            raise SerializerError from e
