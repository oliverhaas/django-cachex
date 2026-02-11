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
    """MessagePack-based serializer for efficient binary serialization."""

    def dumps(self, obj: Any) -> bytes | int:
        return msgpack.dumps(obj)

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return msgpack.loads(data, raw=False)
        except Exception as e:
            raise SerializerError from e
