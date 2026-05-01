# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

from typing import Any

import msgpack

from django_cachex.serializers.base import BaseSerializer


class MessagePackSerializer(BaseSerializer):
    """MessagePack-based serializer for efficient binary serialization."""

    def _dumps(self, obj: Any) -> bytes:
        return msgpack.dumps(obj)

    def _loads(self, data: bytes) -> Any:
        return msgpack.loads(data, raw=False, strict_map_key=False)
