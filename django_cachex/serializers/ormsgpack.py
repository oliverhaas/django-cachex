from typing import Any

import ormsgpack

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class OrMessagePackSerializer(BaseSerializer):
    """MessagePack serializer backed by ormsgpack (Rust)."""

    def dumps(self, obj: Any) -> bytes | int:
        try:
            return ormsgpack.packb(obj)
        except Exception as e:
            raise SerializerError from e

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return ormsgpack.unpackb(data)
        except Exception as e:
            raise SerializerError from e
