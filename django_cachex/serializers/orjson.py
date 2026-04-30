from typing import Any

import orjson

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class OrjsonSerializer(BaseSerializer):
    """JSON serializer backed by orjson (Rust).

    Natively serializes datetime, date, UUID, dataclasses, and enums.
    Decimal and other arbitrary types raise SerializerError unless caller pre-converts.
    """

    def dumps(self, obj: Any) -> bytes | int:
        try:
            return orjson.dumps(obj)
        except Exception as e:
            raise SerializerError from e

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return orjson.loads(data)
        except Exception as e:
            raise SerializerError from e
