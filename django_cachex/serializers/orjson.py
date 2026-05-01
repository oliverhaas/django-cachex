from typing import Any

import orjson

from django_cachex.serializers.base import BaseSerializer


class OrjsonSerializer(BaseSerializer):
    """JSON serializer backed by orjson (Rust).

    Natively serializes datetime, date, UUID, dataclasses, and enums.
    Other arbitrary types raise ``SerializerError`` on dumps.
    """

    def _dumps(self, obj: Any) -> bytes:
        return orjson.dumps(obj)

    def _loads(self, data: bytes) -> Any:
        return orjson.loads(data)
