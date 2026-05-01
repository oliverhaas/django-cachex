from typing import Any

import ormsgpack

from django_cachex.serializers.base import BaseSerializer


class OrMessagePackSerializer(BaseSerializer):
    """MessagePack serializer backed by ormsgpack (Rust)."""

    def _dumps(self, obj: Any) -> bytes:
        return ormsgpack.packb(obj)

    def _loads(self, data: bytes) -> Any:
        return ormsgpack.unpackb(data)
