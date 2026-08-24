from typing import Any

import ormsgpack

from django_cachex.serializers.base import BaseSerializer


class OrmsgpackSerializer(BaseSerializer):
    """MessagePack serializer backed by ormsgpack (Rust)."""

    def _dumps(self, obj: Any) -> bytes:
        # Matches MsgpackSerializer's strict_map_key=False, so swapping
        # serializers doesn't change what is cacheable.
        return ormsgpack.packb(obj, option=ormsgpack.OPT_NON_STR_KEYS)

    def _loads(self, data: bytes) -> Any:
        # The option is needed on the way out too: unpackb rejects a non-str
        # map key without it, so packing and unpacking must agree.
        return ormsgpack.unpackb(data, option=ormsgpack.OPT_NON_STR_KEYS)
