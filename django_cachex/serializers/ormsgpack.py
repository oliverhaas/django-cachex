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
        """Decode one msgpack value; trailing bytes raise, so a serializer chain falls through to pickle or JSON."""
        # The option is needed on the way out too: unpackb rejects a non-str
        # map key without it, so packing and unpacking must agree.
        obj = ormsgpack.unpackb(data, option=ormsgpack.OPT_NON_STR_KEYS)
        # unpackb ignores trailing bytes. Data that repacks to itself has none;
        # other data has none exactly when it no longer decodes without its last byte.
        if _repack(obj) != data and _decodes(memoryview(data)[:-1]):
            msg = "trailing data after the msgpack value"
            raise ValueError(msg)
        return obj


def _repack(obj: Any) -> bytes | None:
    try:
        return ormsgpack.packb(obj, option=ormsgpack.OPT_NON_STR_KEYS)
    except ormsgpack.MsgpackEncodeError:
        return None


def _decodes(data: memoryview) -> bool:
    try:
        ormsgpack.unpackb(data, option=ormsgpack.OPT_NON_STR_KEYS)
    except ormsgpack.MsgpackDecodeError:
        return False
    return True
