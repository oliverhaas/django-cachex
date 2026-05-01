import pickle
from typing import Any

from django_cachex.serializers.base import BaseSerializer


class PickleSerializer(BaseSerializer):
    """Pickle-based serializer matching Django's RedisSerializer interface."""

    def __init__(self, *, protocol: int | None = None) -> None:
        self.protocol = protocol if protocol is not None else pickle.DEFAULT_PROTOCOL

    def _dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, self.protocol)

    def _loads(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301
