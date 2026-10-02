import pickle
from typing import Any

from django.core.exceptions import ImproperlyConfigured

from django_cachex.serializers.base import BaseSerializer


class PickleSerializer(BaseSerializer):
    """Pickle-based serializer matching Django's RedisSerializer interface."""

    def __init__(self, *, protocol: int | None = None) -> None:
        self.protocol = protocol if protocol is not None else pickle.DEFAULT_PROTOCOL
        # A negative protocol selects pickle.HIGHEST_PROTOCOL.
        if (
            isinstance(self.protocol, bool)
            or not isinstance(self.protocol, int)
            or self.protocol > pickle.HIGHEST_PROTOCOL
        ):
            msg = f"{type(self).__name__} protocol must be an int of at most {pickle.HIGHEST_PROTOCOL}, got {self.protocol!r}"
            raise ImproperlyConfigured(msg)

    def _dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, self.protocol)

    def _loads(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301
