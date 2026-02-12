import pickle
from typing import Any

from django.core.exceptions import ImproperlyConfigured

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class PickleSerializer(BaseSerializer):
    """Pickle-based serializer matching Django's RedisSerializer interface."""

    def __init__(self, *, protocol: int | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        if protocol is None:
            protocol = pickle.DEFAULT_PROTOCOL

        if protocol > pickle.HIGHEST_PROTOCOL:
            msg = f"protocol can't be higher than pickle.HIGHEST_PROTOCOL: {pickle.HIGHEST_PROTOCOL}"
            raise ImproperlyConfigured(msg)

        self.protocol = protocol

    def dumps(self, obj: Any) -> bytes | int:
        return pickle.dumps(obj, self.protocol)

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return pickle.loads(data)  # noqa: S301
        except Exception as e:
            raise SerializerError from e
