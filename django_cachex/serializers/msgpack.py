from typing import Any

import msgpack

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class MSGPackSerializer(BaseSerializer):
    def dumps(self, obj: Any) -> bytes | int:
        return msgpack.dumps(obj)

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return msgpack.loads(data, raw=False)
        except Exception as e:
            raise SerializerError from e
