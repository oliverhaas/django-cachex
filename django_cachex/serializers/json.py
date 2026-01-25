import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class JSONSerializer(BaseSerializer):
    encoder_class = DjangoJSONEncoder

    def dumps(self, obj: Any) -> bytes | int:
        return json.dumps(obj, cls=self.encoder_class).encode()

    def loads(self, data: bytes | int) -> Any:
        try:
            if isinstance(data, int):
                return data
            return json.loads(data.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializerError from e
