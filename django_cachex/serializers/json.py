import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class JSONSerializer(BaseSerializer):
    """JSON-based serializer using Django's DjangoJSONEncoder.

    Serializes values to JSON format, which is human-readable and interoperable
    but limited to JSON-compatible types (strings, numbers, lists, dicts, bools, None).

    By default uses Django's DjangoJSONEncoder which adds support for:
    - datetime, date, time objects
    - timedelta (as total seconds)
    - Decimal (as string)
    - UUID (as string)
    - Promise (lazy strings)

    Attributes:
        encoder_class: The JSON encoder class to use. Defaults to DjangoJSONEncoder.
            Can be overridden by subclasses for custom encoding.

    Example:
        Configure in Django settings::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": "redis://localhost:6379/1",
                    "OPTIONS": {
                        "serializer": "django_cachex.serializers.json.JSONSerializer",
                    }
                }
            }
    """

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
