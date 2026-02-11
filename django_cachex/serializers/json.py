# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.base import BaseSerializer


class JSONSerializer(BaseSerializer):
    """JSON-based serializer using Django's DjangoJSONEncoder."""

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
