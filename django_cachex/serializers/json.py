# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from django_cachex.serializers.base import BaseSerializer


class JSONSerializer(BaseSerializer):
    """JSON serializer using ``DjangoJSONEncoder`` (handles datetime, UUID, etc.)."""

    encoder_class = DjangoJSONEncoder

    def _dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, cls=self.encoder_class).encode()

    def _loads(self, data: bytes) -> Any:
        return json.loads(data.decode())
