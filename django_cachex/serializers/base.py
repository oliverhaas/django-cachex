# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

from typing import Any


class BaseSerializer:
    """Base class for cache value serializers.

    Duck-type compatible with Django's RedisSerializer (dumps/loads interface).
    """

    def __init__(self, **kwargs: Any) -> None:
        pass

    def dumps(self, obj: Any) -> bytes | int:
        raise NotImplementedError

    def loads(self, data: bytes | int) -> Any:
        raise NotImplementedError
