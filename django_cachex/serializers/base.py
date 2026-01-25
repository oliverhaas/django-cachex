from typing import Any


class BaseSerializer:
    """Base class for cache value serializers.

    This interface is duck-type compatible with Django's RedisSerializer from
    ``django.core.cache.backends.redis``. Django does not define a base class;
    any object with ``dumps`` and ``loads`` methods works as a serializer.

    Django's RedisSerializer structure (as of Django 6.0)::

        class RedisSerializer:
            def __init__(self, protocol=None):
                self.protocol = pickle.HIGHEST_PROTOCOL if protocol is None else protocol

            def dumps(self, obj):
                if type(obj) is int:
                    return obj  # Don't pickle integers for atomic incr/decr
                return pickle.dumps(obj, self.protocol)

            def loads(self, data):
                try:
                    return int(data)
                except ValueError:
                    return pickle.loads(data)

    Our serializers accept ``**kwargs`` for configuration (e.g., ``protocol`` for
    pickle version). The ``create_serializer()`` function in ``django_cachex.compat``
    passes the Django cache OPTIONS as kwargs.
    """

    def __init__(self, **kwargs: Any) -> None:
        pass

    def dumps(self, obj: Any) -> bytes | int:
        raise NotImplementedError

    def loads(self, data: bytes | int) -> Any:
        raise NotImplementedError
