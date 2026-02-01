# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

from typing import Any


class BaseCompressor:
    """Base class for cache value compressors.

    Django's redis cache backend does not include compression support.
    This is a django-cachex-ng extension that follows the same pattern as
    serializers: any object with ``compress`` and ``decompress`` methods works.

    Compression is skipped for values smaller than ``min_length`` bytes to avoid
    overhead on small values where compression provides little benefit.

    Args:
        min_length: Minimum value size in bytes before compression is applied.
                    Values smaller than this are returned uncompressed.
                    Defaults to 256 bytes.
    """

    min_length: int = 256

    def __init__(self, *, min_length: int | None = None, **kwargs: Any) -> None:
        if min_length is not None:
            self.min_length = min_length

    def compress(self, data: bytes) -> bytes:
        if len(data) > self.min_length:
            return self._compress(data)
        return data

    def _compress(self, data: bytes) -> bytes:
        raise NotImplementedError

    def decompress(self, data: bytes) -> bytes:
        raise NotImplementedError
