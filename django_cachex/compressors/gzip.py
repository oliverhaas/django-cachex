# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

import gzip

from django_cachex.compressors.base import BaseCompressor


class GzipCompressor(BaseCompressor):
    """gzip compressor with configurable compression level."""

    level: int = 9

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level

    def _compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self.level)

    def _decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)
