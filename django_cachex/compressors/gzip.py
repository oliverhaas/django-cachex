# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project. The code similarity
# is somewhat coincidental given the minimal nature of wrapping gzip.

import gzip

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class GzipCompressor(BaseCompressor):
    def _compress(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def decompress(self, data: bytes) -> bytes:
        try:
            return gzip.decompress(data)
        except Exception as e:
            raise CompressorError from e
