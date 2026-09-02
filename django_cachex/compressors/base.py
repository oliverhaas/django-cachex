# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

from django_cachex.exceptions import CompressorError


class BaseCompressor:
    """Base class for cache value compressors.

    Subclasses implement ``_compress`` and ``_decompress``. Compression is
    skipped for values up to ``min_length`` bytes (boundary inclusive).
    """

    min_length: int = 256

    def __init__(self, *, min_length: int | None = None) -> None:
        if min_length is not None:
            self.min_length = min_length

    def compress(self, data: bytes) -> bytes:
        if len(data) > self.min_length:
            try:
                return self._compress(data)
            except Exception as e:
                msg = f"{type(self).__name__} could not compress {len(data)} bytes: {e!r}"
                raise CompressorError(msg) from e
        return data

    def decompress(self, data: bytes) -> bytes:
        try:
            return self._decompress(data)
        except Exception as e:
            msg = f"{type(self).__name__} could not decompress {len(data)} bytes: {e!r}"
            raise CompressorError(msg) from e

    def _compress(self, data: bytes) -> bytes:
        raise NotImplementedError

    def _decompress(self, data: bytes) -> bytes:
        raise NotImplementedError
