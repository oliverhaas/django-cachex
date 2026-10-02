# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause

from django.core.exceptions import ImproperlyConfigured

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

    def _check_level(self, level: object, lowest: int | None, highest: int) -> None:
        """Raise ``ImproperlyConfigured`` unless ``level`` is an int from ``lowest`` (``None``: open) to ``highest``.

        Subclasses call it from ``__init__``: the library rejects a bad level only on the first write.
        """
        if (
            isinstance(level, int)
            and not isinstance(level, bool)
            and (lowest is None or lowest <= level)
            and level <= highest
        ):
            return
        bounds = f"at most {highest}" if lowest is None else f"from {lowest} to {highest}"
        msg = f"{type(self).__name__} level must be an int {bounds}, got {level!r}"
        raise ImproperlyConfigured(msg)

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
            received = f"{len(data)} bytes" if isinstance(data, bytes | bytearray | memoryview) else type(data).__name__
            msg = f"{type(self).__name__} could not decompress {received}: {e!r}"
            raise CompressorError(msg) from e

    def _compress(self, data: bytes) -> bytes:
        raise NotImplementedError

    def _decompress(self, data: bytes) -> bytes:
        raise NotImplementedError
