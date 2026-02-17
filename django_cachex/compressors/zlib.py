import zlib
from typing import Any

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class ZlibCompressor(BaseCompressor):
    """Zlib compressor with configurable compression level."""

    level: int = 6

    def __init__(self, *, level: int | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if level is not None:
            self.level = level

    def _compress(self, data: bytes) -> bytes:
        return zlib.compress(data, self.level)

    def decompress(self, data: bytes) -> bytes:
        try:
            return zlib.decompress(data)
        except Exception as e:
            raise CompressorError from e
