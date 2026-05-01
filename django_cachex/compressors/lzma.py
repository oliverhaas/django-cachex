import lzma

from django_cachex.compressors.base import BaseCompressor


class LzmaCompressor(BaseCompressor):
    """LZMA compressor with configurable compression level (``preset`` in lzma terms)."""

    level: int = 4

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level

    def _compress(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=self.level)

    def _decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)
