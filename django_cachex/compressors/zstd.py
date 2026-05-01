from compression import zstd

from django_cachex.compressors.base import BaseCompressor


class ZstdCompressor(BaseCompressor):
    """Zstandard compressor with configurable compression level."""

    level: int = 3

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level

    def _compress(self, data: bytes) -> bytes:
        return zstd.compress(data, level=self.level)

    def _decompress(self, data: bytes) -> bytes:
        return zstd.decompress(data)
