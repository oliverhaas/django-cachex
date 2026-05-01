from lz4 import frame as lz4_frame

from django_cachex.compressors.base import BaseCompressor


class Lz4Compressor(BaseCompressor):
    """LZ4 compressor with configurable compression level."""

    level: int = 0

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level

    def _compress(self, data: bytes) -> bytes:
        return lz4_frame.compress(data, compression_level=self.level)

    def _decompress(self, data: bytes) -> bytes:
        return lz4_frame.decompress(data)
