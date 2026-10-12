from compression import zstd

from django_cachex.compressors.base import BaseCompressor


class ZstdCompressor(BaseCompressor):
    """Zstandard compressor with configurable compression level.

    Frames carry a content checksum, so a corrupted frame raises instead of
    decoding to wrong bytes.
    """

    level: int = 3

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level
        self._check_level(self.level, *zstd.CompressionParameter.compression_level.bounds())
        self._options: dict[int, int] = {
            zstd.CompressionParameter.compression_level: self.level,
            zstd.CompressionParameter.checksum_flag: 1,
        }

    def _compress(self, data: bytes) -> bytes:
        return zstd.compress(data, options=self._options)

    def _decompress(self, data: bytes) -> bytes:
        return zstd.decompress(data)
