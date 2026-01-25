try:
    from compression import zstd
except ImportError:
    from backports import zstd

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class ZStdCompressor(BaseCompressor):
    def _compress(self, data: bytes) -> bytes:
        return zstd.compress(data)

    def decompress(self, data: bytes) -> bytes:
        try:
            return zstd.decompress(data)
        except zstd.ZstdError as e:
            raise CompressorError from e
