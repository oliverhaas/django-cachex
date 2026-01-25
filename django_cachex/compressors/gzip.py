import gzip

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class GzipCompressor(BaseCompressor):
    def _compress(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def decompress(self, data: bytes) -> bytes:
        try:
            return gzip.decompress(data)
        except gzip.BadGzipFile as e:
            raise CompressorError from e
