from lz4 import frame as lz4_frame

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class Lz4Compressor(BaseCompressor):
    def _compress(self, data: bytes) -> bytes:
        return lz4_frame.compress(data)

    def decompress(self, data: bytes) -> bytes:
        try:
            return lz4_frame.decompress(data)
        except Exception as e:
            raise CompressorError from e
