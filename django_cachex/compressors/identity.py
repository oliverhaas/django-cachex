from django_cachex.compressors.base import BaseCompressor


class IdentityCompressor(BaseCompressor):
    def _compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data
