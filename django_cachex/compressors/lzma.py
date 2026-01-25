import lzma
from typing import Any

from django_cachex.compressors.base import BaseCompressor
from django_cachex.exceptions import CompressorError


class LzmaCompressor(BaseCompressor):
    """LZMA compressor with configurable preset level.

    Args:
        preset: Compression level from 0 (fastest) to 9 (best compression).
                Defaults to 4.
    """

    preset: int = 4

    def __init__(self, *, preset: int | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if preset is not None:
            self.preset = preset

    def _compress(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=self.preset)

    def decompress(self, data: bytes) -> bytes:
        try:
            return lzma.decompress(data)
        except lzma.LZMAError as e:
            raise CompressorError from e
