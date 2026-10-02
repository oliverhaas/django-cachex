import lzma

from django.core.exceptions import ImproperlyConfigured

from django_cachex.compressors.base import BaseCompressor


class LzmaCompressor(BaseCompressor):
    """LZMA compressor with configurable compression level (``preset`` in lzma terms)."""

    level: int = 4

    def __init__(self, *, level: int | None = None, min_length: int | None = None) -> None:
        super().__init__(min_length=min_length)
        if level is not None:
            self.level = level
        # lzma has no constant for the highest preset, so 9 comes from its docs.
        preset = self.level
        if isinstance(preset, bool) or not isinstance(preset, int) or not 0 <= preset & ~lzma.PRESET_EXTREME <= 9:
            msg = (
                f"{type(self).__name__} level must be a preset from 0 to 9, "
                f"optionally OR-ed with lzma.PRESET_EXTREME, got {preset!r}"
            )
            raise ImproperlyConfigured(msg)

    def _compress(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=self.level)

    def _decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)
