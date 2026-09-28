"""Tests for all compressor implementations."""

import pytest

from django_cachex.compressors.gzip import GzipCompressor
from django_cachex.compressors.lz4 import Lz4Compressor
from django_cachex.compressors.lzma import LzmaCompressor
from django_cachex.compressors.zlib import ZlibCompressor
from django_cachex.compressors.zstd import ZstdCompressor
from django_cachex.exceptions import CompressorError

ALL_COMPRESSORS = [
    GzipCompressor,
    Lz4Compressor,
    LzmaCompressor,
    ZlibCompressor,
    ZstdCompressor,
]


@pytest.fixture(params=ALL_COMPRESSORS, ids=lambda c: c.__name__)
def compressor(request):
    """Parametrized fixture that yields each compressor instance."""
    return request.param()


# Large enough to exceed min_length (256 bytes)
LARGE_DATA = b"Hello, World! " * 50  # 700 bytes
SMALL_DATA = b"tiny"  # Below min_length


def test_roundtrip_large_data(compressor):
    compressed = compressor.compress(LARGE_DATA)
    decompressed = compressor.decompress(compressed)
    assert decompressed == LARGE_DATA


def test_roundtrip_binary_data(compressor):
    data = bytes(range(256)) * 4  # 1024 bytes of all byte values
    compressed = compressor.compress(data)
    decompressed = compressor.decompress(compressed)
    assert decompressed == data


def test_roundtrip_repeated_data(compressor):
    data = b"\x00" * 1000  # Highly compressible
    compressed = compressor.compress(data)
    decompressed = compressor.decompress(compressed)
    assert decompressed == data


def test_compression_reduces_size(compressor):
    data = b"abcdefghij" * 100  # 1000 bytes, highly compressible
    compressed = compressor.compress(data)
    assert len(compressed) < len(data)


def test_small_data_returned_as_is(compressor):
    result = compressor.compress(SMALL_DATA)
    assert result == SMALL_DATA


def test_data_at_boundary(compressor):
    data = b"x" * 256  # Exactly at default min_length
    result = compressor.compress(data)
    # At boundary, should still be returned as-is (> not >=)
    assert result == data


def test_data_above_boundary(compressor):
    data = b"x" * 257  # Just above min_length
    compressed = compressor.compress(data)
    # Should be compressed (though result may differ per compressor)
    decompressed = compressor.decompress(compressed)
    assert decompressed == data


def test_custom_min_length():
    comp = ZlibCompressor(min_length=10)
    assert comp.min_length == 10
    data = b"x" * 20
    compressed = comp.compress(data)
    decompressed = comp.decompress(compressed)
    assert decompressed == data


def test_invalid_data_raises_error(compressor):
    with pytest.raises(CompressorError):
        compressor.decompress(b"this is not compressed data!!")


def test_decompress_non_bytes_raises_compressor_error(compressor):
    with pytest.raises(CompressorError, match="could not decompress object"):
        compressor.decompress(object())


def test_custom_level(compressor):
    cls = type(compressor)
    custom = cls(level=1)
    assert custom.level == 1
    # Roundtrip still works at custom level.
    data = b"abcdefghij" * 100
    assert custom.decompress(custom.compress(data)) == data
