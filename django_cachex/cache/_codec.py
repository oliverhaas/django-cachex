"""The serializer and compressor stack of the backends that store encoded bytes (RESP and S3)."""

from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_cachex.exceptions import CompressorError, SerializerError


def _load_codec(config: str | type | Any) -> Any:
    """Resolve a serializer/compressor config (dotted path, class or instance) to an instance."""
    if isinstance(config, str):
        config = import_string(config)
    if callable(config):
        return config()
    return config


class ValueCodec:
    """``OPTIONS["serializer"]`` and ``OPTIONS["compressor"]``, with the fallback chain on read.

    The first serializer and compressor write; reads try each in turn, so a
    list lets a cache move to a new format while old values still load.
    """

    _serializers: list[Any]
    _compressors: list[Any]

    @staticmethod
    def _create_serializers(config: Any) -> list[Any]:
        if config is None:
            config = "django_cachex.serializers.pickle.PickleSerializer"
        items: list[Any] = list(config) if isinstance(config, (list, tuple)) else [config]
        if not items:
            msg = "OPTIONS['serializer'] must not be empty; configure at least one serializer or omit it"
            raise ImproperlyConfigured(msg)
        return [_load_codec(item) for item in items]

    @staticmethod
    def _create_compressors(config: Any) -> list[Any]:
        if config is None:
            return []
        items: list[Any] = list(config) if isinstance(config, (list, tuple)) else [config]
        return [_load_codec(item) for item in items]

    def _decompress(self, value: bytes) -> bytes:
        """Decompress with the first compressor that accepts ``value``; payloads under ``min_length`` pass through."""
        for compressor in self._compressors:
            try:
                return compressor.decompress(value)
            except CompressorError:
                continue
        return value

    def _deserialize(self, value: bytes) -> Any:
        """Deserialize with fallback support for multiple serializers."""
        last_error: SerializerError | None = None
        for serializer in self._serializers:
            try:
                return serializer.loads(value)
            except SerializerError as e:
                last_error = e
                continue
        if last_error is not None:
            raise last_error
        raise SerializerError("No serializers configured")
