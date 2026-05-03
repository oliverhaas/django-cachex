"""Cluster cache backend base class.

Driver-agnostic cluster behavior; per-driver concrete subclasses live in
:mod:`django_cachex.cache.valkey_py` (``valkey-py``) and
:mod:`django_cachex.cache.redis_py` (``redis-py``).
"""

from typing import TYPE_CHECKING

from django_cachex.cache.resp import RespCache

if TYPE_CHECKING:
    from django_cachex.adapters.pipeline import Pipeline


class KeyValueClusterCache(RespCache):
    """Cluster cache backend base class.

    Extends ``RespCache`` with cluster-specific behaviour (no
    transactions on pipelines). Subclasses set ``_adapter_class`` to
    their specific cluster adapter.
    """

    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        """Create a pipeline. Cluster pipelines never use transactions."""
        return super().pipeline(transaction=False, version=version)


__all__ = ["KeyValueClusterCache"]
