from django_cachex.client.pipeline.base import WrappedPipeline
from django_cachex.client.pipeline.hashes import HashPipelineMixin
from django_cachex.client.pipeline.lists import ListPipelineMixin
from django_cachex.client.pipeline.sets import SetPipelineMixin
from django_cachex.client.pipeline.sorted_sets import SortedSetPipelineMixin


class Pipeline(
    ListPipelineMixin,
    SetPipelineMixin,
    HashPipelineMixin,
    SortedSetPipelineMixin,
    WrappedPipeline,
):
    """Full pipeline with all data structure operations.

    Combines the base pipeline with all mixins to provide
    a complete interface for batched Redis operations.
    """


__all__ = [
    "Pipeline",
    "WrappedPipeline",
]
