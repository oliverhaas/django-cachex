"""Each adapter against each RESP server image.

``resp_images`` chooses the container image and ``resp_adapter`` the client
library, so the two together cover the cross product. The topology is pinned
to standalone: the cluster container always runs grokzen/redis-cluster and
ignores the requested image, and sentinel adds no parsing behaviour.
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


@pytest.mark.parametrize("topology", ["default"], indirect=True)
class TestClientLibraries:
    def test_set_get_across_images(self, cache: RespCache, resp_images: tuple[str, str]):
        image, client_library = resp_images

        cache.set("test_key", "hello")
        assert cache.get("test_key") == "hello", f"image={image}, client_library={client_library}"

        cache.set("int_key", 42)
        assert cache.get("int_key") == 42, f"image={image}, client_library={client_library}"

    def test_native_parser_round_trips_nested_values(self, cache: RespCache, native_parser: bool):
        """hiredis / libvalkey must decode what the pure-Python parser decodes."""
        parser = "native" if native_parser else "python"

        cache.set("test_key", {"nested": {"data": [1, 2, 3]}})
        assert cache.get("test_key") == {"nested": {"data": [1, 2, 3]}}, f"parser={parser}"
