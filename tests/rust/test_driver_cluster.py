from django_cachex._driver import RustValkeyDriver
from tests.fixtures.containers import CLUSTER_NODE_COUNT


def _cluster_urls(host: str, port: int) -> list[str]:
    return [f"redis://{host}:{port + i}" for i in range(CLUSTER_NODE_COUNT)]


def test_connect_cluster_and_round_trip(cluster_container):
    host, port = cluster_container
    driver = RustValkeyDriver.connect_cluster(_cluster_urls(host, port))
    driver.flushdb()

    driver.set("a", b"1")
    driver.set("b", b"2")
    driver.set("c", b"3")

    assert driver.get("a") == b"1"
    assert driver.get("b") == b"2"
    assert driver.get("c") == b"3"


def test_cluster_get_missing_returns_none(cluster_container):
    host, port = cluster_container
    driver = RustValkeyDriver.connect_cluster(_cluster_urls(host, port))
    driver.flushdb()

    assert driver.get("nope") is None
