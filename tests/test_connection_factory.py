import pytest

from django_cachex import pool


def test_connection_factory_redefine_from_opts():
    cf = pool.get_connection_factory(
        options={
            "connection_factory": "django_cachex.pool.SentinelConnectionFactory",
            "sentinels": [("127.0.0.1", "26739")],
        },
    )
    assert cf.__class__.__name__ == "SentinelConnectionFactory"


@pytest.mark.parametrize(
    "conn_factory,expected",
    [
        ("django_cachex.pool.SentinelConnectionFactory", pool.SentinelConnectionFactory),
        ("django_cachex.pool.ConnectionFactory", pool.ConnectionFactory),
    ],
)
def test_connection_factory_opts(conn_factory: str, expected):
    cf = pool.get_connection_factory(
        options={
            "connection_factory": conn_factory,
            "sentinels": [("127.0.0.1", "26739")],
        },
    )
    assert isinstance(cf, expected)


def test_connection_factory_no_sentinels():
    with pytest.raises(ValueError):
        pool.get_connection_factory(
            options={
                "connection_factory": "django_cachex.pool.SentinelConnectionFactory",
            },
        )
