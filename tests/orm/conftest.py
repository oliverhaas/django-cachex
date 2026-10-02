"""Pytest configuration for the ORM cache tests."""

import os
from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from django.conf import settings

from tests.fixtures.containers import REDIS_IMAGE
from tests.orm.utils import override_orm_settings

if TYPE_CHECKING:
    from collections.abc import Generator

POSTGRES_IMAGE = os.environ.get("CACHEX_ORM_TEST_POSTGRES_IMAGE", "postgres:18")


@pytest.fixture(scope="session")
def _orm_postgres() -> Generator[None]:
    """Start PostgreSQL for the default database unless it already has a host."""
    params = settings.DATABASES["default"]
    if params["ENGINE"] != "django.db.backends.postgresql" or params["HOST"]:
        yield
        return

    from testcontainers.community.postgres import PostgresContainer

    container = PostgresContainer(
        POSTGRES_IMAGE,
        username=params["USER"],
        password=params["PASSWORD"],
        dbname=params["NAME"],
        driver=None,
    )
    container.start()
    try:
        params["HOST"] = container.get_container_host_ip()
        params["PORT"] = str(container.get_exposed_port(5432))
        # Child processes import the settings afresh.
        os.environ["CACHEX_ORM_TEST_PG_HOST"] = params["HOST"]
        os.environ["CACHEX_ORM_TEST_PG_PORT"] = params["PORT"]
        yield
    finally:
        with suppress(Exception):
            container.stop()


@pytest.fixture(scope="session", autouse=True)
def django_db_modify_db_settings(
    django_db_modify_db_settings_parallel_suffix: None,
    _orm_postgres: None,
    request: pytest.FixtureRequest,
) -> None:
    """Point the Redis cache aliases at a container before anything opens a connection."""
    redis_aliases = [alias for alias, params in settings.CACHES.items() if params.get("LOCATION") == ""]
    if not redis_aliases:
        return
    factory, _ = request.getfixturevalue("redis_container_factory")
    host, port = factory(REDIS_IMAGE)
    location = f"redis://{host}:{port}/0"
    os.environ["CACHEX_ORM_TEST_REDIS_URL"] = location
    for alias in redis_aliases:
        settings.CACHES[alias]["LOCATION"] = location


@pytest.fixture(params=[True, False], ids=["final_sql_check", "no_final_sql_check"])
def final_sql_check(request: pytest.FixtureRequest) -> Generator[bool]:
    """Run the test with the ``FINAL_SQL_CHECK`` setting on, then off."""
    with override_orm_settings(FINAL_SQL_CHECK=request.param):
        yield request.param
