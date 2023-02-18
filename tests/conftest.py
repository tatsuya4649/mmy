import asyncio
import logging

import pytest

# Fixtures
from .test_etcd import startup_etcd_cluter, up_etcd_docker_containers
from .test_mysql import fix_up_mysql_docker_containers


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop

    loop.close()
