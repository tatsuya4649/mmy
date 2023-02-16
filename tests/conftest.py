import asyncio
import os

import pytest

from mmy.log import init_log
from mmy.parse import MmyMySQLInfo, MmyYAML, parse_yaml

from .test_etcd import startup_etcd_cluter

init_log()

from mmy.mysql.client import TableName

TEST_TABLE1: TableName = TableName("user")
TEST_TABLE2: TableName = TableName("post")


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop

    loop.close()


def _mysql_info() -> MmyMySQLInfo:
    resource_dirs = os.path.join(os.path.dirname(__file__), "resources")
    resource_test_yaml = os.path.join(resource_dirs, "test.yaml")
    mmy_yaml: MmyYAML = parse_yaml(resource_test_yaml)
    return mmy_yaml.mysql


@pytest.fixture
def test_mysql_info():
    yield _mysql_info()
