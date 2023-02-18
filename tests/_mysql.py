import os

import pytest

from mmy.mysql.client import MySQLAuthInfo, TableName
from mmy.parse import MmyYAML, parse_yaml

TEST_TABLE1: TableName = TableName("user")
TEST_TABLE2: TableName = TableName("post")


def _mmy_info():
    resource_dirs = os.path.join(os.path.dirname(__file__), "resources")
    resource_test_yaml = os.path.join(resource_dirs, "test.yaml")
    mmy_yaml: MmyYAML = parse_yaml(resource_test_yaml)
    return mmy_yaml


@pytest.fixture(scope="session")
def mmy_info():
    yield _mmy_info()


ROOT_USERINFO = MySQLAuthInfo(
    user="root",
    password="root",
)
