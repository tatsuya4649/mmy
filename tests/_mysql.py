import os

import aiomysql
import pytest
from mmy.mysql.client import MySQLAuthInfo, TableName
from mmy.parse import MmyYAML, parse_yaml

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


async def delete_all_table(
    container,
    mmy_info: MmyYAML,
):
    async with aiomysql.connect(
        host=str(container.value.host),
        port=container.value.port,
        user=ROOT_USERINFO.user,
        password=ROOT_USERINFO.password,
        db=mmy_info.mysql.db,
    ) as connect:
        cur = await connect.cursor()
        await cur.execute("DELETE FROM %s" % (TEST_TABLE2))
        await connect.commit()

        cur = await connect.cursor()
        await cur.execute("ALTER TABLE `%s`  AUTO_INCREMENT = 1" % (TEST_TABLE2))
        await connect.commit()
