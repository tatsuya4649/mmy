import asyncio
import ipaddress
from enum import Enum
from typing import Coroutine

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.mysql.client import (
    INSERT_ONCE_LIMIT,
    MySQLClient,
    MySQLClientTooManyInsertAtOnce,
    MySQLColumns,
    MySQLKeys,
    TableName,
    _Extra,
)
from pymysql.err import OperationalError
from python_on_whales import docker
from python_on_whales.exceptions import NoSuchContainer
from rich import print

from ._mysql import ROOT_USERINFO, TEST_TABLE2, _mmy_info
from .docker import DockerStartupError, container


class DockerMySQL(Enum):
    MySQL1 = container(
        service_name="mysql1",
        container_name="mmy1",
        host=ipaddress.ip_address("127.0.0.1"),
        port=10001,
    )
    MySQL2 = container(
        service_name="mysql2",
        container_name="mmy2",
        host=ipaddress.ip_address("127.0.0.1"),
        port=10002,
    )
    MySQL3 = container(
        service_name="mysql3",
        container_name="mmy3",
        host=ipaddress.ip_address("127.0.0.1"),
        port=10003,
    )
    MySQL4 = container(
        service_name="mysql4",
        container_name="mmy4",
        host=ipaddress.ip_address("127.0.0.1"),
        port=10004,
    )
    MySQL5 = container(
        service_name="mysql5",
        container_name="mmy5",
        host=ipaddress.ip_address("127.0.0.1"),
        port=10005,
    )


DEFAULT = 5


def get_mysql_docker_for_test(count: int = DEFAULT):
    if count > len(list(DockerMySQL)):
        raise ValueError

    return list(DockerMySQL)[:count]


class TMySQLClient(MySQLClient):
    GENERATE_RANDOM_DATA_REPEAT_COUNT: int = 20
    GENERATE_RANDOM_DATA_MIN_COUNT: int = 10000

    def __init__(
        self,
        host="127.0.0.1",
        port=10001,
        connect_timeout=3,
        **kwargs,
    ):
        _mmy = _mmy_info()
        super().__init__(
            host=host,
            port=port,
            auth=_mmy.mysql,
            connect_timeout=connect_timeout,
            **kwargs,
        )

    async def _generate_random(self, table: TableName, call: str, *args):
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                sargs = list(map(lambda x: str(x), args))
                await cursor.execute(f"CALL {call}({','.join(sargs)})")

            await connect.commit()

    async def generate_random_post(self):
        await self._generate_random(
            TEST_TABLE2,
            "generate_random_post",
            self.GENERATE_RANDOM_DATA_REPEAT_COUNT,
            self.GENERATE_RANDOM_DATA_MIN_COUNT,
        )

    def start_docker(self, container: DockerMySQL):
        docker.compose.start([container.value.service_name])

    def stop_docker(self, container: DockerMySQL):
        docker.compose.stop([container.value.service_name])

    def is_stopped_docker(self, container: DockerMySQL) -> bool:
        mcs = docker.compose.ps([container.value.service_name])
        return container.value.container_name not in [i.name for i in mcs]


async def up_mysql_docker_container(container: DockerMySQL):
    docker.compose.up(
        container.value.service_name,
        detach=True,
    )
    RETRY_COUNT: int = 60
    for _ in range(RETRY_COUNT):
        try:
            cli = TMySQLClient(
                port=container.value.port,
            )
            await cli.ping()
            break
        except OperationalError as e:
            errcode = e.args[0]
            # Can't connect to MySQL server on '127.0.0.1'
            if errcode == 2003:
                await asyncio.sleep(10)
                # PING error, retry
                continue
            else:
                raise e
    else:
        raise DockerStartupError
    pass


async def up_mysql_docker_containers():
    for mysql in get_mysql_docker_for_test():
        await up_mysql_docker_container(mysql)

    return


@pytest_asyncio.fixture(scope="session", autouse=True)
async def fix_up_mysql_docker_containers():
    for mysql in get_mysql_docker_for_test():
        await up_mysql_docker_container(mysql)

    yield


@pytest.fixture
def fix_down_all_mysql_docker_containers():
    service_names = [i.value.service_name for i in DockerMySQL]
    ps = docker.compose.ps(service_names)
    for ps_info, service_name in zip(ps, service_names):
        if ps_info.state.running is False:
            raise RuntimeError(f'Already not running: "{service_name}"')

        try:
            docker.compose.stop(services=[service_name])
        except NoSuchContainer:
            continue

        assert ps_info.state.running is False

    yield


def down_all_mysql_docker_containers(volumes: bool = False):
    print("Delete all MySQL docker containers")
    for container in list(DockerMySQL):
        container_name: str = container.value.container_name
        try:
            docker.stop([container_name])
        except NoSuchContainer:
            continue

    if volumes:
        print("Delete all volumes of MySQL's container")
        delete_volumes = list()
        for container in list(DockerMySQL):
            try:
                inspect = docker.container.inspect(container_name)
            except NoSuchContainer:
                continue
            assert inspect.state.status == "exited"
            for mount in inspect.mounts:
                if mount.type == "volume":
                    delete_volumes.append(mount.name)
        docker.system.prune(all=False, volumes=True)
        for volume in delete_volumes:
            docker.volume.remove(volume)

    return


async def delete_all_table(
    container,
    mmy_info,
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
        await cur.execute("OPTIMIZE TABLE %s" % (TEST_TABLE2))
        await connect.commit()


async def random_generate_data(
    container,
    mmy_info,
    genereate_repeate: int,
    min_count: int,
):
    async with aiomysql.connect(
        host=str(container.value.host),
        port=container.value.port,
        user=ROOT_USERINFO.user,
        cursorclass=DictCursor,
        password=ROOT_USERINFO.password,
        db=mmy_info.mysql.db,
    ) as connect:
        # Delete all table data from container
        await delete_all_table(container, mmy_info)

        cur = await connect.cursor()
        await cur.execute(
            "CALL generate_random_post(%d, %d)" % (genereate_repeate, min_count)
        )
        await connect.commit()
        return


@pytest_asyncio.fixture(scope="function", autouse=True)
async def insert_random_data():
    cli = TMySQLClient()
    await cli.generate_random_post()


@pytest.mark.asyncio
async def test_check_delete_with_consistent_hash():
    start = "0" * 16
    end = "f" * 16

    cli = TMySQLClient()
    await cli.consistent_hashing_delete(
        table=TEST_TABLE2,
        start=start,
        end=end,
    )
    await cli.optimize_table(
        table=TEST_TABLE2,
    )

    rows = await cli.get_table_rows(
        table=TEST_TABLE2,
    )
    """

    2 is 0000000000000000 and ffffffffffffffff because it is not deleted.

    """
    assert rows <= 2


@pytest.mark.asyncio
async def test_primary_keys():
    cli = TMySQLClient()
    keys: list[MySQLKeys] = await cli.primary_keys_info(
        table=TEST_TABLE2,
    )
    assert len(keys) == 1
    for key in keys:
        assert key.Key_name == cli.IDENTIFY_PRIMARY


async def expected_2003_error(coroutine: Coroutine[None, None, None]):
    try:
        await coroutine
    except OperationalError as e:
        assert e.args[0] == 2003
        return

    # Not received 2003 error from MySQL
    assert False


@pytest.mark.asyncio
async def test_pause_docker():
    for i in DockerMySQL:
        container = i.value

        cli = TMySQLClient(
            port=container.port,
        )
        try:
            cli.stop_docker(i)
            for _ in range(10):
                if not cli.is_stopped_docker(i):
                    await asyncio.sleep(1)
                    continue

                break
            else:
                raise RuntimeError

            await expected_2003_error(cli.ping())
        finally:
            cli.start_docker(i)


@pytest.mark.asyncio
async def test_insert():
    import hashlib
    import time

    datas = list()
    COUNT = 10
    for _ in range(COUNT):
        now = time.time_ns()
        name = hashlib.md5(str(now).encode("utf-8")).hexdigest()
        datas.append(
            {
                "name": name,
            }
        )

    cli = TMySQLClient()
    before_rows = await cli.get_table_rows(TEST_TABLE2)
    await cli.insert(TEST_TABLE2, datas)
    after_rows = await cli.get_table_rows(TEST_TABLE2)
    assert before_rows + len(datas) == after_rows
    return


@pytest.mark.asyncio
async def test_insert_too_many():
    import hashlib
    import time

    datas = list()
    COUNT = INSERT_ONCE_LIMIT + 1
    for _ in range(COUNT):
        now = time.time_ns()
        name = hashlib.md5(str(now).encode("utf-8")).hexdigest()
        datas.append(
            {
                "name": name,
            }
        )

    cli = TMySQLClient()
    with pytest.raises(MySQLClientTooManyInsertAtOnce):
        await cli.insert(TEST_TABLE2, datas)


@pytest.mark.asyncio
async def test_columns_info():
    cli = TMySQLClient()
    columns: list[MySQLColumns] = await cli.columns_info(TEST_TABLE2)
    assert isinstance(columns, list)
    for column in columns:
        assert isinstance(column, MySQLColumns)


@pytest.mark.asyncio
async def test_exclude_auto_increment():
    cli = TMySQLClient()
    columns: list[MySQLColumns] = await cli.columns_info(TEST_TABLE2)
    _excluded: list[MySQLColumns] = cli.exclude_auto_increment_columns(columns)
    assert isinstance(columns, list)
    assert isinstance(_excluded, list)
    for column in _excluded:
        assert isinstance(column, MySQLColumns)
        assert column.Extra is not _Extra.AUTO_INCREMENT
    assert len(columns) >= len(_excluded)
