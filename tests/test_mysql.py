import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Coroutine

import pytest
import pytest_asyncio
from pymysql.err import OperationalError
from python_on_whales import docker
from rich import print
from src.mysql import (
    INSERT_ONCE_LIMIT,
    MySQLClient,
    MySQLClientTooManyInsertAtOnce,
    MySQLColumns,
    MySQLKeys,
    _Extra,
)


@dataclass
class container:
    container_name: str
    service_name: str
    port: int


class DockerMySQL(Enum):
    MySQL1 = container(
        service_name="mysql1",
        container_name="mmysql1",
        port=10001,
    )
    MySQL2 = container(
        service_name="mysql2",
        container_name="mmysql2",
        port=10002,
    )


class TMySQLClient(MySQLClient):
    GENERATE_RANDOM_DATA_REPEAT_COUNT: int = 10
    GENERATE_RANDOM_DATA_MIN_COUNT: int = 10000

    def __init__(
        self,
        host="127.0.0.1",
        port=10001,
        user="root",
        password="root",
        db="test",
        connect_timeout=3,
        **kwargs,
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            connect_timeout=connect_timeout,
            **kwargs,
        )

    async def _generate_random(self, call: str, *args):
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                sargs = list(map(lambda x: str(x), args))
                await cursor.execute(f"CALL {call}({','.join(sargs)})")

            await connect.commit()

    async def generate_random_user(self):
        await self._generate_random(
            "generate_random_user",
            self.GENERATE_RANDOM_DATA_REPEAT_COUNT,
            self.GENERATE_RANDOM_DATA_MIN_COUNT,
        )

    async def generate_random_post(self):
        await self._generate_random(
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


@pytest_asyncio.fixture(scope="class", autouse=True)
async def up_mysql():
    mcs = [i.value.service_name for i in list(DockerMySQL)]
    _run_containers = docker.compose.ps(mcs)
    docker_mysqls = [i.value for i in list(DockerMySQL)]
    _r = [i.name for i in _run_containers]
    for mysql in docker_mysqls:
        if not mysql.container_name in _r:
            docker.compose.up([mysql.service_name], detach=True)

    RETRY_COUNT: int = 10
    for _ in range(RETRY_COUNT):
        for mysql in docker_mysqls:
            _, port = docker.compose.port(mysql.service_name, 3306)

            try:
                cli = TMySQLClient(
                    port=port,
                )
                await cli.ping()
            except OperationalError as e:
                errcode = e.args[0]
                # Can't connect to MySQL server on '127.0.0.1'
                if errcode == 2003:
                    await asyncio.sleep(10)
                else:
                    raise e
                break
        else:
            # OK, PING with all MySQL containers
            break

        # PING error, retry
        continue
    else:

        class DockerStartupError(RuntimeError):
            pass

        # Reach max retry count
        raise DockerStartupError

    yield


@pytest_asyncio.fixture(scope="function", autouse=True)
async def insert_random_data():
    cli = TMySQLClient()
    await cli.generate_random_post()
    await cli.generate_random_user()


TEST_TABLE1: str = "user"


@pytest.mark.asyncio
async def test_check_delete_with_consistent_hash():
    start = "0" * 16
    end = "f" * 16

    cli = TMySQLClient()
    await cli.consistent_hashing_delete(
        table=TEST_TABLE1,
        start=start,
        end=end,
    )
    await cli.optimize_table(
        table=TEST_TABLE1,
    )

    rows = await cli.get_table_rows(
        table=TEST_TABLE1,
    )
    """

    2 is 0000000000000000 and ffffffffffffffff because it is not deleted.

    """
    assert rows <= 2


@pytest.mark.asyncio
async def test_primary_keys():
    cli = TMySQLClient()
    keys: list[MySQLKeys] = await cli.primary_keys_info(
        table=TEST_TABLE1,
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
    before_rows = await cli.get_table_rows(TEST_TABLE1)
    await cli.insert(TEST_TABLE1, datas)
    after_rows = await cli.get_table_rows(TEST_TABLE1)
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
        await cli.insert(TEST_TABLE1, datas)


@pytest.mark.asyncio
async def test_columns_info():
    cli = TMySQLClient()
    columns: list[MySQLColumns] = await cli.columns_info(TEST_TABLE1)
    assert isinstance(columns, list)
    for column in columns:
        assert isinstance(column, MySQLColumns)


@pytest.mark.asyncio
async def test_exclude_auto_increment():
    cli = TMySQLClient()
    columns: list[MySQLColumns] = await cli.columns_info(TEST_TABLE1)
    _excluded: list[MySQLColumns] = cli.exclude_auto_increment_columns(columns)
    assert isinstance(columns, list)
    assert isinstance(_excluded, list)
    for column in _excluded:
        assert isinstance(column, MySQLColumns)
        assert column.Extra is not _Extra.AUTO_INCREMENT
    assert len(columns) >= len(_excluded)
