import asyncio
import ipaddress
import logging
import random
from enum import Enum

import pytest
import pytest_asyncio
from python_on_whales import Container, docker
from rich import print

from mmy.etcd import (
    EtcdConnectError,
    EtcdPingError,
    MySQLEtcdClient,
    MySQLEtcdData,
    MySQLEtcdDuplicateNode,
)
from mmy.server import Server, State, _Server

from .docker import DockerStartupError, container
from .test_mysql import DockerMySQL

logger = logging.getLogger(__name__)


class DockerEtcd(Enum):
    Etcd1 = container(
        service_name="etcd1",
        container_name="etcd1",
        host=ipaddress.ip_address("172.160.0.11"),
        port=2379,
    )
    Etcd2 = container(
        service_name="etcd2",
        container_name="etcd2",
        host=ipaddress.ip_address("172.160.0.12"),
        port=2379,
    )
    Etcd3 = container(
        service_name="etcd3",
        container_name="etcd3",
        host=ipaddress.ip_address("172.160.0.13"),
        port=2379,
    )
    Etcd4 = container(
        service_name="etcd4",
        container_name="etcd4",
        host=ipaddress.ip_address("172.160.0.14"),
        port=2379,
    )
    Etcd5 = container(
        service_name="etcd5",
        container_name="etcd5",
        host=ipaddress.ip_address("172.160.0.15"),
        port=2379,
    )
    Etcd6 = container(
        service_name="etcd6",
        container_name="etcd6",
        host=ipaddress.ip_address("172.160.0.16"),
        port=2379,
    )
    Etcd7 = container(
        service_name="etcd7",
        container_name="etcd7",
        host=ipaddress.ip_address("172.160.0.17"),
        port=2379,
    )
    Etcd8 = container(
        service_name="etcd8",
        container_name="etcd8",
        host=ipaddress.ip_address("172.160.0.18"),
        port=2379,
    )
    Etcd9 = container(
        service_name="etcd9",
        container_name="etcd9",
        host=ipaddress.ip_address("172.160.0.19"),
        port=2379,
    )
    Etcd10 = container(
        service_name="etcd10",
        container_name="etcd10",
        host=ipaddress.ip_address("172.160.0.20"),
        port=2379,
    )


@pytest.fixture
def docker_etcd_server() -> list[_Server]:
    return [
        _Server(
            host=i.value.host,
            port=i.value.port,
        )
        for i in DockerEtcd
    ]


@pytest_asyncio.fixture(scope="function", autouse=True)
async def startup_etcd_cluter():
    mc = MySQLEtcdClient(
        host=DockerEtcd.Etcd1.value.host,
        port=DockerEtcd.Etcd1.value.port,
    )
    await mc.delete()
    for mysql in DockerMySQL:
        s = _Server(
            host=mysql.value.host,
            port=mysql.value.port,
        )
        try:
            await mc.add_new_node(s)
        except MySQLEtcdDuplicateNode:
            pass


@pytest.fixture
def docker_etcd_client():
    mc = MySQLEtcdClient(
        host=DockerEtcd.Etcd1.value.host,
        port=DockerEtcd.Etcd1.value.port,
    )
    yield mc


class TEtcdClient(MySQLEtcdClient):
    pass


@pytest_asyncio.fixture(scope="session", autouse=True)
async def up_etcd_docker_containers():
    ecs = [i.value.service_name for i in list(DockerEtcd)]
    _run_containers = docker.compose.ps(ecs)
    docker_etcds = [i.value for i in list(DockerEtcd)]
    _r = [i.name for i in _run_containers]
    for etcd in docker_etcds:
        if not etcd.container_name in _r:
            docker.compose.up([etcd.service_name], detach=True)

    RETRY_COUNT: int = 30
    for etcd in docker_etcds:
        print(f'UP "{etcd.service_name}" container')
        for _ in range(RETRY_COUNT):
            try:
                cli = TEtcdClient(
                    host=etcd.host,
                    port=etcd.port,
                    timeout=0.5,
                )
                await cli.ping()
                break
            except EtcdPingError:
                await asyncio.sleep(10)
                # PING error, retry
                continue
        else:
            # Reach max retry count
            raise DockerStartupError

    print("OK, etcd cluster setup")

    yield


def break_etcd_container(container: DockerEtcd):
    ps = docker.compose.ps([container.value.service_name])
    psf: Container = ps[0]
    if psf.state.running is False:
        raise RuntimeError(f'Already not running: "{container.value.service_name}"')
    docker.compose.stop(container.value.service_name)
    assert psf.state.running is False


def restore_container(container: DockerEtcd):
    docker.compose.start(container.value.service_name)
    pass


def factory_etcd_client(etcd: DockerEtcd, timeout=1):
    _etcd = etcd.value
    cli = MySQLEtcdClient(
        host=_etcd.host,
        port=_etcd.port,
        timeout=timeout,
    )
    return cli


@pytest_asyncio.fixture
async def etcd_flush_all_data():
    for i in DockerEtcd:
        try:
            cli = MySQLEtcdClient(
                host=i.value.host,
                port=i.value.port,
            )
            await cli.delete()
        except EtcdConnectError:
            continue

        break
    else:
        raise RuntimeError("Can't communicate with etcd docker")

    yield


async def etcd_put_mysql_container(containers: list[DockerMySQL]):
    logger.info("Put MySQL containers info into etcd")
    for i in DockerEtcd:
        try:
            cli = MySQLEtcdClient(
                host=i.value.host,
                port=i.value.port,
            )
            data = MySQLEtcdData(
                nodes=[
                    Server(
                        host=container.value.host,
                        port=container.value.port,
                        state=State.Run,
                    )
                    for container in containers
                ]
            )
            await cli.put(data=data)
        except EtcdConnectError:
            continue

        break
    else:
        raise RuntimeError("Can't communicate with etcd docker")


class TestMySQLEtcdClient:
    @pytest_asyncio.fixture
    async def get_run_dockers(self):
        _all = self.all_docker_nodes(State.Run)
        yield _all

    @pytest_asyncio.fixture
    async def get_broken_dockers(self):
        _all = self.all_docker_nodes(State.Broken)
        yield _all

    @pytest_asyncio.fixture
    async def get_unknown_dockers(self):
        _all = self.all_docker_nodes(State.Unknown)
        yield _all

    @pytest.fixture
    def random_etcd_client(self):
        _etcds = [i for i in DockerEtcd]
        _etcd = random.choice(_etcds)
        return factory_etcd_client(_etcd)

    @pytest_asyncio.fixture
    async def flush_all_data(self, random_etcd_client: MySQLEtcdClient):
        await random_etcd_client.delete()
        yield

    @pytest_asyncio.fixture
    async def put_run_dockers_data(
        self, flush_all_data, random_etcd_client, get_run_dockers
    ):
        data = MySQLEtcdData(
            nodes=get_run_dockers,
        )
        await random_etcd_client.put(data)

    def all_docker_nodes(self, state: State) -> list[Server]:
        datas: list[Server] = list()
        for i in DockerEtcd:
            datas.append(
                Server(
                    host=i.value.host,
                    port=i.value.port,
                    state=state,
                )
            )

        return datas

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "etcd_client",
        [factory_etcd_client(i) for i in DockerEtcd],
    )
    async def test_put(self, flush_all_data, etcd_client: MySQLEtcdClient):
        nodes = self.all_docker_nodes(State.Run)
        data = MySQLEtcdData(
            nodes=nodes,
        )
        await etcd_client.put(data)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "state",
        [
            State.Run,
            State.Unknown,
            State.Broken,
        ],
    )
    @pytest.mark.parametrize(
        "etcd_client",
        [factory_etcd_client(i) for i in DockerEtcd],
    )
    async def test_get(
        self,
        state: State,
        flush_all_data: None,
        etcd_client: MySQLEtcdClient,
    ):
        nodes = self.all_docker_nodes(state)
        data = MySQLEtcdData(
            nodes=nodes,
        )
        await etcd_client.put(data)
        res: MySQLEtcdData = await etcd_client.get()
        assert len(res.nodes) == len(nodes)
        for node in res.nodes:
            assert node.state is state

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "etcd_client",
        [factory_etcd_client(i) for i in DockerEtcd],
    )
    async def test_delete(
        self,
        put_run_dockers_data: None,
        etcd_client: MySQLEtcdClient,
    ):
        await etcd_client.delete()
        res: MySQLEtcdData = await etcd_client.get()
        assert len(res.nodes) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "old_state",
        [i for i in State],
    )
    @pytest.mark.parametrize(
        "new_state",
        [i for i in State],
    )
    @pytest.mark.parametrize(
        "etcd_client",
        [factory_etcd_client(i) for i in DockerEtcd],
    )
    async def test_update(
        self,
        flush_all_data: None,
        etcd_client: MySQLEtcdClient,
        old_state: State,
        new_state: State,
    ):
        nodes = self.all_docker_nodes(old_state)
        data = MySQLEtcdData(
            nodes=nodes,
        )
        await etcd_client.put(data)

        for node in nodes:
            await etcd_client.update_state(node, new_state)

        res: MySQLEtcdData = await etcd_client.get()
        assert len(res.nodes) == len(nodes)
        for node in res.nodes:
            assert node.state is new_state

    @pytest.mark.asyncio
    async def test_broken(self):
        etcd_dockers = list(DockerEtcd)
        etcd_docker_count = len(etcd_dockers)
        _majority = etcd_docker_count // 2
        for index, etcd in enumerate(etcd_dockers):
            # All client is gone
            next_client_index = index + 1
            if next_client_index == etcd_docker_count:
                return

            break_etcd_container(etcd)
            if index + 1 < _majority:
                cli = factory_etcd_client(etcd_dockers[next_client_index], 1)
                await cli.ping()
            else:
                cli = factory_etcd_client(etcd_dockers[next_client_index], 1)
                with pytest.raises(EtcdPingError) as e:
                    await cli.ping(try_count=0)
