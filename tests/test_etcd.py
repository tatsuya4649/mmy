import ipaddress
from enum import Enum

import pytest
import pytest_asyncio

from mmy.etcd import MySQLEtcdClient, MySQLEtcdDuplicateNode
from mmy.server import _Server

from .docker import container
from .test_mysql import DockerMySQL


class DockerEtcd(Enum):
    Etcd1 = container(
        service_name="etcd1",
        container_name="etcd1",
        host=ipaddress.ip_address("127.0.0.1"),
        port=12379,
    )
    Etcd2 = container(
        service_name="etcd2",
        container_name="etcd2",
        host=ipaddress.ip_address("127.0.0.1"),
        port=12381,
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
