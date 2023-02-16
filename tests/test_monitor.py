import asyncio
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio

from mmy.etcd import MySQLEtcdClient, MySQLEtcdData, MySQLEtcdDuplicateNode
from mmy.monitor import MmyMonitor
from mmy.mysql.hosts import MySQLHosts
from mmy.server import Server, State, _Server

from .test_etcd import DockerEtcd, docker_etcd_client, docker_etcd_server
from .test_mysql import DockerMySQL

MYSQL_DOCKERS = [
    DockerMySQL.MySQL1,
    DockerMySQL.MySQL2,
]


@pytest.fixture
def _monitor_mysqls():
    mh = MySQLHosts()

    m1 = DockerMySQL.MySQL1.value
    m2 = DockerMySQL.MySQL2.value
    mh.update(
        [
            Server(
                host=m1.host,
                port=m1.port,
                state=State.Unknown,
            ),
            Server(
                host=m2.host,
                port=m2.port,
                state=State.Unknown,
            ),
        ]
    )
    return mh


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _startup_etcd_cluter(_monitor_mysqls):
    mc = MySQLEtcdClient(
        host=DockerEtcd.Etcd1.value.host,
        port=DockerEtcd.Etcd1.value.port,
    )
    await mc.delete()
    for mysql in MYSQL_DOCKERS:
        s = _Server(
            host=mysql.value.host,
            port=mysql.value.port,
        )
        try:
            await mc.add_new_node(s)
        except MySQLEtcdDuplicateNode:
            pass


@asynccontextmanager
async def run_monitor_server(monitor: MmyMonitor):
    monitor_task = asyncio.create_task(monitor.start_monitor())
    try:
        yield monitor
    finally:
        monitor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor_task


@pytest.mark.asyncio
async def test_monitor(
    test_mysql_info,
    _monitor_mysqls,
    docker_etcd_server,
    docker_etcd_client,
):
    monitor = MmyMonitor(
        mysql_hosts=_monitor_mysqls,
        auth=test_mysql_info,
        etcd=docker_etcd_server,
        min_jitter=0,
        max_jitter=0.5,
        connect_timeout=1,
    )
    async with run_monitor_server(monitor):
        await asyncio.sleep(1)

        info: MySQLEtcdData = await docker_etcd_client.get()
        assert len(info.nodes) == len(_monitor_mysqls)
        for node in info.nodes:
            assert node.state is State.Run
