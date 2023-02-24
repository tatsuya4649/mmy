"""

Integration test about MySQL cluster management.

"""
import asyncio
import hashlib
import logging
import random
import time
from enum import Enum, auto
from pprint import pformat
from typing import Any

import aiomysql
import pytest
from aiomysql.cursors import DictCursor
from mmy.etcd import (
    EtcdConnectError,
    EtcdError,
    EtcdPingError,
    MySQLEtcdClient,
    MySQLEtcdData,
    MySQLEtcdDuplicateNode,
)
from mmy.mysql.add import MySQLAdder
from mmy.mysql.client import TableName
from mmy.mysql.delete import (
    MySQLDeleter,
    MySQLDeleterNoServer,
    MySQLDeleterNotExistError,
)
from mmy.mysql.hosts import MySQLHosts
from mmy.parse import MmyYAML
from mmy.ring import MySQLRing
from mmy.server import Server, State, _Server
from rich import print

from ._mysql import ROOT_USERINFO, TEST_TABLE2, mmy_info
from .test_etcd import (
    DockerEtcd,
    break_etcd_container,
    etcd_flush_all_data,
    restore_etcd_container,
    up_etcd_docker_containers,
)
from .test_mysql import (
    DockerMySQL,
    delete_all_table,
    get_mysql_docker_for_test,
    random_generate_data,
    up_mysql_docker_container,
)
from .test_proxy import mysql_hosts, proxy_server_start

logger = logging.getLogger(__name__)


class OpeType(Enum):
    Add = auto()
    Delete = auto()


class TestIntegration:
    OPERATION_COUNT: int = 10

    @property
    def random_operation(self) -> OpeType:
        return random.choice(list(OpeType))

    @property
    def random_mysql_docker(self) -> DockerMySQL:
        return random.choice(get_mysql_docker_for_test())

    @pytest.mark.asyncio
    async def test_random_add_delete(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        # Setup
        for container in DockerMySQL:
            await up_mysql_docker_container(container)
            await delete_all_table(container, mmy_info)

        await random_generate_data(
            container=DockerMySQL.MySQL1,
            mmy_info=mmy_info,
            genereate_repeate=25,
            min_count=100000,
        )

        # Fetch MySQL cluster data.
        # This data is used for unmatch data test.
        user_data: list[dict[str, Any]] = list()
        post_data: list[dict[str, Any]] = list()
        for container in DockerMySQL:
            async with aiomysql.connect(
                host=str(container.value.host),
                port=container.value.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                # Get all data
                cur = await connect.cursor()
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                post_data.extend(await cur.fetchall())

        mysqls = get_mysql_docker_for_test()
        mysql_info = mmy_info.mysql
        etcd_info = mmy_info.etcd
        nodes_count: int = 0
        current_nodes: set[DockerMySQL] = set()

        adder = MySQLAdder(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        deleter = MySQLDeleter(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        # Add initial node
        _mysql1 = DockerMySQL.MySQL1
        await adder.do(
            server=_Server(
                host=_mysql1.value.host,
                port=_mysql1.value.port,
            )
        )
        current_nodes.add(_mysql1)

        # Do operation
        for i in range(self.OPERATION_COUNT):
            logger.info(f"Complete {int(100*(i/self.OPERATION_COUNT))}%")

            ope: OpeType = self.random_operation
            mysql: DockerMySQL = self.random_mysql_docker
            server: _Server = _Server(
                host=mysql.value.host,
                port=mysql.value.port,
            )
            # All MySQL Docker nodes is added
            if nodes_count == len(mysqls) and ope is OpeType.Add:
                continue

            logger.info(current_nodes)
            logger.info(f"{ope}, {mysql}")
            match ope:
                case OpeType.Add:
                    if mysql in current_nodes:
                        with pytest.raises(MySQLEtcdDuplicateNode):
                            await adder.do(
                                server=server,
                                raise_duplicate=True,
                            )
                        logger.info("DUPLICATE ADD")
                        continue

                    await adder.do(
                        server=server,
                    )

                    nodes_count += 1
                    logger.info(
                        [i.address_format() for i in adder.ring.get_instances()]
                    )
                    current_nodes.add(mysql)

                    ring = MySQLRing(
                        mysql_info=mmy_info.mysql,
                        init_nodes=[
                            _Server(
                                host=con.value.host,
                                port=con.value.port,
                            )
                            for con in current_nodes
                        ],
                    )
                    assert ring._hr.get_points() == adder.ring._hr.get_points()
                    assert len(adder.ring) == len(current_nodes)
                case OpeType.Delete:
                    if mysql not in current_nodes:
                        with pytest.raises(MySQLDeleterNotExistError):
                            await deleter.do(
                                server=server,
                            )
                        logger.info("NO EXISTS DELETE")
                        continue

                    if len(current_nodes) == 1:
                        with pytest.raises(MySQLDeleterNoServer):
                            await deleter.do(
                                server=server,
                            )
                        logger.info("NO NODE DELETE")
                        continue

                    await deleter.do(
                        server=server,
                        not_update_while_moving=True,
                    )

                    nodes_count -= 1
                    logger.info(deleter.ring.get_instances())
                    current_nodes.remove(mysql)
                    assert len(deleter.ring) == len(current_nodes)

                    ring = MySQLRing(
                        mysql_info=mmy_info.mysql,
                        init_nodes=[
                            _Server(
                                host=con.value.host,
                                port=con.value.port,
                            )
                            for con in current_nodes
                        ],
                    )
                    assert ring._hr.get_points() == deleter.ring._hr.get_points()

            logger.info(f"{len(current_nodes)}, {current_nodes}")
            # Waiting for propagating new MySQL node info on etcd
            for _ in range(50):
                await asyncio.sleep(0.1)
                etcd = MySQLEtcdClient(
                    host=mmy_info.etcd.nodes[0].host,
                    port=mmy_info.etcd.nodes[0].port,
                )
                data: MySQLEtcdData = await etcd.get()
                logger.info(f"{len(current_nodes)}, {current_nodes}")
                logger.info(f"{len(data.nodes)}, {data.nodes}")
                if len(data.nodes) != len(current_nodes):
                    continue

                break
            else:
                raise RuntimeError

            ring = MySQLRing(
                mysql_info=mmy_info.mysql,
                init_nodes=[
                    _Server(
                        host=con.value.host,
                        port=con.value.port,
                    )
                    for con in current_nodes
                ],
            )

            logger.info("Unmatching test. Data placed correctly.")
            for data_index, post in enumerate(post_data):
                if data_index % 1000 == 0:
                    logger.info(f"Done: {int(100*(data_index/len(post_data)))}%")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in current_nodes:
                    try:
                        async with aiomysql.connect(
                            host=str(con.value.host),
                            port=con.value.port,
                            user=ROOT_USERINFO.user,
                            cursorclass=DictCursor,
                            password=ROOT_USERINFO.password,
                            db=mmy_info.mysql.db,
                        ) as connect:
                            cur = await connect.cursor()
                            await cur.execute(
                                f"SELECT COUNT(*) as count FROM {TEST_TABLE2} WHERE id=%(_id)s",
                                {
                                    "_id": key,
                                },
                            )
                            _f = await cur.fetchone()
                            assert _f["count"] == 1

                            if not (
                                con.value.host == instance.host
                                and con.value.port == instance.port
                            ):
                                logger.info(pformat(ring._hr.get_points()))
                                raise RuntimeError(
                                    f"Data({key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')

    async def _setup_for_test_proxy(self, mmy_info):
        # Setup
        for container in DockerMySQL:
            await up_mysql_docker_container(container)
            await delete_all_table(container, mmy_info)

        await random_generate_data(
            container=DockerMySQL.MySQL1,
            mmy_info=mmy_info,
            genereate_repeate=24,
            min_count=100000,
        )
        # Fetch MySQL cluster data.
        # This data is used for unmatch data test.
        user_data: list[dict[str, Any]] = list()
        post_data: list[dict[str, Any]] = list()
        for container in DockerMySQL:
            async with aiomysql.connect(
                host=str(container.value.host),
                port=container.value.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                # Get all data
                cur = await connect.cursor()
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                post_data.extend(await cur.fetchall())

        return user_data, post_data

    @property
    def generate_sha_256(self) -> str:
        t = time.time_ns()
        s = hashlib.sha256(str(t).encode("utf-8")).hexdigest()
        return s

    @pytest.mark.asyncio
    async def test_proxy(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
        unused_tcp_port,
        up_etcd_docker_containers,
    ):
        from mmy.mysql.hosts import MySQLHosts

        from .test_proxy import run_proxy_server

        user_data, post_data = await self._setup_for_test_proxy(
            mmy_info=mmy_info,
        )

        current_nodes: set[DockerMySQL] = set()
        mysql_servers: list[Server] = list()
        mysqls = get_mysql_docker_for_test()
        mysql_info = mmy_info.mysql
        etcd_info = mmy_info.etcd

        adder = MySQLAdder(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        deleter = MySQLDeleter(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        # Add initial node
        _mysql1 = DockerMySQL.MySQL1
        await adder.do(
            server=_Server(
                host=_mysql1.value.host,
                port=_mysql1.value.port,
            )
        )
        current_nodes.add(_mysql1)

        mysql_hosts = MySQLHosts()
        for node in mmy_info.etcd.nodes:
            try:
                etcd_cli = MySQLEtcdClient(
                    host=node.host,
                    port=node.port,
                )
                nodes_data = await etcd_cli.get()
                mysql_hosts.update(nodes_data.nodes)
                mysql_servers = nodes_data.nodes
                break
            except EtcdConnectError:
                continue
        else:
            raise RuntimeError

        async def watch_etcd():
            while True:
                for node in mmy_info.etcd.nodes:
                    try:
                        etcd_cli = MySQLEtcdClient(
                            host=node.host,
                            port=node.port,
                        )
                        async for item in etcd_cli.watch_mysql():
                            logger.info("Detect change of MySQL nodes")
                            logger.info(f"New MySQL nodes: {pformat(item.nodes)}")
                            mysql_hosts.update(item.nodes)
                    except EtcdPingError:
                        logger.warning(f"etcd ping error: {node.address_format()}")
                    except EtcdError as e:
                        logger.error(e)

        # Watching host update
        asyncio.create_task(watch_etcd())

        break_etcd_node: list[DockerEtcd] | None = None
        # Do operation
        for i in range(self.OPERATION_COUNT):
            if break_etcd_node is not None:
                for _node in break_etcd_node:
                    logger.info(f"Restore etcd node: {_node.name}")
                    restore_etcd_container(_node)

            break_etcd_node = self.random_break_etcd_node()
            async with run_proxy_server(
                mysql_hosts=mysql_hosts,
                unused_tcp_port=unused_tcp_port,
            ):
                logger.info(f"Complete {int(100*(i/self.OPERATION_COUNT))}%")

                ope: OpeType = self.random_operation
                mysql: DockerMySQL = self.random_mysql_docker
                server: _Server = _Server(
                    host=mysql.value.host,
                    port=mysql.value.port,
                )
                now_hosts = mysql_hosts.get_hosts()
                # All MySQL Docker nodes is added
                if len(now_hosts) == len(mysqls) and ope is OpeType.Add:
                    continue
                elif len(now_hosts) == 1 and ope is OpeType.Delete:
                    continue
                elif server in now_hosts and ope is OpeType.Add:
                    continue
                elif server not in now_hosts and ope is OpeType.Delete:
                    continue

                logger.info(f"Current nodes: {[i.name for i in current_nodes]}")
                logger.info(f"{ope}, {mysql}")

                async def operate(_server, _container):
                    i = 0
                    while True:
                        try:
                            match ope:
                                case OpeType.Add:
                                    await adder.do(
                                        server=_server,
                                    )
                                    current_nodes.add(_container)
                                case OpeType.Delete:
                                    await deleter.do(
                                        server=_server,
                                        not_update_while_moving=False,
                                    )
                                    current_nodes.remove(_container)
                        except MySQLDeleterNoServer as e:
                            logger.warning(e)
                            if i > 5:
                                raise e

                            i += 1
                            await asyncio.sleep(1)
                            continue

                        break

                # Do test at same time while dynamic moving data
                unmatch_task = asyncio.create_task(
                    self.unmatch_test_proxy(
                        user_data=user_data,
                        post_data=post_data,
                        mmy_info=mmy_info,
                        port=unused_tcp_port,
                        mysql_servers=mysql_servers,
                    )
                )
                insert_task = asyncio.create_task(
                    self.insert_correctly(
                        mmy_info=mmy_info,
                        port=unused_tcp_port,
                    )
                )
                await operate(server, mysql)

                logger.info("Wait unmatch task")
                await unmatch_task
                # Test for placed correctly data that is inserted while moving data
                inserted_ids = await insert_task
                await self.unmatch_inserted_which_moving_data(
                    post_ids=inserted_ids[TEST_TABLE2],
                    mmy_info=mmy_info,
                    port=unused_tcp_port,
                )

            await asyncio.sleep(0.1)
            continue

    def random_break_etcd_node(
        self,
    ) -> list[DockerEtcd]:
        nodes = random.sample(list(DockerEtcd), len(list(DockerEtcd)) // 3)
        for n in nodes:
            logger.info(f"Break etcd node: {n.name}")
            break_etcd_container(n)
        return nodes

    async def unmatch_inserted_which_moving_data(
        self,
        post_ids,
        mmy_info,
        port,
    ):
        """

        Do this test that checks about data inserted while moving data after random operation via mmy proxy.

        """
        from mmy.mysql.proxy_connection import proxy_connect

        from .test_proxy import HOST

        for index, _id in enumerate(post_ids):
            if index % 100 == 0:
                logger.info(f"Unmatch test Complete {index}/{len(post_ids)}")

            key = str(_id)
            _client = proxy_connect(
                key=key,
                host=HOST,
                port=port,
                db=mmy_info.mysql.db,
                user=ROOT_USERINFO.user,
                password=ROOT_USERINFO.password,
                connect_timeout=0.1,
                local_infile=True,
            )
            async with _client as connect:
                cursor = await connect.cursor()
                async with cursor:
                    await cursor.execute(
                        key=key,
                        query=f"SELECT * FROM {TEST_TABLE2} WHERE id=%(_id)s",
                        args={
                            "_id": key,
                        },
                    )
                    res = await cursor.fetchall()
                    assert len(res) == 1

    async def _unmatch_raise(
        self,
        e: Exception,
        table: TableName,
        data: dict[str, Any],
        _id: str,
        mmy_info: MmyYAML,
        mysql_servers: list[Server],
    ):
        mysqls = get_mysql_docker_for_test()
        for mysql in mysqls:
            _client = aiomysql.connect(
                host=mysql.values.host,
                port=mysql.values.port,
                db=mmy_info.mysql.db,
                user=ROOT_USERINFO.user,
                password=ROOT_USERINFO.password,
                connect_timeout=0.1,
                local_infile=True,
            )
            async with _client as connect:
                cursor = await connect.cursor()
                async with cursor:
                    await cursor.execute(
                        query=f"SELECT * FROM {table} WHERE id=%(_id)s",
                        args={
                            "_id": _id,
                        },
                    )
                    res = await cursor.fetchone()
                    if res is not None:
                        ring = MySQLRing(
                            mysql_info=mmy_info.mysql,
                            init_nodes=[
                                i for i in mysql_servers if i.state is not State.Run
                            ],
                        )
                        instance: Server = ring.get_instance(_id)
                        logger.error(f"{table}: {_id} on {mysql.name}: {pformat(data)}")
                        logger.error(f"{_id} must be on {instance.address_format()}")

        raise e

    async def unmatch_test_proxy(
        self,
        user_data,
        post_data,
        mmy_info,
        port,
        mysql_servers: list[Server],
    ):
        from mmy.mysql.proxy_connection import proxy_connect

        from .test_proxy import HOST

        RETRY_COUNT: int = 5

        for index, data in enumerate(post_data):
            if index % 1000 == 0:
                logger.info(
                    f"Unmatch test Complete {TEST_TABLE2}: {index}/{len(post_data)}"
                )

            key = str(data["id"])
            i = 0
            while True:
                try:
                    _client = proxy_connect(
                        key=key,
                        host=HOST,
                        port=port,
                        db=mmy_info.mysql.db,
                        user=ROOT_USERINFO.user,
                        password=ROOT_USERINFO.password,
                        connect_timeout=0.1,
                        local_infile=True,
                    )
                    async with _client as connect:
                        cursor = await connect.cursor()
                        async with cursor:
                            await cursor.execute(
                                key=key,
                                query=f"SELECT * FROM {TEST_TABLE2} WHERE id=%(_id)s",
                                args={
                                    "_id": key,
                                },
                            )
                            res = await cursor.fetchall()
                            assert len(res) == 1

                    break
                except AssertionError as e:
                    if i > RETRY_COUNT:
                        await self._unmatch_raise(
                            e, TEST_TABLE2, data, key, mmy_info, mysql_servers
                        )
                    await asyncio.sleep(1)
                    i += 1

    async def insert_correctly(
        self,
        mmy_info,
        port,
    ) -> dict[TableName, list[str]]:
        from mmy.mysql.proxy_connection import proxy_connect

        from .test_proxy import HOST

        INSERT_COUNT: int = 1000
        _random_base = random.randint(1000000, 20000000)
        ids: dict[TableName, list[str]] = dict()
        ids.setdefault(TEST_TABLE2, list())
        for index in range(INSERT_COUNT):
            if index % 100 == 0:
                logger.info(f"Insert while moving {index}/{INSERT_COUNT}")
            _id = _random_base + index
            key = str(_id)
            key = hashlib.md5(str(_id).encode("utf-8")).hexdigest()
            _client = proxy_connect(
                key=key,
                host=HOST,
                port=port,
                db=mmy_info.mysql.db,
                user=ROOT_USERINFO.user,
                password=ROOT_USERINFO.password,
                connect_timeout=0.1,
                local_infile=True,
            )
            async with _client as connect:
                cursor = await connect.cursor()
                async with cursor:
                    res = await cursor.execute(
                        key=key,
                        query=f"INSERT INTO {TEST_TABLE2}(id, title) VALUES(%(_id)s, %(title)s)",
                        args={
                            "_id": key,
                            "title": hashlib.sha512(key.encode("utf-8")),
                        },
                    )
                    assert res == 1
                    ids[TEST_TABLE2].append(key)

                await connect.commit()

        return ids
