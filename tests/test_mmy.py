"""

Integration test about MySQL cluster management.

"""
import asyncio
import logging
import random
from enum import Enum, auto
from pprint import pformat
from typing import Any

import aiomysql
import pytest
from aiomysql.cursors import DictCursor
from mmy.etcd import MySQLEtcdClient, MySQLEtcdData, MySQLEtcdDuplicateNode
from mmy.mysql.add import MySQLAdder
from mmy.mysql.delete import (
    MySQLDeleter,
    MySQLDeleterNoServer,
    MySQLDeleterNotExistError,
)
from mmy.parse import MmyYAML
from mmy.ring import MySQLRing
from mmy.server import _Server
from rich import print

from ._mysql import ROOT_USERINFO, TEST_TABLE1, TEST_TABLE2, mmy_info
from .test_etcd import etcd_flush_all_data
from .test_mysql import (
    DockerMySQL,
    delete_all_table,
    get_mysql_docker_for_test,
    random_generate_data,
    up_mysql_docker_container,
)
from .test_proxy import proxy_server_start

logger = logging.getLogger(__name__)


class OpeType(Enum):
    Add = auto()
    Delete = auto()


class TestIntegration:
    OPERATION_COUNT: int = 30

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
            genereate_repeate=14,
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
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
                user_data.extend(await cur.fetchall())

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
            for data_index, user in enumerate(user_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {int(100*(data_index/len(user_data)))}%")

                key = user["id"]  # This is auto_increment
                check_key = user["md5"]
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
                                f"SELECT COUNT(*) as count FROM {TEST_TABLE1} WHERE md5=%(md5)s",
                                {
                                    "md5": check_key,
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
                                    f"Data({check_key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')

            for data_index, post in enumerate(post_data):
                if data_index % 100 == 0:
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

    @pytest.mark.asyncio
    async def test_proxy(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
        proxy_server_start,
    ):
        class MySQLClientType(Enum):
            SELECT = auto()
            INSERT = auto()
            DELETE = auto()
            UPDATE = auto()

        # TODO: run proxy server
        # TODO: Do dynamic change MySQL nodes
        # TODO: MySQL client operation by mmy client
