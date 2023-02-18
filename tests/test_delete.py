import logging
from copy import copy
from typing import Any

import aiomysql
import pytest
from aiomysql.cursors import DictCursor
from tqdm import tqdm

from mmy.mysql.add import PingType
from mmy.mysql.delete import MySQLDeleter
from mmy.parse import MmyYAML
from mmy.server import _Server

from ._mysql import ROOT_USERINFO, TEST_TABLE1, TEST_TABLE2, mmy_info
from .test_etcd import etcd_flush_all_data, etcd_put_mysql_container
from .test_mysql import (
    DockerMySQL,
    fix_down_all_mysql_docker_containers,
    get_mysql_docker_for_test,
    up_mysql_docker_container,
)

logger = logging.getLogger(__name__)


class TestDelete:
    async def delete_all_table(
        self,
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
            await cur.execute("DELETE FROM %s" % (TEST_TABLE1))
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute("DELETE FROM %s" % (TEST_TABLE2))
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute("OPTIMIZE TABLE %s" % (TEST_TABLE1))
            await connect.commit()
            cur = await connect.cursor()
            await cur.execute("OPTIMIZE TABLE %s" % (TEST_TABLE2))
            await connect.commit()

    @pytest.mark.asyncio
    async def test_only_delete(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        mysql_containers = get_mysql_docker_for_test()
        for container in mysql_containers:
            await up_mysql_docker_container(container)
            await self.delete_all_table(container, mmy_info)

        await etcd_put_mysql_container(mysql_containers)
        for container in tqdm(mysql_containers):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            deleter = MySQLDeleter(
                mysql_info=mmy_info.mysql,
                etcd_info=mmy_info.etcd,
            )
            await deleter.do(
                server=_server,
                ping_type=PingType.OnlyTargetServer,
                delete_data=True,
            )

    @pytest.mark.asyncio
    async def test_inconsistency(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        """

        When add node, test whether inserted data is deleted or different node has data.

        """

        mysql_containers = get_mysql_docker_for_test()
        await etcd_put_mysql_container(mysql_containers)
        GENERAETE_REPEAT: int = 10
        MIN_COUNT: int = 10000
        user_data: list[dict[str, Any]] = list()
        post_data: list[dict[str, Any]] = list()
        delete_before_user_count: int = 0
        delete_before_post_count: int = 0
        for container in mysql_containers:
            await up_mysql_docker_container(container)

            # Connection to MySQL without mmy proxy.
            async with aiomysql.connect(
                host=str(container.value.host),
                port=container.value.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                # Delete all table data from container
                await self.delete_all_table(container, mmy_info)
                cur = await connect.cursor()
                await cur.execute(
                    "CALL generate_random_user(%d, %d)" % (GENERAETE_REPEAT, MIN_COUNT)
                )
                await connect.commit()

                cur = await connect.cursor()
                await cur.execute(
                    "CALL generate_random_post(%d, %d)" % (GENERAETE_REPEAT, MIN_COUNT)
                )
                await connect.commit()

                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as user_count FROM %s" % (TEST_TABLE1)
                )
                _user_count = await cur.fetchone()
                assert _user_count is not None
                delete_before_user_count += _user_count["user_count"]
                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2)
                )
                _post_count = await cur.fetchone()
                assert _post_count is not None
                delete_before_post_count += _post_count["post_count"]

                cur = await connect.cursor()
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
                user_data.extend(await cur.fetchall())

                cur = await connect.cursor()
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                post_data.extend(await cur.fetchall())

        cons = copy(mysql_containers)
        cons.pop(0)
        deleter = MySQLDeleter(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        for container in tqdm(cons):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            await deleter.do(
                server=_server,
                ping_type=PingType.OnlyTargetServer,
                delete_data=True,
            )

        logger.info(f"Inconsistency test1")
        # Inconsistency test1: Total rows count
        delete_after_user_count: int = 0
        delete_after_post_count: int = 0
        for container in tqdm(mysql_containers):
            logger.info(container.value.service_name)
            async with aiomysql.connect(
                host=str(container.value.host),
                port=container.value.port,
                user=ROOT_USERINFO.user,
                password=ROOT_USERINFO.password,
                cursorclass=DictCursor,
                db=mmy_info.mysql.db,
            ) as connect:
                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as user_count FROM %s" % (TEST_TABLE1)
                )
                _user_count = await cur.fetchone()
                assert _user_count is not None

                user_count: int = _user_count["user_count"]
                delete_after_user_count += user_count
                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2)
                )
                _post_count = await cur.fetchone()
                assert _post_count is not None
                post_count: int = _post_count["post_count"]
                delete_after_post_count += post_count
                logger.info(f"user_count: {user_count}")
                logger.info(f"post_count: {post_count}")

        logger.info(f"Inserted test data: {TEST_TABLE1}, {TEST_TABLE2}")
        logger.info(f"\tbefore user_count: {delete_before_user_count}")
        logger.info(f"\tbefore post_count: {delete_before_post_count}")
        logger.info(f"\tafter user_count: {delete_after_user_count}")
        logger.info(f"\tafter post_count: {delete_after_post_count}")
        assert delete_after_user_count == delete_before_user_count
        assert delete_after_post_count == delete_before_post_count

        # Incosistency test2: only one data
        # Check same data on multiple MySQL nodes
        logger.info("Inconsistency test with user datas")
        for item in tqdm(user_data):
            i = 0
            for container in mysql_containers:
                async with aiomysql.connect(
                    host=str(container.value.host),
                    port=container.value.port,
                    user=ROOT_USERINFO.user,
                    cursorclass=DictCursor,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:
                    cur = await connect.cursor()
                    await cur.execute(
                        "SELECT COUNT(*) as count FROM %s WHERE uuid='%s'"
                        % (TEST_TABLE1, item["uuid"])
                    )
                    _f = await cur.fetchone()
                    i += _f["count"]

            assert i == 1

        logger.info("Inconsistency test with post datas")
        for item in tqdm(post_data):
            i = 0
            for container in mysql_containers:
                async with aiomysql.connect(
                    host=str(container.value.host),
                    port=container.value.port,
                    user=ROOT_USERINFO.user,
                    cursorclass=DictCursor,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:
                    cur = await connect.cursor()
                    await cur.execute(
                        "SELECT COUNT(*) as count FROM %s WHERE uuid='%s'"
                        % (TEST_TABLE2, item["uuid"])
                    )
                    _f = await cur.fetchone()
                    i += _f["count"]

            assert i == 1
