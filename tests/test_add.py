import logging
from copy import copy
from typing import Any

import aiomysql
import pytest
from aiomysql.cursors import DictCursor
from tqdm import tqdm

from mmy.mysql.add import MySQLAdder
from mmy.parse import MmyYAML
from mmy.server import _Server

from ._mysql import ROOT_USERINFO, TEST_TABLE1, TEST_TABLE2, mmy_info
from .test_etcd import etcd_flush_all_data, up_etcd_docker_containers
from .test_mysql import (
    DockerMySQL,
    down_all_mysql_docker_containers,
    fix_down_all_mysql_docker_containers,
    get_mysql_docker_for_test,
    up_mysql_docker_container,
)

logger = logging.getLogger(__name__)


class TestAdd:
    async def delete_all_table(
        self,
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
            await cur.execute("DELETE FROM %s" % (TEST_TABLE1))
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute("DELETE FROM %s" % (TEST_TABLE2))
            await connect.commit()

    @pytest.mark.asyncio
    async def test_only_add(
        self,
        fix_down_all_mysql_docker_containers,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        for container in tqdm(get_mysql_docker_for_test()):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            # Start MySQL container
            await up_mysql_docker_container(container)
            await self.delete_all_table(container, mmy_info)
            adder = MySQLAdder(
                mysql_info=mmy_info.mysql,
                etcd_info=mmy_info.etcd,
            )
            await adder.do(
                server=_server,
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

        await up_mysql_docker_container(DockerMySQL.MySQL1)
        GENERAETE_REPEAT: int = 14
        MIN_COUNT: int = 10000
        user_data: list[dict[str, Any]] = list()
        post_data: list[dict[str, Any]] = list()
        # Connection to MySQL without mmy proxy.
        async with aiomysql.connect(
            host=str(DockerMySQL.MySQL1.value.host),
            port=DockerMySQL.MySQL1.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            await self.delete_all_table(DockerMySQL.MySQL1, mmy_info)
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
            await cur.execute("SELECT COUNT(*) as user_count FROM %s" % (TEST_TABLE1))
            _user_count = await cur.fetchone()
            assert _user_count is not None
            add_before_user_count: int = _user_count["user_count"]
            cur = await connect.cursor()
            await cur.execute("SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2))
            _post_count = await cur.fetchone()
            assert _post_count is not None
            add_before_post_count: int = _post_count["post_count"]

            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
            user_data = await cur.fetchall()

            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
            post_data = await cur.fetchall()

        logger.info(f"Inserted test data: {TEST_TABLE1}, {TEST_TABLE2}")
        logger.info(f"user_count: {add_before_user_count}")
        logger.info(f"post_count: {add_before_post_count}")
        cons = get_mysql_docker_for_test(2)
        total_cons = copy(cons)
        cons.remove(cons[0])
        for container in tqdm(cons):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            # Delete data inserted when startup Docker container
            await self.delete_all_table(container, mmy_info)

            adder = MySQLAdder(
                mysql_info=mmy_info.mysql,
                etcd_info=mmy_info.etcd,
            )
            await adder.do(
                server=_server,
            )

        logger.info(f"Inconsistency test1")
        # Inconsistency test1: Total rows count
        add_after_user_count: int = 0
        add_after_post_count: int = 0
        for container in tqdm(total_cons):
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
                add_after_user_count += user_count
                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2)
                )
                _post_count = await cur.fetchone()
                assert _post_count is not None
                post_count: int = _post_count["post_count"]
                add_after_post_count += post_count
                logger.info(f"user_count: {user_count}")
                logger.info(f"post_count: {post_count}")

        assert add_after_user_count == add_before_user_count
        assert add_after_post_count == add_before_post_count

        # Incosistency test2: only one data
        # Check same data on multiple MySQL nodes
        logger.info("Inconsistency test with user datas")
        for item in tqdm(user_data):
            i = 0
            for container in total_cons:
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
            for container in total_cons:
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
