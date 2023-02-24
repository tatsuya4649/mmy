import logging
from typing import Any

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.mysql.client import MySQLInsertDuplicateBehavior, TableName
from mmy.mysql.reshard import MySQLReshard
from mmy.ring import MySQLRing
from mmy.server import Server, State

from ._mysql import ROOT_USERINFO, TEST_TABLE2, delete_all_table, mmy_info
from .test_etcd import up_etcd_docker_containers
from .test_mysql import get_mysql_docker_for_test, up_mysql_docker_container

logger = logging.getLogger(__name__)


class TestReshard:
    GENERAETE_REPEAT = 20
    MIN_COUNT = 10000

    async def generate_data(self, mmy_info, container):
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
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

    async def get_from_all_mysqls(
        self, mmy_info
    ) -> dict[TableName, list[dict[str, Any]]]:
        res: dict[TableName, list[dict[str, Any]]] = dict()
        res.setdefault(TEST_TABLE2, list())
        for mysql_container in get_mysql_docker_for_test():
            async with aiomysql.connect(
                host=str(mysql_container.value.host),
                port=mysql_container.value.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                cur = await connect.cursor()
                await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                res[TEST_TABLE2].extend(await cur.fetchall())
        return res

    @pytest_asyncio.fixture
    async def setup_mysql_nodes(self, mmy_info):
        for mysql_container in get_mysql_docker_for_test():
            # Start MySQL container
            await up_mysql_docker_container(mysql_container)
            await delete_all_table(mysql_container, mmy_info)
            await self.generate_data(mmy_info, mysql_container)

    @pytest.mark.asyncio
    async def test(
        self,
        setup_mysql_nodes,
        mmy_info,
    ):
        before_reshard = await self.get_from_all_mysqls(mmy_info)
        reshard = MySQLReshard(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
        )
        # Actually do resharding
        await reshard.do(
            delete_unnecessary=True,
        )

        # Data matching test
        post_datas: list[dict[str, Any]] = before_reshard[TEST_TABLE2]
        ring = MySQLRing(
            init_nodes=[
                Server(
                    host=i.value.host,
                    port=i.value.port,
                    state=State.Run,
                )
                for i in get_mysql_docker_for_test()
            ],
            mysql_info=mmy_info.mysql,
        )
        mysqls = get_mysql_docker_for_test()
        post_data_count: int = 0
        for con in mysqls:
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
                    f"SELECT COUNT(*) as count FROM {TEST_TABLE2}",
                )
                res = await cur.fetchone()
                post_data_count += res["count"]

        assert post_data_count == len(post_datas)

        for data in post_datas:
            _id = data["id"]
            instance = ring.get_instance(_id)
            for con in mysqls:
                async with aiomysql.connect(
                    host=str(con.value.host),
                    port=con.value.port,
                    cursorclass=DictCursor,
                    user=ROOT_USERINFO.user,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:
                    cur = await connect.cursor()
                    await cur.execute(
                        f"SELECT * FROM {TEST_TABLE2} WHERE id=%s", args=(_id)
                    )
                    res = await cur.fetchall()
                    if (
                        con.value.host == instance.host
                        and con.value.port == instance.port
                    ):
                        assert len(res) == 1
                    else:
                        assert len(res) == 0
