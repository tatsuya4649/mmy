import hashlib
import logging
import random
from datetime import datetime, timezone
from typing import Any

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.mysql.client import MySQLInsertDuplicateBehavior, TableName
from mmy.mysql.reshard import MySQLReshard, NodeByResult, ReshardResult, _Reshard
from mmy.parse import MmyYAML
from mmy.ring import MySQLRing
from mmy.server import Server, State

from ._mysql import ROOT_USERINFO, TEST_TABLE2, delete_all_table, mmy_info
from .test_add import (
    _original_consistent_hashing_delete,
    consistent_hashing_delete,
    consistent_hashing_select,
    get_already_hashed_key,
)
from .test_etcd import up_etcd_docker_containers
from .test_mysql import (
    DockerMySQL,
    get_mysql_docker_for_test,
    up_mysql_docker_container,
)
from .test_ring import SAMPLE_MD5_ROWS_COUNT, sample_md5_bulk

logger = logging.getLogger(__name__)


class TestReshard:
    DATA_LENGTH = 10000
    LOG_STEP: int = 10000

    def _generate_random_data(self, index: int) -> list[dict[str, Any]]:
        res: list[dict[str, Any]] = list()
        import hashlib
        import random
        import time
        from datetime import datetime, timezone

        base_string: str = str(time.time_ns())
        for i in range(self.DATA_LENGTH):
            _seed = f"{base_string}_{i}"
            _id = hashlib.sha1(_seed.encode("utf-8")).hexdigest()
            _md5 = hashlib.md5(_seed.encode("utf-8")).hexdigest()
            _title = hashlib.sha224(_seed.encode("utf-8")).hexdigest()
            _duration = random.uniform(0, 100)
            _do_on = datetime.now(timezone.utc)
            res.append(
                {
                    "id": _id,
                    "md5": _md5,
                    "title": _title,
                    "duration": _duration,
                    "do_on": _do_on,
                }
            )
        return res

    async def generate_data(
        self, mmy_info: MmyYAML, container: DockerMySQL, index: int
    ):
        logger.info(container)
        async with aiomysql.connect(
            host=str(container.value.host),
            port=container.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            cur = await connect.cursor()
            await cur.executemany(
                f"INSERT INTO {TEST_TABLE2}(id, md5, title, duration, do_on) VALUES (%(id)s, %(md5)s, %(title)s, %(duration)s, %(do_on)s)",
                self._generate_random_data(index),
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

        BULK_INSERT_SIZE = 1000

        for index, mysql_container in enumerate(get_mysql_docker_for_test()):
            # Start MySQL container
            await up_mysql_docker_container(mysql_container)
            await delete_all_table(mysql_container, mmy_info)
            await self.generate_data(
                mmy_info=mmy_info,
                container=mysql_container,
                index=index,
            )
            async with aiomysql.connect(
                host=str(DockerMySQL.MySQL1.value.host),
                port=DockerMySQL.MySQL1.value.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:

                cur = await connect.cursor()

                def _get_random_bytes() -> bytes:
                    return str(random.uniform(0, 100)).encode("utf-8")

                logger.info(f"Insert random data: {SAMPLE_MD5_ROWS_COUNT} rows")
                for bulk_index, bulk in enumerate(
                    sample_md5_bulk(bulk_count=BULK_INSERT_SIZE)
                ):
                    logger.info(
                        f"{bulk_index}: Insert size {bulk_index*BULK_INSERT_SIZE}"
                    )
                    _ds = list()
                    for i in bulk:
                        _id = i[:-1] + f"{index}"
                        _ds.append(
                            {
                                "id": _id,
                                "md5": hashlib.md5(_get_random_bytes()).hexdigest(),
                                "title": hashlib.sha256(
                                    _get_random_bytes()
                                ).hexdigest(),
                                "duration": random.uniform(0, 10),
                                "do_on": datetime.now(timezone.utc),
                            }
                        )

                    await cur.executemany(
                        query=f"INSERT INTO {TEST_TABLE2}(\
                            id, \
                            md5, \
                            title, \
                            duration, \
                            do_on \
                            ) VALUES (%(id)s, %(md5)s, %(title)s, %(duration)s, %(do_on)s)",
                        args=_ds,
                    )

                await connect.commit()

        return

    @pytest.mark.asyncio
    async def test_matching(
        self,
        setup_mysql_nodes,
        mmy_info,
        mocker,
    ):
        before_reshard = await self.get_from_all_mysqls(mmy_info)
        reshard = MySQLReshard(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
        )
        mocker.patch(
            "mmy.mysql.client.MySQLClient.consistent_hashing_select",
            side_effect=consistent_hashing_select,
            autospec=True,
        )
        mocker.patch(
            "mmy.mysql.client.MySQLClient.consistent_hashing_delete",
            side_effect=consistent_hashing_delete,
            autospec=True,
        )

        def get_instance(self, data: Any):
            return get_already_hashed_key(
                ring=self._hr,
                key=data,
            )

        mocker.patch(
            "mmy.ring.Ring.get_instance",
            side_effect=get_instance,
            autospec=True,
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
        logger.info("Test total rows count across multiple MySQL nodes")
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

        logger.info("Matching test")
        for index, con in enumerate(mysqls):
            logger.info(f"Matching test: {con.name} {index}/{len(mysqls)}")
            async with aiomysql.connect(
                host=str(con.value.host),
                port=con.value.port,
                cursorclass=DictCursor,
                user=ROOT_USERINFO.user,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                for index, data in enumerate(post_datas):
                    if index % self.LOG_STEP == 0:
                        logger.info(f"Passed matching test: {index}/{len(post_datas)}")

                    _id = data["id"]
                    instance = ring.get_instance(_id)
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

    @pytest.mark.asyncio
    async def test_inconsistency(
        self,
        setup_mysql_nodes,
        mmy_info,
    ):
        reshard = MySQLReshard(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
        )
        await reshard.do(
            delete_unnecessary=True,
        )

        # Do resharging again
        node_by_result: NodeByResult = await reshard.do(
            delete_unnecessary=True,
        )
        node: ReshardResult
        reshard: _Reshard
        for node in node_by_result:
            for table in node.tables:
                reshard = node.result_by_table[table]
                assert reshard.moved_rows == 0
