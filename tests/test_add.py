import bisect
import hashlib
import logging
import random
import time
from datetime import datetime, timezone
from pprint import pformat
from typing import Any, Coroutine

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.etcd import MySQLEtcdClient
from mmy.mysql.add import PING_RETRY_COUNT, AdderStep, MySQLAdder
from mmy.mysql.client import (
    CHSIter,
    MySQLClient,
    MySQLInsertDuplicateBehavior,
    MySQLKeys,
    TableName,
)
from mmy.mysql.errcode import MySQLErrorCode
from mmy.mysql.sql import SQLPoint
from mmy.parse import MmyYAML
from mmy.ring import MovedData, MySQLRing
from mmy.server import Server, _Server
from pymysql.err import OperationalError
from rich import print

from ._mysql import ROOT_USERINFO, TEST_TABLE2, delete_all_table, mmy_info
from .test_etcd import etcd_flush_all_data, up_etcd_docker_containers
from .test_mysql import (
    DockerMySQL,
    down_all_mysql_docker_containers,
    fix_down_all_mysql_docker_containers,
    get_mysql_docker_for_test,
    up_mysql_docker_container,
)

logger = logging.getLogger(__name__)

from uhashring import HashRing


async def consistent_hashing_select(
    self,
    table: TableName,
    start: SQLPoint,
    end: SQLPoint,
    _or: bool,
    limit_once: int = 10000,
):
    keys: list[MySQLKeys] = await self.primary_keys_info(table)

    return self.select(
        table=table,
        limit_once=limit_once,
        iter_type=CHSIter,
        primary_key=keys[0],
        start=start,
        end=end,
        _or=_or,
        hash_fn=None,
    )


_original_consistent_hashing_delete = MySQLClient.consistent_hashing_delete


async def consistent_hashing_delete(*args, **kwargs):
    return await _original_consistent_hashing_delete(
        *args,
        **kwargs,
        hash_fn=None,
    )


def get_already_hashed_key(
    ring: HashRing,
    key: Any,
) -> Server:
    _keys = ring._keys
    _key_index: int = bisect.bisect(_keys, key)
    if _key_index == len(_keys):
        _key_index = 0
    _key: str = _keys[_key_index]
    nodename: str = ring.ring[_key]
    instance: Server = ring._nodes[nodename]["instance"]
    return instance


class TestAdd:
    GENERAETE_REPEAT: int = 24
    MIN_COUNT: int = 10000
    sleep_secs_before_datamove_again: float = 0.1
    SLEEP_SECS_AFTER_ETCD_ADD_NEW_NODE: float = 0.1
    LOG_STEP: int = 1000

    async def generate_random_data(
        self,
        container: DockerMySQL,
        mmy_info: MmyYAML,
    ):
        async with aiomysql.connect(
            host=str(DockerMySQL.MySQL1.value.host),
            port=DockerMySQL.MySQL1.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            await delete_all_table(DockerMySQL.MySQL1, mmy_info)
            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

    def _adder_do(
        self,
        adder: MySQLAdder,
        server: _Server,
    ):
        return adder.do(
            server=server,
            sleep_secs_before_datamove_again=self.sleep_secs_before_datamove_again,
            sleep_secs_after_etcd_add_new_node=self.SLEEP_SECS_AFTER_ETCD_ADD_NEW_NODE,
            delete_from_old=True,
        )

    @pytest.mark.asyncio
    async def test_only_add(
        self,
        fix_down_all_mysql_docker_containers,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        for container in get_mysql_docker_for_test():
            _server = _Server(
                host=container.value.host,
                port=container.value.port,
            )
            # Start MySQL container
            await up_mysql_docker_container(container)
            await delete_all_table(container, mmy_info)

            adder = MySQLAdder(
                mysql_info=mmy_info.mysql,
                etcd_info=mmy_info.etcd,
            )
            await self._adder_do(
                adder=adder,
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
            await delete_all_table(DockerMySQL.MySQL1, mmy_info)
            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

            await cur.execute("SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2))
            _post_count = await cur.fetchone()
            assert _post_count is not None
            add_before_post_count: int = _post_count["post_count"]

            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
            post_data = await cur.fetchall()

        logger.info(f"Inserted test data: {TEST_TABLE2}")
        logger.info(f"Before post_count: {add_before_post_count}")
        cons = get_mysql_docker_for_test()
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )

        for index, container in enumerate(cons):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            if index > 0:
                # Delete data inserted when startup Docker container except MySQL1
                await delete_all_table(container, mmy_info)

            await self._adder_do(
                adder=adder,
                server=_server,
            )

        logger.info(f"Inconsistency test1")
        # Inconsistency test1: Total rows count
        add_after_user_count: int = 0
        add_after_post_count: int = 0
        for container in cons:
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
                    "SELECT COUNT(*) as post_count FROM %s" % (TEST_TABLE2)
                )
                _post_count = await cur.fetchone()
                assert _post_count is not None
                post_count: int = _post_count["post_count"]
                add_after_post_count += post_count
                logger.info(f"After post_count: {post_count}")

        assert add_after_post_count == add_before_post_count

        # Incosistency test2: only one data
        # Check same data on multiple MySQL nodes
        logger.info("Inconsistency test with post datas")
        for index, item in enumerate(post_data):
            if index % self.LOG_STEP == 0:
                logger.info(f"Testing post data... {index}/{len(user_data)}")

            i = 0
            for container in cons:
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
                        "SELECT COUNT(*) as count FROM %s WHERE md5='%s'"
                        % (TEST_TABLE2, item["md5"])
                    )
                    _f = await cur.fetchone()
                    i += _f["count"]

            assert i == 1

    @pytest.mark.asyncio
    async def test_inconsistency2(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        """

        Add MySQL node one by one into mmy cluster,
        and check for correcting hashed data and placed node.

        """
        await up_mysql_docker_container(DockerMySQL.MySQL1)
        # Connection to MySQL without mmy proxy.
        async with aiomysql.connect(
            host=str(DockerMySQL.MySQL1.value.host),
            port=DockerMySQL.MySQL1.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            await self.generate_random_data(DockerMySQL.MySQL1, mmy_info)

            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
            init_post_data = await cur.fetchall()

        logger.info(f"Inserted test data: {TEST_TABLE2}")
        logger.info(f"Before post_count: {len(init_post_data)}")
        cons = get_mysql_docker_for_test()
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )

        # Add MySQL server into mmy cluster
        for index, container in enumerate(cons):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            if index > 0:
                # Delete data inserted when startup Docker container except MySQL1
                await delete_all_table(container, mmy_info)

            await self._adder_do(
                adder=adder,
                server=_server,
            )
            all_cluster_mysqls = cons[: index + 1]

            ring = MySQLRing(
                mysql_info=mmy_info.mysql,
                init_nodes=[
                    _Server(
                        host=con.value.host,
                        port=con.value.port,
                    )
                    for con in all_cluster_mysqls
                ],
                insert_duplicate_behavior=MySQLInsertDuplicateBehavior.Raise,
            )
            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for data_index, post in enumerate(init_post_data):
                if data_index % self.LOG_STEP == 0:
                    logger.info(f"Done: {data_index}/{len(init_post_data)}")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in all_cluster_mysqls:
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
                        _count = _f["count"]

                        if (
                            con.value.host == instance.host
                            and con.value.port == instance.port
                        ):
                            assert _count == 1
                        else:
                            assert _count == 0

    @pytest_asyncio.fixture
    async def init_add_mysql1(self, mmy_info):
        mysql_dockers = get_mysql_docker_for_test()
        for mysql_docker in mysql_dockers:
            await delete_all_table(mysql_docker, mmy_info)
        await self.generate_random_data(DockerMySQL.MySQL1, mmy_info)

        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        _server = _Server(
            host=DockerMySQL.MySQL1.value.host,
            port=DockerMySQL.MySQL1.value.port,
        )
        await self._adder_do(
            adder=adder,
            server=_server,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("step", list(AdderStep))
    async def test_rollback(
        self,
        step,
        mmy_info,
        etcd_flush_all_data,
        init_add_mysql1,
        mocker,
    ):
        async with aiomysql.connect(
            host=str(DockerMySQL.MySQL1.value.host),
            port=DockerMySQL.MySQL1.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
            init_post_data = await cur.fetchall()

        class Mock(Exception):
            pass

        match step:
            case AdderStep.Init:
                mocker.patch(
                    "mmy.mysql.add.MySQLAdder.do",
                    side_effect=Mock,
                )
            case AdderStep.FetchNodesInfo:
                mocker.patch(
                    "mmy.mysql.add.MySQLEtcdClient.get",
                    side_effect=Mock,
                )
            case AdderStep.HashRingCalculate:
                mocker.patch(
                    "mmy.mysql.add.MySQLRing.add",
                    side_effect=Mock,
                )
            case AdderStep.Ping:
                mocker.patch(
                    "mmy.mysql.add.MySQLRing.add",
                    side_effect=Mock,
                )
                mocker.patch(
                    "mmy.mysql.add.MySQLAdder.ping_mysql",
                    side_effect=Mock,
                )
            case AdderStep.PutNewNodeInfo:

                async def dummy_ring_add(
                    self: MySQLRing,
                    node: Server,
                ) -> list[Coroutine[None, None, MovedData | None]]:
                    _nodename: str = self.nodename_from_node(node)
                    self._node_hash[_nodename] = node
                    return []

                mocker.patch(
                    "mmy.mysql.add.MySQLRing.add",
                    side_effect=dummy_ring_add,
                    autospec=True,
                )
                mocker.patch(
                    "mmy.mysql.add.MySQLEtcdClient.add_new_node",
                    side_effect=Mock,
                )
            case AdderStep.DataMove:

                async def dummy_move():
                    raise Mock

                async def dummy_ring_add(
                    self: MySQLRing,
                    node: Server,
                ) -> list[Coroutine[None, None, MovedData | None]]:
                    _nodename: str = self.nodename_from_node(node)
                    self._node_hash[_nodename] = node
                    return [dummy_move()]

                mocker.patch(
                    "mmy.mysql.add.MySQLRing.add",
                    side_effect=dummy_ring_add,
                    autospec=True,
                )
            case AdderStep.UpdateNewNodeState | AdderStep.DataMoveAgain:
                mocker.patch(
                    "mmy.mysql.add.MySQLEtcdClient.update_state",
                    side_effect=Mock,
                )
            case AdderStep.DeleteFromOldNodes:
                mocker.patch(
                    "mmy.mysql.add.MySQLRing.delete_from_old_nodes",
                    side_effect=Mock,
                )
            case AdderStep.OptimizeTable:
                mocker.patch(
                    "mmy.mysql.add.MySQLRing.optimize_table_old_nodes",
                    side_effect=Mock,
                )
            case AdderStep.Done:
                return

            case _:
                raise RuntimeError

        _c = DockerMySQL.MySQL2.value
        _server = _Server(
            host=_c.host,
            port=_c.port,
        )
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        with pytest.raises(Mock):
            await self._adder_do(
                adder=adder,
                server=_server,
            )

        mocker.stopall()

        # Check rollback result
        etcd_cli = MySQLEtcdClient(
            host=mmy_info.etcd.nodes[0].host,
            port=mmy_info.etcd.nodes[0].port,
        )
        data = await etcd_cli.get()
        if step is AdderStep.OptimizeTable:
            assert len(data.nodes) == 2
        else:
            assert len(data.nodes) == 1  # This 1 is MySQL1 docker on test init

        async def rows_count_must_be_1_table2(host, port, _id):
            async with aiomysql.connect(
                host=str(host),
                port=port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:
                cur = await connect.cursor()
                await cur.execute(
                    "SELECT COUNT(*) as count FROM %s WHERE id='%s'"
                    % (TEST_TABLE2, _id)
                )
                _f = await cur.fetchone()
                assert _f["count"] == 1

        if step is AdderStep.OptimizeTable:
            ring = MySQLRing(
                mysql_info=mmy_info.mysql,
                init_nodes=[
                    _Server(
                        host=DockerMySQL.MySQL1.value.host,
                        port=DockerMySQL.MySQL1.value.port,
                    ),
                    _Server(
                        host=DockerMySQL.MySQL2.value.host,
                        port=DockerMySQL.MySQL2.value.port,
                    ),
                ],
            )
            for data in init_post_data:
                node = ring.get(data["id"])
                instance = node["instance"]
                await rows_count_must_be_1_table2(
                    host=instance.host,
                    port=instance.port,
                    _id=data["id"],
                )

        else:
            instance = DockerMySQL.MySQL1.value
            for data in init_post_data:
                await rows_count_must_be_1_table2(
                    host=instance.host,
                    port=instance.port,
                    _id=data["id"],
                )

    @pytest.mark.asyncio
    async def test_retry_connection(
        self,
        etcd_flush_all_data,
        init_add_mysql1,
        mocker,
        mmy_info,
        caplog,
    ):
        self.raise_count: int = 0

        async def ping():
            if self.raise_count < PING_RETRY_COUNT - 1:
                self.raise_count += 1
                _e = random.choice(
                    [
                        MySQLErrorCode.ER_CON_COUNT_ERROR,
                        MySQLErrorCode.ER_OUT_OF_RESOURCES,
                        MySQLErrorCode.ER_TOO_MANY_USER_CONNECTIONS,
                        MySQLErrorCode.ER_CR_CONNECTION_ERROR,
                    ]
                )
                raise OperationalError(
                    _e.value.error_code,
                    "this is mock ping error",
                )

            return

        mocker.patch(
            "mmy.mysql.add._get_ping_retry_interval_secs",
            return_value=0.01,
        )
        mocker.patch(
            "mmy.mysql.client.MySQLClient.ping",
            side_effect=ping,
        )

        _c = DockerMySQL.MySQL2.value
        _server = _Server(
            host=_c.host,
            port=_c.port,
        )
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        await self._adder_do(
            adder=adder,
            server=_server,
        )
        assert (
            len(list(filter(lambda x: x.levelno == logging.ERROR, caplog.records)))
            == PING_RETRY_COUNT - 1
        )

    @pytest.mark.asyncio
    async def test_inconsistency3(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
        mocker,
    ):
        INSERT_WHEN_MOVING_COUNT = 1 << 12
        BULK_INSERT_SIZE: int = 1000
        """

        When DataMove step, insert data into old nodes.
        And test for existing it in new node.
        This test for checking MoveDataAgain step works correctly.

        """
        from .test_ring import SAMPLE_MD5_ROWS_COUNT, sample_md5_bulk

        await delete_all_table(DockerMySQL.MySQL1, mmy_info)
        logger.info("Insert test data into first MySQL node")
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
            for index, bulk in enumerate(sample_md5_bulk(bulk_count=BULK_INSERT_SIZE)):
                logger.info(f"{index}: Insert size {index*BULK_INSERT_SIZE}")
                await cur.executemany(
                    query=f"INSERT INTO {TEST_TABLE2}(\
                        id, \
                        md5, \
                        title, \
                        duration, \
                        do_on \
                        ) VALUES (%(id)s, %(md5)s, %(title)s, %(duration)s, %(do_on)s)",
                    args=[
                        {
                            "id": i,
                            "md5": hashlib.md5(_get_random_bytes()).hexdigest(),
                            "title": hashlib.sha256(_get_random_bytes()).hexdigest(),
                            "duration": random.uniform(0, 10),
                            "do_on": datetime.now(timezone.utc),
                        }
                        for i in bulk
                    ],
                )

            await connect.commit()

        _original = MySQLAdder.actually_move_data
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        cons = get_mysql_docker_for_test()
        for index, container in enumerate(cons):
            logger.info(
                f"Test for adding container({container.name}) {index}/{len(cons)}"
            )
            if index > 0:
                # Delete data inserted when startup Docker container except MySQL1
                await delete_all_table(container, mmy_info)
            else:
                # When No node in mmy cluster, no move data.
                _c = container.value
                _server = _Server(
                    host=_c.host,
                    port=_c.port,
                )
                await self._adder_do(
                    adder=adder,
                    server=_server,
                )
                continue

            mocker.stopall()

            async def actually_move_data(*args, **kwargs):
                await _original(*args, **kwargs)
                _ring = adder.ring
                logger.info(
                    "Insert random rows with previous hash ring without move node"
                )
                logger.info("it emulates inserting data while moving data to new node")
                insert_by_node: dict[str, list[dict[str, Any]]] = dict()
                address_map: dict[str, Server] = dict()
                # Create insert data
                for i in range(INSERT_WHEN_MOVING_COUNT):
                    if i % self.LOG_STEP == 0:
                        logger.info(f"INSERT {i}/{INSERT_WHEN_MOVING_COUNT}")

                    _b = str(time.time_ns())
                    _id = hashlib.md5(_b.encode("utf-8")).hexdigest()
                    _title = hashlib.sha256(_b.encode("utf-8")).hexdigest()
                    _md5 = hashlib.md5(_id.encode("utf-8")).hexdigest()
                    _duration = random.uniform(0, 10)
                    _do_on = datetime.now(timezone.utc)

                    _instance: Server = get_already_hashed_key(
                        _ring._hr_without_move, _id
                    )
                    address_map[_instance.address_format()] = _instance
                    insert_by_node.setdefault(_instance.address_format(), list())
                    insert_by_node[_instance.address_format()].append(
                        {
                            "id": _id,
                            "md5": _md5,
                            "title": _title,
                            "duration": _duration,
                            "do_on": _do_on,
                        }
                    )

                _total: int = 0
                for address_format, values in insert_by_node.items():
                    _total += len(values)
                    _instance = address_map[address_format]
                    # mysql random generate
                    async with aiomysql.connect(
                        host=str(_instance.host),
                        port=_instance.port,
                        user=ROOT_USERINFO.user,
                        cursorclass=DictCursor,
                        password=ROOT_USERINFO.password,
                        db=mmy_info.mysql.db,
                    ) as connect:
                        cur = await connect.cursor()
                        async with cur:
                            await cur.executemany(
                                query=f"INSERT INTO {TEST_TABLE2}(\
                                    id, \
                                    md5, \
                                    title, \
                                    duration, \
                                    do_on \
                                    ) VALUES (%(id)s, %(md5)s, %(title)s, %(duration)s, %(do_on)s)",
                                args=values,
                            )
                        await connect.commit()

                assert _total == INSERT_WHEN_MOVING_COUNT

            mocker.patch(
                "mmy.mysql.add.MySQLAdder.actually_move_data",
                side_effect=actually_move_data,
                autospec=True,
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

            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )

            all_cluster_mysqls = cons[: index + 1]
            before_add_post_count: int = 0
            for _mysql in all_cluster_mysqls:
                async with aiomysql.connect(
                    host=str(_mysql.value.host),
                    port=_mysql.value.port,
                    user=ROOT_USERINFO.user,
                    cursorclass=DictCursor,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:

                    # Get all data
                    cur = await connect.cursor()
                    await cur.execute(
                        "SELECT COUNT(*) as count FROM %s" % (TEST_TABLE2)
                    )
                    res = await cur.fetchone()
                    before_add_post_count += res["count"]

            await self._adder_do(
                adder=adder,
                server=_server,
            )

            post_data: list[dict[str, Any]] = list()
            for _mysql in all_cluster_mysqls:
                async with aiomysql.connect(
                    host=str(_mysql.value.host),
                    port=_mysql.value.port,
                    user=ROOT_USERINFO.user,
                    cursorclass=DictCursor,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:
                    cur = await connect.cursor()
                    await cur.execute(
                        "SELECT COUNT(*) AS count FROM %s" % (TEST_TABLE2)
                    )
                    # Get all data
                    cur = await connect.cursor()
                    await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                    _r = await cur.fetchall()
                    post_data.extend(_r)
                    logger.info(f"{_mysql.name}: {len(_r)} rows")

            assert before_add_post_count + INSERT_WHEN_MOVING_COUNT == len(post_data)

            ring = MySQLRing(
                mysql_info=mmy_info.mysql,
                init_nodes=[
                    _Server(
                        host=con.value.host,
                        port=con.value.port,
                    )
                    for con in all_cluster_mysqls
                ],
                insert_duplicate_behavior=MySQLInsertDuplicateBehavior.Raise,
            )
            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for con in all_cluster_mysqls:
                logger.info(f"Test {con.name}")
                async with aiomysql.connect(
                    host=str(con.value.host),
                    port=con.value.port,
                    user=ROOT_USERINFO.user,
                    cursorclass=DictCursor,
                    password=ROOT_USERINFO.password,
                    db=mmy_info.mysql.db,
                ) as connect:
                    for data_index, post in enumerate(post_data):
                        if data_index % self.LOG_STEP == 0:
                            logger.info(f"Done: {data_index}/{len(post_data)}")

                        _id: str = post["id"]  # not AUTO_INCREMENT
                        instance = get_already_hashed_key(ring._hr, _id)

                        cur = await connect.cursor()
                        await cur.execute(
                            f"SELECT COUNT(*) as count FROM {TEST_TABLE2} WHERE id=%(_id)s",
                            {
                                "_id": _id,
                            },
                        )
                        _f = await cur.fetchone()
                        _count = _f["count"]

                        try:
                            if (
                                con.value.host == instance.host
                                and con.value.port == instance.port
                            ):
                                assert _count == 1
                            else:
                                assert _count == 0
                        except AssertionError as e:
                            logger.info("#################")
                            logger.info(
                                cur.mogrify(
                                    f"SELECT COUNT(*) as count FROM {TEST_TABLE2} WHERE id=%(_id)s",
                                    {
                                        "_id": _id,
                                    },
                                )
                            )
                            logger.info(_id)
                            logger.info(con.name)
                            logger.info(instance)
                            raise e
