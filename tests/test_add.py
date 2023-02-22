import hashlib
import logging
import random
import time
from typing import Any, Coroutine

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.etcd import MySQLEtcdClient
from mmy.mysql.add import PING_RETRY_COUNT, AdderStep, MySQLAdder
from mmy.mysql.client import MySQLClient, MySQLInsertDuplicateBehavior
from mmy.mysql.errcode import MySQLErrorCode
from mmy.parse import MmyYAML
from mmy.ring import MovedData, MySQLRing
from mmy.server import Server, _Server
from pymysql.err import OperationalError
from rich import print

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
    GENERAETE_REPEAT: int = 9
    MIN_COUNT: int = 10000
    sleep_secs_before_datamove_again: float = 0.1
    SLEEP_SECS_AFTER_ETCD_ADD_NEW_NODE: float = 0.1

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
            await self.delete_all_table(DockerMySQL.MySQL1, mmy_info)
            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_user(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

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

            cur = await connect.cursor()
            await cur.execute("ALTER TABLE `%s`  AUTO_INCREMENT = 1" % (TEST_TABLE1))
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute("ALTER TABLE `%s`  AUTO_INCREMENT = 1" % (TEST_TABLE2))
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
            await self.delete_all_table(DockerMySQL.MySQL1, mmy_info)
            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_user(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
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
        logger.info(f"Before user_count: {add_before_user_count}")
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
                await self.delete_all_table(container, mmy_info)

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
                logger.info(f"After user_count: {user_count}")
                logger.info(f"After post_count: {post_count}")

        assert add_after_user_count == add_before_user_count
        assert add_after_post_count == add_before_post_count

        # Incosistency test2: only one data
        # Check same data on multiple MySQL nodes
        logger.info("Inconsistency test with user datas")
        for index, item in enumerate(user_data):
            if index % 100 == 0:
                logger.info(f"Testing user data... {index}/{len(user_data)}")

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
                        % (TEST_TABLE1, item["md5"])
                    )
                    _f = await cur.fetchone()
                    i += _f["count"]

            assert i == 1

        logger.info("Inconsistency test with post datas")
        for index, item in enumerate(post_data):
            if index % 100 == 0:
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

            # Get all data
            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
            init_user_data = await cur.fetchall()

            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
            init_post_data = await cur.fetchall()

        logger.info(f"Inserted test data: {TEST_TABLE1}, {TEST_TABLE2}")
        logger.info(f"Before user_count: {len(init_user_data)}")
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
                await self.delete_all_table(container, mmy_info)

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
            logger.info(f"Inconsistency test about init {TEST_TABLE1} data")
            for data_index, user in enumerate(init_user_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {data_index/len(init_user_data)}")

                key = user["id"]  # This is auto_increment
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in all_cluster_mysqls:
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
                                f"SELECT COUNT(*) as count FROM {TEST_TABLE1} WHERE id=%(id)s",
                                {
                                    "id": key,
                                },
                            )
                            _f = await cur.fetchone()
                            assert _f["count"] == 1

                            if not (
                                con.value.host == instance.host
                                and con.value.port == instance.port
                            ):
                                raise RuntimeError(
                                    f"Data({key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')

            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for data_index, post in enumerate(init_post_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {data_index/len(init_post_data)}")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in all_cluster_mysqls:
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
                                raise RuntimeError(
                                    f"Data({key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')

    @pytest_asyncio.fixture
    async def init_add_mysql1(self, mmy_info):
        mysql_dockers = get_mysql_docker_for_test()
        for mysql_docker in mysql_dockers:
            await self.delete_all_table(mysql_docker, mmy_info)
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
            # Get all data
            cur = await connect.cursor()
            await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
            init_user_data = await cur.fetchall()

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

        async def rows_count_must_be_1_table1(host, port, md5):
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
                    "SELECT COUNT(*) as count FROM %s WHERE md5='%s'"
                    % (TEST_TABLE1, md5)
                )
                _f = await cur.fetchone()
                assert _f["count"] == 1

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
            for data in init_user_data:
                node = ring.get(data["id"])
                instance = node["instance"]
                await rows_count_must_be_1_table1(
                    host=instance.host,
                    port=instance.port,
                    md5=data["md5"],
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
            for data in init_user_data:
                await rows_count_must_be_1_table1(
                    host=instance.host,
                    port=instance.port,
                    md5=data["md5"],
                )

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
        COUNT = 1 << 12
        """

        When DataMove step, insert data into old nodes.
        And test for existing it in new node.
        This test for checking MoveDataAgain step works correctly.

        """
        await self.delete_all_table(DockerMySQL.MySQL1, mmy_info)
        async with aiomysql.connect(
            host=str(DockerMySQL.MySQL1.value.host),
            port=DockerMySQL.MySQL1.value.port,
            user=ROOT_USERINFO.user,
            cursorclass=DictCursor,
            password=ROOT_USERINFO.password,
            db=mmy_info.mysql.db,
        ) as connect:
            # Get all data
            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_user(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

            cur = await connect.cursor()
            await cur.execute(
                "CALL generate_random_post(%d, %d)"
                % (self.GENERAETE_REPEAT, self.MIN_COUNT)
            )
            await connect.commit()

        _original = MySQLAdder.actually_move_data
        adder = MySQLAdder(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        cons = get_mysql_docker_for_test()
        for index, container in enumerate(cons):
            if index > 0:
                # Delete data inserted when startup Docker container except MySQL1
                await self.delete_all_table(container, mmy_info)
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
                for i in range(COUNT):
                    if i % 100 == 0:
                        logger.info(f"INSERT {int(100*(i/COUNT))}%")
                    _id = random.randint(1000000000, 2000000000)
                    _bid = _id.to_bytes(4, "little")
                    _name = hashlib.sha256(_bid).hexdigest()
                    _md5 = hashlib.md5(_bid).hexdigest()

                    _node = _ring.get_without_move(data=_id)
                    _instance = _node["instance"]
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
                            await cur.execute(
                                f"INSERT INTO {TEST_TABLE1}(id, name, md5) VALUES (%s, %s, %s)",
                                ((_id, _name, _md5)),
                            )
                        await connect.commit()

                        cur = await connect.cursor()
                        async with cur:
                            await cur.execute(
                                f"INSERT INTO {TEST_TABLE2}(id, title, md5) VALUES (%s, %s, %s)",
                                ((_id, _name, _md5)),
                            )
                        await connect.commit()

            mocker.patch(
                "mmy.mysql.add.MySQLAdder.actually_move_data",
                side_effect=actually_move_data,
                autospec=True,
            )

            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )

            all_cluster_mysqls = cons[: index + 1]
            before_add_user_count: int = 0
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
                        "SELECT COUNT(*) as count FROM %s" % (TEST_TABLE1)
                    )
                    res = await cur.fetchone()
                    before_add_user_count += res["count"]
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

            user_data: list[dict[str, Any]] = list()
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

                    # Get all data
                    cur = await connect.cursor()
                    await cur.execute("SELECT * FROM %s" % (TEST_TABLE1))
                    user_data.extend(await cur.fetchall())

                    cur = await connect.cursor()
                    await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                    post_data.extend(await cur.fetchall())

            assert before_add_user_count + COUNT == len(user_data)
            assert before_add_post_count + COUNT == len(post_data)

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
            logger.info(f"Inconsistency test about init {TEST_TABLE1} data")
            for data_index, user in enumerate(user_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {data_index/len(user_data)}")

                key = user["id"]  # This is auto_increment
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in all_cluster_mysqls:
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
                                f"SELECT COUNT(*) as count FROM {TEST_TABLE1} WHERE id=%(id)s",
                                {
                                    "id": key,
                                },
                            )
                            _f = await cur.fetchone()
                            assert _f["count"] == 1

                            if not (
                                con.value.host == instance.host
                                and con.value.port == instance.port
                            ):
                                raise RuntimeError(
                                    f"Data({key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')

            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for data_index, post in enumerate(post_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {int(100*(data_index/len(post_data)))}")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in all_cluster_mysqls:
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
                                raise RuntimeError(
                                    f"Data({key}) must be in {instance.address_format()} but, in {con.value.address_format}"
                                )
                            else:
                                break  # Pass
                    except AssertionError:
                        continue

                else:
                    raise RuntimeError(f'Not found data: "{key}"')
