import hashlib
import logging
import random
from copy import copy
from typing import Any

import aiomysql
import pytest
import pytest_asyncio
from aiomysql.cursors import DictCursor
from mmy.mysql.add import PingType
from mmy.mysql.client import MySQLClient, MySQLInsertDuplicateBehavior
from mmy.mysql.delete import MySQLDeleter, MySQLDeleterNoServer
from mmy.mysql.sql import SQLPoint
from mmy.parse import MmyYAML
from mmy.ring import MySQLRing
from mmy.server import Server, State, _Server
from rich import print

from ._mysql import ROOT_USERINFO, TEST_TABLE2, mmy_info
from .test_etcd import etcd_flush_all_data, etcd_put_mysql_container
from .test_mysql import (
    DockerMySQL,
    delete_all_table,
    fix_down_all_mysql_docker_containers,
    get_mysql_docker_for_test,
    up_mysql_docker_container,
)

logger = logging.getLogger(__name__)


class TestDelete:
    GENERAETE_REPEAT: int = 7
    MIN_COUNT: int = 100000
    sleep_secs_before_datamove_again: float = 0.1

    @pytest.mark.asyncio
    async def test_only_delete(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        mysql_containers = get_mysql_docker_for_test()
        for container in mysql_containers:
            await up_mysql_docker_container(container)
            await delete_all_table(container, mmy_info)

        await etcd_put_mysql_container(mysql_containers)
        for index, container in enumerate(mysql_containers):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            deleter = MySQLDeleter(
                mysql_info=mmy_info.mysql,
                etcd_info=mmy_info.etcd,
            )
            if index == len(mysql_containers) - 1:
                with pytest.raises(MySQLDeleterNoServer):
                    await deleter.do(
                        server=_server,
                        ping_type=PingType.OnlyTargetServer,
                        delete_data=True,
                    )
            else:
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
                await delete_all_table(container, mmy_info)

                cur = await connect.cursor()
                await cur.execute(
                    "CALL generate_random_post(%d, %d)"
                    % (self.GENERAETE_REPEAT, self.MIN_COUNT)
                )
                await connect.commit()

        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=[
                _Server(
                    host=con.value.host,
                    port=con.value.port,
                )
                for con in mysql_containers
            ],
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.Raise,
        )
        # Delete not owned data from MySQL containers
        for container in mysql_containers:
            node = Server(
                host=container.value.host,
                port=container.value.port,
                state=State.Unknown,
            )
            for md in ring.not_owner_points(node):
                cli = MySQLClient(
                    host=node.host,
                    port=node.port,
                    auth=mmy_info.mysql,
                )
                for table in ring.table_names():
                    await cli.consistent_hashing_delete(
                        table=table,
                        start=SQLPoint(
                            point=md.start_point,
                            equal=md.start_equal,
                            greater=True,
                            less=False,
                        ),
                        end=SQLPoint(
                            point=md.end_point,
                            equal=md.end_equal,
                            greater=False,
                            less=True,
                        ),
                        _or=md._or,
                    )

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

        delete_before_post_count = len(post_data)
        delete_before_user_count = len(user_data)

        cons = copy(mysql_containers)
        cons.pop(0)
        deleter = MySQLDeleter(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        for index, container in enumerate(cons):
            logger.info(f"Delete {container.value.service_name}")
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
            delete_after_post_count: int = 0
            for container in mysql_containers:
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
                    delete_after_post_count += post_count
                    logger.info(f"post_count: {post_count}")

            logger.info(f"Inserted test data: {TEST_TABLE2}")
            logger.info(f"\tbefore post_count: {delete_before_post_count}")
            logger.info(f"\tafter post_count: {delete_after_post_count}")
            assert delete_after_post_count == delete_before_post_count

        # Incosistency test2: only one data
        # Check same data on multiple MySQL nodes
        logger.info("Inconsistency test with post datas")
        for data_index, item in enumerate(post_data):
            if data_index % 100 == 0:
                logger.info(f"Done: {data_index/len(post_data)}")

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
                        f"SELECT COUNT(*) as count FROM {TEST_TABLE2} WHERE md5=%(md5)s",
                        {
                            "md5": item["md5"],
                        },
                    )
                    _f = await cur.fetchone()
                    i += _f["count"]

            assert i == 1

    @pytest.mark.asyncio
    async def test_inconsistency2(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
        before_deleted_nodes,
    ):
        """

        Delect MySQL node one by one from mmy cluster,
        and check for correctiong hashed data and placed node.

        """
        user_data: list[dict[str, Any]] = list()
        post_data: list[dict[str, Any]] = list()

        mysql_containers = get_mysql_docker_for_test()
        for container in mysql_containers:
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

        logger.info(f"Inserted test data: {TEST_TABLE2}")
        logger.info(f"Before post_count: {len(post_data)}")

        cons = get_mysql_docker_for_test()
        deleter = MySQLDeleter(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        for index, container in enumerate(cons):
            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )
            if index == len(cons) - 1:
                with pytest.raises(MySQLDeleterNoServer):
                    await deleter.do(
                        server=_server,
                        delete_data=True,
                    )
                return
            else:
                await deleter.do(
                    server=_server,
                    delete_data=True,
                )

            ring = MySQLRing(
                mysql_info=mmy_info.mysql,
                init_nodes=[
                    _Server(
                        host=con.value.host,
                        port=con.value.port,
                    )
                    for con in cons[index + 1 :]
                ],
                insert_duplicate_behavior=MySQLInsertDuplicateBehavior.Raise,
            )

            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for data_index, post in enumerate(post_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {data_index/len(post_data)}")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in cons[index + 1 :]:
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
    async def before_deleted_nodes(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
    ):
        """
        Setup all MySQL nodes with random data,
        and calculate hash ring and delete data that must not be owned.
        """
        mysql_containers = get_mysql_docker_for_test()
        await etcd_put_mysql_container(mysql_containers)
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
                await delete_all_table(container, mmy_info)

                cur = await connect.cursor()
                await cur.execute(
                    "CALL generate_random_post(%d, %d)"
                    % (self.GENERAETE_REPEAT, self.MIN_COUNT)
                )
                await connect.commit()

        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=[
                _Server(
                    host=con.value.host,
                    port=con.value.port,
                )
                for con in mysql_containers
            ],
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.Raise,
        )
        # Delete not owned data from MySQL containers
        for contianer in mysql_containers:
            node = Server(
                host=contianer.value.host,
                port=contianer.value.port,
                state=State.Unknown,
            )
            for md in ring.not_owner_points(node):
                cli = MySQLClient(
                    host=node.host,
                    port=node.port,
                    auth=mmy_info.mysql,
                )
                for table in ring.table_names():
                    await cli.consistent_hashing_delete(
                        table=table,
                        start=SQLPoint(
                            point=md.start_point,
                            equal=md.start_equal,
                            greater=True,
                            less=False,
                        ),
                        end=SQLPoint(
                            point=md.end_point,
                            equal=md.end_equal,
                            greater=False,
                            less=True,
                        ),
                        _or=md._or,
                    )
        return

    def _deleter_do(
        self,
        deleter: MySQLDeleter,
        server: _Server,
    ):
        return deleter.do(
            server=server,
            sleep_secs_before_datamove_again=self.sleep_secs_before_datamove_again,
            delete_data=True,
        )

    @pytest.mark.asyncio
    async def test_inconsistency3(
        self,
        etcd_flush_all_data,
        mmy_info: MmyYAML,
        before_deleted_nodes,
        mocker,
    ):
        COUNT = 1 << 10
        """

        When DataMove step, insert data into deleted nodes.
        And test for existing inserted data in deleted node.
        This test for checking MoveDataAgain step works correctly.

        """
        cons = get_mysql_docker_for_test()
        delete_cons = cons[1:]
        _original = MySQLDeleter.actually_move_data
        deleter = MySQLDeleter(
            mysql_info=mmy_info.mysql,
            etcd_info=mmy_info.etcd,
        )
        for index, container in enumerate(delete_cons):
            mocker.stopall()
            before_delete_user_count: int = 0
            before_delete_post_count: int = 0

            async def actually_move_data(*args, **kwargs):
                _deleter: MySQLDeleter = args[0]
                assert isinstance(_deleter, MySQLDeleter)
                res = await _original(*args, **kwargs)
                # Include deleted node
                _prev_ring = deleter.prev_ring
                logger.info(
                    "Insert random rows with previous hash ring without move node"
                )
                logger.info("it emulates inserting data while moving data to new node")

                for i in range(COUNT):
                    if i % 100 == 0:
                        logger.info(f"INSERT {int(100*(i/COUNT))}%")

                    while True:
                        _id = random.randint(1000000000, 2000000000)
                        _node = _prev_ring.get(data=_id)
                        _instance: Server = _node["instance"]
                        if (
                            _instance.address_format()
                            != _deleter.server.address_format()
                        ):
                            continue

                        _bid = _id.to_bytes(4, "little")
                        _name = hashlib.sha256(_bid).hexdigest()
                        _md5 = hashlib.md5(_bid).hexdigest()
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
                                    f"INSERT INTO {TEST_TABLE2}(id, title, md5) VALUES (%s, %s, %s)",
                                    ((_id, _name, _md5)),
                                )
                            await connect.commit()

                        break

                return res

            mocker.patch(
                "mmy.mysql.delete.MySQLDeleter.actually_move_data",
                side_effect=actually_move_data,
                autospec=True,
            )

            _c = container.value
            _server = _Server(
                host=_c.host,
                port=_c.port,
            )

            for _mysql in cons:
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
                    before_delete_post_count += res["count"]

            await self._deleter_do(
                deleter=deleter,
                server=_server,
            )

            user_data: list[dict[str, Any]] = list()
            post_data: list[dict[str, Any]] = list()
            for _mysql in cons:
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
                    async with cur:
                        await cur.execute("SELECT * FROM %s" % (TEST_TABLE2))
                        res = await cur.fetchall()
                        post_data.extend(res)

            async with aiomysql.connect(
                host=str(_server.host),
                port=_server.port,
                user=ROOT_USERINFO.user,
                cursorclass=DictCursor,
                password=ROOT_USERINFO.password,
                db=mmy_info.mysql.db,
            ) as connect:

                # Get all data
                cur = await connect.cursor()
                await cur.execute("SELECT COUNT(*) as count FROM %s" % (TEST_TABLE2))
                res = await cur.fetchone()
                assert res["count"] == 0

            assert before_delete_user_count + COUNT == len(user_data)
            assert before_delete_post_count + COUNT == len(post_data)

            ring = deleter.ring
            logger.info(f"Inconsistency test about init {TEST_TABLE2} data")
            for data_index, post in enumerate(post_data):
                if data_index % 100 == 0:
                    logger.info(f"Done: {data_index/len(post_data)}")

                key = post["id"]  # not AUTO_INCREMENT
                nodeinfo = ring.get(key)
                instance = nodeinfo["instance"]

                for con in cons:
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
                        if not (
                            con.value.host == instance.host
                            and con.value.port == instance.port
                        ):
                            assert len(res) == 1
                        else:
                            assert len(res) == 0
