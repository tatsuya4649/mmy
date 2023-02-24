import logging
from pprint import pformat
from typing import Any

from pymysql.err import IntegrityError

from ..etcd import MmyEtcdInfo, MySQLEtcdClient, MySQLEtcdData
from ..ring import Md, MySQLRing, RingMeta
from ..server import Server
from .client import (
    MmyMySQLInfo,
    MySQLClient,
    MySQLColumns,
    MySQLFetchAll,
    MySQLInsertDuplicateBehavior,
    MySQLKeys,
    TableName,
)
from .errcode import MySQLErrorCode
from .sql import SQLPoint

logger = logging.getLogger(__name__)


class MySQLReshard(RingMeta):
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
        insert_duplicate_behavior: MySQLInsertDuplicateBehavior = MySQLInsertDuplicateBehavior.Raise,
    ):
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info
        self._ring: MySQLRing | None = None
        self._insert_duplicate_behavior = insert_duplicate_behavior

    async def get_mysql_nodes(self) -> MySQLEtcdData:
        for node in self._etcd_info.nodes:
            cli = MySQLEtcdClient(
                host=node.host,
                port=node.port,
            )
            mysql_info: MySQLEtcdData = await cli.get()
            return mysql_info
        else:
            raise RuntimeError("Can't get MySQL info from etcd cluster")

    @property
    def ring(self) -> MySQLRing:
        assert self._ring is not None
        return self._ring

    async def insert_frag(
        self,
        from_server: Server,
        table: TableName,
        frag: list[dict[str, Any]],
        keys: list[MySQLKeys],
    ):
        primary_key: str = keys[0].Column_name
        data_by_nodes: dict[str, list[dict[str, Any]]] = dict()
        server_from_address_format: dict[str, Server] = dict()
        for f in frag:
            _i: Server = self.ring.get_instance(f[primary_key])
            _nodename: str = _i.address_format()
            data_by_nodes.setdefault(_nodename, list())
            data_by_nodes[_nodename].append(f)
            server_from_address_format[_nodename] = _i

        for nodename, frag in data_by_nodes.items():
            server = server_from_address_format[nodename]
            assert server != from_server

            client = MySQLClient(
                host=server.host,
                port=server.port,
                auth=self._mysql_info,
            )
            _insert_ignore: bool = True
            await client.insert(
                table=table,
                datas=frag,
                insert_ignore=_insert_ignore,
            )
        return

    async def _do_reshard_node(self, mysql_node: Server) -> list[Md]:
        client = MySQLClient(
            host=mysql_node.host,
            port=mysql_node.port,
            auth=self._mysql_info,
        )
        mds: list[Md] = self.ring.not_owner_points(mysql_node)
        for item in mds:
            item.node = mysql_node

        logger.info(f"Not owned point: {len(mds)} points")
        for index, md in enumerate(mds):
            # Not owner point must not be in this node
            assert md.node == mysql_node
            if index % 10 == 0:
                logger.info(f"Reshared complete: {index}/{len(mds)}")

            for table in self.ring.table_names():
                logger.info(
                    f'Move "{table}": {md.start_point[:self.LENGTH]}~{md.end_point[:self.LENGTH]} from {mysql_node.address_format()}'
                )
                keys: list[MySQLKeys] = await client.primary_keys_info(table)
                not_owner = await client.consistent_hashing_select(
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
                async for frag in not_owner:
                    logger.info(f"Not owned data count: {len(frag)}")
                    await self.insert_frag(
                        from_server=mysql_node,
                        table=table,
                        frag=frag,
                        keys=keys,
                    )

        return mds

    async def do_delete_unnecessary(
        self,
        mds_node: dict[str, list[Md]],
        nodename_map: dict[str, Server],
    ):
        logger.info("Delete unnecessary data")
        for nodename, mds in mds_node.items():
            mysql_node = nodename_map[nodename]
            client = MySQLClient(
                host=mysql_node.host,
                port=mysql_node.port,
                auth=self._mysql_info,
            )
            for table in self.ring.table_names():
                for md in mds:
                    assert mysql_node == md.node
                    deleted_count: int = await client.consistent_hashing_delete(
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
                    logger.info(
                        f'Delete unnecessary "{table}": {md.start_point[:self.LENGTH]}~{md.end_point[:self.LENGTH]} from {mysql_node.address_format()}: {deleted_count}rows'
                    )
        return

    async def do(
        self,
        delete_unnecessary: bool = False,
    ):
        mysql_data: MySQLEtcdData = await self.get_mysql_nodes()
        self._ring = MySQLRing(
            init_nodes=mysql_data.nodes,
            mysql_info=self._mysql_info,
        )
        mds_node: dict[str, list[Md]] = dict()
        nodename_map: dict[str, Server] = dict()
        for mysql_node in mysql_data.nodes:
            logger.info(f"Reshard MySQL node: {mysql_node.address_format()}")
            mds: list[Md] = await self._do_reshard_node(mysql_node)
            _nodename = mysql_node.address_format()
            mds_node[_nodename] = mds
            nodename_map[_nodename] = mysql_node

        if delete_unnecessary:
            await self.do_delete_unnecessary(
                mds_node=mds_node,
                nodename_map=nodename_map,
            )
