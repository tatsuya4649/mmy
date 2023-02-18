import asyncio
import logging
from typing import Coroutine

from rich import print
from tqdm import tqdm

from mmy.etcd import (
    EtcdConnectError,
    MmyEtcdInfo,
    MySQLEtcdClient,
    MySQLEtcdData,
    MySQLEtcdDuplicateNode,
)
from mmy.mysql.client import MmyMySQLInfo, MySQLClient, TableName
from mmy.ring import MovedData, MySQLRing, Node, RingNoMoveYet
from mmy.server import State, _Server, address_from_server, state_rich_str

from .add import PingType

logger = logging.getLogger(__name__)


class MySQLDeleterNoServer(ValueError):
    pass


class MySQLDeleter:
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
    ):
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info

    async def do(
        self,
        server: _Server,
        ping_type: PingType = PingType.AllServer,
        delete_data: bool = True,
    ):
        async def ping_mysql(_s: _Server):
            try:
                server_cli = MySQLClient(
                    host=_s.host,
                    port=_s.port,
                    auth=self._mysql_info,
                )
                await server_cli.ping()
            except asyncio.TimeoutError:
                logger.error(
                    f"PING error with new MySQL server: {server.address_format()}"
                )

        try:
            for etcd in self._etcd_info.nodes:
                try:
                    etcd_cli = MySQLEtcdClient(
                        host=etcd.host,
                        port=etcd.port,
                    )
                    now_data: MySQLEtcdData = await etcd_cli.get()
                except EtcdConnectError:
                    continue

                _ring = MySQLRing(
                    mysql_info=self._mysql_info,
                    init_nodes=map(
                        lambda x: Node(host=x.host, port=x.port), now_data.nodes
                    ),
                )
                node = Node(
                    host=server.host,
                    port=server.port,
                )
                prev_nodes_length: int = len(_ring)
                moves: list[Coroutine[None, None, None]] = await _ring.delete(
                    node=node,
                )
                new_nodes_length: int = len(_ring)
                if new_nodes_length == 0:
                    raise MySQLDeleterNoServer(
                        "If delete this server, there won't be one left"
                    )
                logger.info(f"Moving data fragments count: {len(moves)}")
                logger.info(f"Prev nodes count: {prev_nodes_length}")
                logger.info(f"New nodes count: {new_nodes_length}")
                assert new_nodes_length == prev_nodes_length - 1

                match ping_type:
                    case PingType.OnlyTargetServer:
                        ping_nodes: list[Node] = list()
                        for mdp in _ring.from_to_data:
                            _from = mdp._from.node
                            _to = mdp._to

                            if _from not in ping_nodes:
                                ping_nodes.append(_from)
                            if _to not in ping_nodes:
                                ping_nodes.append(_to)

                        for node in ping_nodes:
                            await ping_mysql(node)
                    case PingType.AllServer:
                        etcd_cli = MySQLEtcdClient(
                            host=etcd.host,
                            port=etcd.port,
                        )
                        data: MySQLEtcdData = await etcd_cli.get()
                        for _node in data.nodes:
                            await ping_mysql(_node)
                    case PingType.NonPing:
                        pass
                    case _:
                        raise ValueError

                logger.info("Delete new node: %s..." % (address_from_server(server)))
                try:
                    logger.info("Update state of deleted MySQL node on etcd")
                    await etcd_cli.update_state(
                        server=server,
                        state=State.Move,
                    )
                    logger.info("New MySQL state: %s" % (State.Run.value))

                    moved_datas: list[MovedData] = list()
                    for index, move in enumerate(tqdm(moves)):
                        logger.info(f"Move index: {index}")
                        moved_data = await move
                        assert moved_data is not None
                        moved_datas.append(moved_data)

                    self.merge_moved_data(moved_datas)

                    if delete_data:
                        logger.info(
                            "Delete unnecessary data(non owner of hashed) from deleted MySQL node"
                        )
                        try:
                            await _ring.delete_data_from_deleted_node(
                                node=node,
                            )
                            logger.info("Optimize MySQL nodes with data moved")
                            await _ring.optimize_table_deleted_node(
                                node=node,
                            )
                        except RingNoMoveYet:
                            logger.info("No moved data")

                    logger.info("Done !")
                    return
                except EtcdConnectError:
                    continue
            else:
                raise EtcdConnectError("No alive etcd nodes")
        except MySQLEtcdDuplicateNode:
            logger.error("This IP address already exists on etcd")

    def merge_moved_data(self, datas: list[MovedData]):
        _d: dict[TableName, int] = dict()
        for data in datas:
            for by_table in data.tables:
                _d.setdefault(by_table.tablename, 0)
                _d[by_table.tablename] += by_table.moved_count

        for table, count in _d.items():
            logger.info("Moved data info:")
            logger.info(f"\tTable: {table}, Count: {count}")
