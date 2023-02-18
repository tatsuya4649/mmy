import asyncio
import logging
from enum import Enum, auto
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
from mmy.mysql.client import MmyMySQLInfo, MySQLClient
from mmy.ring import MySQLRing, Node, RingNoMoveYet
from mmy.server import State, _Server, address_from_server, state_rich_str

logger = logging.getLogger(__name__)


class PingType(Enum):
    OnlyTargetServer = auto()
    AllServer = auto()
    NonPing = auto()


class MySQLAdder:
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
                prev_nodes_length: int = len(_ring)
                node = Node(
                    host=server.host,
                    port=server.port,
                )
                moves: list[Coroutine[None, None, None]] = await _ring.add(
                    node=node,
                )
                new_nodes_length: int = len(_ring)
                logger.info(f"Moving data fragments count: {len(moves)}")
                logger.info(f"Prev nodes count: {prev_nodes_length}")
                logger.info(f"New nodes count: {new_nodes_length}")
                assert new_nodes_length == prev_nodes_length + 1

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

                logger.info("Add new node: %s..." % (address_from_server(server)))
                try:
                    await etcd_cli.add_new_node(server)

                    logger.info(f"Moving data fragments count: {len(moves)}")
                    for index, move in enumerate(tqdm(moves)):
                        logger.info(f"Move index: {index}")
                        await move

                    logger.info("Inserted MySQL data from existed node into new node")
                    logger.info("Update state of new MySQL node on etcd")
                    await etcd_cli.update_state(
                        server=server,
                        state=State.Run,
                    )
                    logger.info("New MySQL state: %s" % (State.Run.value))

                    logger.info(
                        "Delete unnecessary data(non owner of hashed) from existed MySQL node"
                    )
                    try:
                        await _ring.delete_from_old_nodes(
                            new_node=node,
                        )
                        logger.info("Optimize MySQL nodes with data moved")
                        await _ring.optimize_table_old_nodes(
                            new_node=node,
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
