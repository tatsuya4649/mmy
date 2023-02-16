from typing import Coroutine

from loguru import logger
from rich import print

from mmy.etcd import MySQLEtcdClient, MySQLEtcdData, MySQLEtcdDuplicateNode
from mmy.log import init_log
from mmy.ring import MySQLRing, Node
from mmy.server import State, _Server, address_from_server, state_rich_str


async def _add_main(server: _Server):
    try:
        logger.info("Add new node: %s..." % (address_from_server(server)))
        etcd = MySQLEtcdClient()
        datas: tuple[MySQLEtcdData, MySQLEtcdData] = await etcd.add_new_node(server)
        new_data, old_data = datas
        _ring = MySQLRing(
            init_nodes=map(lambda x: Node(host=x.host, port=x.port), old_data.nodes),
        )
        node = Node(
            host=server.host,
            port=server.port,
        )
        move: Coroutine[None, None, None] = await _ring.add(
            node=node,
        )
        await move
        logger.info("Inserted MySQL data from existed node into new node")
        logger.info("Update state of new MySQL node on etcd")
        await etcd.update_state(
            server=server,
            state=State.Run,
        )
        logger.info("New MySQL state: %s" % (State.Run.value))

        logger.info(
            "Delete unnecessary data(non owner of hashed) from existed MySQL node"
        )
        await _ring.delete_from_old_nodes(
            new_node=node,
        )
        logger.info("Optimize MySQL nodes with data moved")
        await _ring.optimize_table_old_nodes(new_node=node)
        logger.info("Done !")
    except MySQLEtcdDuplicateNode:
        logger.error("This IP address already exists on etcd")
