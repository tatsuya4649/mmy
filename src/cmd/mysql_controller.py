import asyncio
import ipaddress
from cmd import Cmd
from typing import Coroutine

import click
from loguru import logger
from rich import print
from src.etcd import (
    MySQLEtcdClient,
    MySQLEtcdData,
    MySQLEtcdDuplicateNode,
    MySQLEtcdNotFoundNode,
)
from src.log import init_log
from src.ring import MySQLRing, Node
from src.server import State, _Server, address_from_server, state_rich_str
from src.state import state_main


async def _delete_main(server: _Server):
    try:
        logger.info("Delete node: %s..." % (address_from_server(server)))
        etcd = MySQLEtcdClient()
        await etcd.delete_node(server)
        logger.info("Done !")
    except MySQLEtcdNotFoundNode:
        logger.error("Not found this node on etcd")


def _parse_server(line: str) -> _Server:
    ip, _, port = line.rpartition(":")
    ipa = ipaddress.ip_address(ip)
    _s: _Server = _Server(
        host=ipa,
        port=int(port),
    )
    return _s


def delete_main(line: str):
    try:
        _s = _parse_server(line)
        asyncio.run(_delete_main(_s))
    except ValueError as e:
        logger.error("IP address error: %s" % (e))
        print("[bold]Try agin[/bold]")


from enum import Enum, auto
from typing import Any

import httpx


class APIType(Enum):
    Added = auto()
    Update = auto()


class APIResponseError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        message: bytes,
    ):
        self.status_code: int = status_code
        self.message: bytes = message

    pass


_ADD = "http://127.0.0.1:8000"


async def notification_app_via_api(_type: APIType, jdata: dict[str, Any] | None = None):
    match _type:
        case APIType.Added:
            async with httpx.AsyncClient() as client:
                logger.info(
                    "Notification that is added new MySQL node to app server via API"
                )
                response = await client.post(
                    _ADD + "/add/node",
                    json=jdata,
                )
                if not response.is_success:
                    raise APIResponseError(
                        status_code=response.status_code,
                        message=response.content,
                    )

                logger.debug(
                    f"Response from app server: [{response.status_code}]: {response.json()}"
                )
                assert jdata is not None
                _s = _Server(
                    host=jdata["host"],
                    port=jdata["port"],
                )
                logger.info(
                    f"OK, All App servers confirmed that the new node({address_from_server(_s)}) was added"
                )
        case APIType.Update:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    _ADD + "/update",
                )
                if not response.is_success:
                    raise APIResponseError(
                        status_code=response.status_code,
                        message=response.content,
                    )
                logger.debug(
                    f"Response from app server: [{response.status_code}]: {response.json()}"
                )
                logger.info(f"OK, notice all App servers to check etcd new data")
    pass


async def _add_main(server: _Server):
    try:
        logger.info("Add new node: %s..." % (address_from_server(server)))
        etcd = MySQLEtcdClient()
        datas: tuple[MySQLEtcdData, MySQLEtcdData] = await etcd.add_new_node(server)
        new_data, old_data = datas
        await asyncio.sleep(1)
        await notification_app_via_api(
            _type=APIType.Added,
            jdata={
                "host": str(server.host),
                "port": server.port,
            },
        )
        # TODO: Calculate consistent hashing
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
        await notification_app_via_api(
            _type=APIType.Update,
        )

        # TODO: Delete unnecessary data from moved node
        logger.info(
            "Delete unnecessary data(non owner of hashed) from existed MySQL node"
        )
        await _ring.delete_from_old_nodes(
            new_node=node,
        )
        # TODO: optimize table
        logger.info("Optimize MySQL nodes with data moved")
        await _ring.optimize_table_old_nodes(new_node=node)
        logger.info("Done !")
    except MySQLEtcdDuplicateNode:
        logger.error("This IP address already exists on etcd")


def add_main(line: str):
    try:
        _s = _parse_server(line)
        asyncio.run(_add_main(_s))
    except ValueError as e:
        logger.error("IP address error: %s" % (e))
        print("[bold]Try agin[/bold]")

    return


class MySQLCmd(Cmd):
    prompt = "mysql) "

    def __init__(self):
        super().__init__()

    def do_state(self, line: str):
        state_main()

    def help_state(self):
        print("Show MySQL cluster state")

    # About add
    def do_add(self, line: str):
        add_main(line)

    def help_add(self):
        print("Add new MySQL node into it's cluster")

    # About delete
    def do_delete(self, line: str):
        delete_main(line)

    def help_delete(self):
        print("Delete MySQL node from it's cluster")

    def do_EOF(self, line: str):
        print("[bold]Bye[/bold]")
        return True  # Stop cmdline

    def emptyline(self) -> bool:
        print("[bold]Please input any command[/bold]")
        return False  # Continue


@click.command()
def cli():
    init_log()
    """

    MySQL cluster controller with etcd.

    """
    cmd = MySQLCmd()
    cmd.cmdloop()


if __name__ == "__main__":
    cli()
