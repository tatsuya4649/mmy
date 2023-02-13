import asyncio
import ipaddress
from cmd import Cmd

import click
from loguru import logger
from rich import print
from src.etcd import MySQLEtcdClient, MySQLEtcdNotFoundNode
from src.log import init_log
from src.mysql.add import _add_main
from src.server import _Server, address_from_server
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
