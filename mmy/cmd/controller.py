import asyncio
import ipaddress
import logging
from cmd import Cmd

import click
from mmy.etcd import MmyEtcdInfo, MySQLEtcdClient, MySQLEtcdNotFoundNode
from mmy.mysql.add import MySQLAdder
from mmy.mysql.client import MmyMySQLInfo
from mmy.mysql.delete import MySQLDeleter
from mmy.parse import MmyYAML, parse_yaml
from mmy.server import _Server, address_from_server
from mmy.state import state_main
from rich import print

logger = logging.getLogger(__name__)


def _parse_server(line: str) -> _Server:
    ip, _, port = line.rpartition(":")
    ipa = ipaddress.ip_address(ip)
    _s: _Server = _Server(
        host=ipa,
        port=int(port),
    )
    return _s


def delete_main(
    line: str,
    mysql_info: MmyMySQLInfo,
    etcd_info: MmyEtcdInfo,
):
    try:
        _s = _parse_server(line)
        deleter = MySQLDeleter(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        asyncio.run(deleter.do(_s))
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


def add_main(
    line: str,
    mysql_info: MmyMySQLInfo,
    etcd_info: MmyEtcdInfo,
):
    try:
        _s = _parse_server(line)
        adder: MySQLAdder = MySQLAdder(
            mysql_info=mysql_info,
            etcd_info=etcd_info,
        )
        asyncio.run(adder.do(_s))
    except ValueError as e:
        logger.error("IP address error: %s" % (e))
        print("[bold]Try agin[/bold]")

    return


def reshard_main(
    line: str,
    mysql_info: MmyMySQLInfo,
    etcd_info: MmyEtcdInfo,
):
    return


class MySQLCmd(Cmd):
    prompt = "mysql) "

    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
    ):
        super().__init__()
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info

    def do_state(self, line: str):
        state_main(self._etcd_info)

    def help_state(self):
        print("Show MySQL cluster state")

    # About add
    def do_add(self, line: str):
        add_main(line, self._mysql_info, self._etcd_info)

    def help_add(self):
        print("Add new MySQL node into it's cluster")

    # About delete
    def do_delete(self, line: str):
        delete_main(
            line=line,
            mysql_info=self._mysql_info,
            etcd_info=self._etcd_info,
        )

    def help_delete(self):
        print("Delete MySQL node from it's cluster")

    # About reshard
    def do_reshard(self, line: str):
        reshard_main(line, self._mysql_info, self._etcd_info)

    def help_reshard(self):
        print("Reshard all data on MySQL cluster")

    def do_EOF(self, line: str):
        print("[bold]Bye[/bold]")
        return True  # Stop cmdline

    def emptyline(self) -> bool:
        print("[bold]Please input any command[/bold]")
        return False  # Continue


@click.command()
@click.argument(
    "config_path",
    type=click.Path(exists=True),
)
def cli(
    config_path,
):
    """

    MySQL cluster controller with etcd.

    CONFIG_PATH: YAML file path included MySQL auth info.

    """
    mmy_yaml: MmyYAML = parse_yaml(config_path)
    cmd = MySQLCmd(
        mysql_info=mmy_yaml.mysql,
        etcd_info=mmy_yaml.etcd,
    )
    cmd.cmdloop()


if __name__ == "__main__":
    cli()
