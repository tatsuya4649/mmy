import asyncio

import click
from loguru import logger
from rich import print
from src.etcd import MySQLEtcdClient, MySQLEtcdData
from src.log import init_log
from src.mysql.hosts import MySQLHosts
from src.mysql.proxy import run_proxy_server
from src.server import _Server, address_from_server


async def etcd_management(
    mysql_hosts: MySQLHosts,
    etcd: _Server,
):
    logger.info("Start managing MySQL cluster and watching change about it")

    cli = MySQLEtcdClient(
        scheme="http",
        host=etcd.host,
        port=etcd.port,
    )
    async for item in cli.watch_mysql():
        mysql_hosts.update(item.nodes)


async def check_mysql(etcd: _Server):
    logger.debug("Test MySQL servers on etcd")
    try:
        client = MySQLEtcdClient(
            scheme="http",
            host=etcd.host,
            port=etcd.port,
        )
        data: MySQLEtcdData = await client.get()
        if len(data.nodes) == 0:
            logger.warning(f"No MySQL server. Please add it with controller.")
        else:
            logger.info(
                f"MySQL servers. {[address_from_server(i) for i in data.nodes]}"
            )
    except Exception as e:
        import os

        logger.exception(e)
        print("\n")
        print("\t[bold]Please fix and try. Bye.[/bold]")
        print("\n")
        os._exit(1)
    return


@click.command()
@click.option("--host", default="0.0.0.0", type=str, help="Bind socket to this host")
@click.option("--port", default=3306, type=int, help="Bind socket to this port")
@click.option("--etcd-host", default="172.16.0.4", type=str, help="Host of etcd server")
@click.option("--etcd-port", default=12379, type=int, help="Port of etcd server")
@click.option(
    "--connection-timeout",
    default=10,
    type=int,
    help="Timeout of MySQL connection phase",
)
@click.option(
    "--command-request-timeout",
    default=10,
    type=int,
    help="Timeout of MySQL command request from client to server",
)
@click.option(
    "--command-response-timeout",
    default=10,
    type=int,
    help="Timeout of MySQL command response from server to client",
)
def _main(
    host: str,
    port: int,
    etcd_host: str,
    etcd_port: int,
    connection_timeout: int,
    command_request_timeout: int,
    command_response_timeout: int,
):
    print("\n")
    print("Hello, this is [bold]mmysql[/bold].")
    print("\n")

    init_log()
    logger.info("Host: %s" % (host))
    logger.info("Port: %s" % (port))
    logger.info("Etcd host: %s" % (etcd_host))
    logger.info("Etcd port: %s" % (etcd_port))

    loop = asyncio.new_event_loop()
    etcd_server = _Server(
        host=etcd_host,
        port=etcd_port,
    )
    loop.run_until_complete(
        check_mysql(
            etcd=etcd_server,
        )
    )

    mysql_hosts = MySQLHosts()

    async def _p():
        await asyncio.gather(
            run_proxy_server(
                mysql_hosts=mysql_hosts,
                host=host,
                port=port,
                connection_timeout=connection_timeout,
                command_request_timeout=command_request_timeout,
                command_response_timeout=command_response_timeout,
            ),
            etcd_management(
                mysql_hosts=mysql_hosts,
                etcd=etcd_server,
            ),
        )

    loop.run_until_complete(_p())


if __name__ == "__main__":
    _main()
