import asyncio
import logging
import os
from pprint import pformat

import click
import httpx
from mmy.const import SYSTEM_NAME
from mmy.etcd import EtcdError, EtcdPingError, MySQLEtcdClient, MySQLEtcdData
from mmy.monitor import MmyMonitor
from mmy.mysql.hosts import MySQLHosts
from mmy.mysql.proxy import ProxyServer
from mmy.parse import MmyMySQLInfo, MmyYAML, parse_yaml
from mmy.server import _Server, address_from_server

logger = logging.getLogger(__name__)


async def monitoring_mysql_servers(
    mysql_hosts: MySQLHosts,
    mysql_info: MmyMySQLInfo,
    etcd: _Server,
    min_jitter: float = 1.0,
    max_jitter: float = 10.0,
):
    """
    Monitor MySQL's health and find broken host, update cluster info on etcd.
    """
    monitor: MmyMonitor = MmyMonitor(
        mysql_hosts=mysql_hosts,
        auth=mysql_info,
        etcd=etcd,
        min_jitter=min_jitter,
        max_jitter=max_jitter,
    )
    await monitor.start_monitor()


async def etcd_management(
    mysql_hosts: MySQLHosts,
    etcd_data: MySQLEtcdData,
):
    logger.info("Start managing MySQL cluster and watching change about it")

    while True:
        await asyncio.sleep(0.5)
        for node in etcd_data.nodes:
            logger.info(f"Watching event on etcd node({node.address_format()})")
            try:
                cli = MySQLEtcdClient(
                    scheme="http",
                    host=node.host,
                    port=node.port,
                )
                async for item in cli.watch_mysql():
                    logger.info(item)
                    logger.info(
                        f"Received event on etcd: {pformat([i.address_format() for i in item.nodes])}"
                    )
                    mysql_hosts.update(item.nodes)

            except EtcdPingError:
                logger.error(f"etcd ping error: {node.address_format()}")
            except EtcdError as e:
                logger.error(e)
            except Exception as e:
                logger.error(e)


async def check_mysql(mysql_hosts: MySQLHosts, etcd: _Server):
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
            mysql_hosts.update(data.nodes)
            logger.info(
                f"MySQL servers. {[address_from_server(i) for i in data.nodes]}"
            )
    except httpx.ConnectError as e:
        logger.error(
            f"Can't connect to etcd cluster. (with {address_from_server(etcd)})"
        )
    except Exception as e:

        logger.exception(e)
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
    "--config-path",
    "-c",
    type=str,
    help="Configuration file path for mmy",
)
@click.option(
    "--command-timeout",
    default=10,
    type=int,
    help="Timeout of MySQL command request from client to server",
)
def _main(
    host: str,
    port: int,
    etcd_host: str,
    etcd_port: int,
    connection_timeout: int,
    command_timeout: int,
    config_path: str,
):
    logger.info("Host: %s" % (host))
    logger.info("Port: %s" % (port))
    logger.info("Etcd host: %s" % (etcd_host))
    logger.info("Etcd port: %s" % (etcd_port))

    mysql_hosts = MySQLHosts()
    loop = asyncio.new_event_loop()
    etcd_server = _Server(
        host=etcd_host,
        port=etcd_port,
    )
    loop.run_until_complete(
        # Get initial MySQL node
        check_mysql(
            mysql_hosts=mysql_hosts,
            etcd=etcd_server,
        )
    )

    mmy_yaml: MmyYAML = parse_yaml(config_path)

    async def _p():
        proxy_server = ProxyServer(
            mysql_hosts=mysql_hosts,
            host=host,
            port=port,
            connection_timeout=connection_timeout,
            command_timeout=command_timeout,
        )
        await asyncio.gather(
            proxy_server.serve(),
            etcd_management(
                mysql_hosts=mysql_hosts,
                etcd_data=mmy_yaml.etcd,
            ),
            monitoring_mysql_servers(
                mysql_hosts=mysql_hosts,
                etcd=etcd_server,
                mysql_info=mmy_yaml.mysql,
            ),
        )

    loop.run_until_complete(_p())


if __name__ == "__main__":
    _main()
