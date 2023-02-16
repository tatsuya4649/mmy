import asyncio
import random

from loguru import logger
from rich import text

from .etcd import ETCD_SCHEME, MySQLEtcdClient
from .mysql.client import MySQLClient
from .mysql.hosts import MySQLHosts
from .parse import MmyMySQLInfo
from .server import State, _Server


class MmyMonitor:
    def __init__(
        self,
        mysql_hosts: MySQLHosts,
        auth: MmyMySQLInfo,
        etcd: list[_Server],
        min_jitter: float = 1.0,
        max_jitter: float = 10.0,
        connect_timeout: int = 5,
    ):
        self._mysql_hosts: MySQLHosts = mysql_hosts
        self._etcd: list[_Server] = etcd
        self._min_jitter: float = min_jitter
        self._max_jitter: float = max_jitter
        self._cont: bool = True
        self._connect_timeout: int = connect_timeout
        self._auth = auth

    async def _detect_update_state(self, server: _Server, new_state: State):
        for etcd in self._etcd:
            try:
                _etcd_client = MySQLEtcdClient(
                    scheme=ETCD_SCHEME,
                    host=etcd.host,
                    port=etcd.port,
                )
                await _etcd_client.update_state(
                    server=server,
                    state=new_state,
                )
                logger.info(f"Updated server state on etcd: {server.address_format()}")
            except Exception as e:
                logger.exception(e)

            break

    async def start_monitor(self):
        while self._cont:
            sleep_jitter: float = random.uniform(self._min_jitter, self._max_jitter)
            logger.debug(f"Enter sleep time: {sleep_jitter}s")
            await asyncio.sleep(sleep_jitter)
            logger.debug("Start monitoring MySQLs server %s" % (self._mysql_hosts))

            _address_map: dict[str, State] = self._mysql_hosts.address_map
            for server in self._mysql_hosts.gen_hosts():
                address = server.address_format()
                cli = MySQLClient(
                    host=server.host,
                    port=server.port,
                    connect_timeout=self._connect_timeout,
                    auth=self._auth,
                )
                try:
                    logger.debug(f"PING test: {server.address_format()}")
                    await cli.ping()
                    logger.debug(f"Passed: {server.address_format()}")
                    if _address_map[address] is not State.Run:
                        logger.info(
                            f"State update: {_address_map[address].name} => {State.Run.name}"
                        )
                        await self._detect_update_state(server, State.Run)

                except asyncio.TimeoutError:
                    logger.error(f"PING error: {server.address_format()}")

                    if _address_map[address] is not State.Broken:
                        logger.info(
                            f"State update: {_address_map[address].name} => {State.Broken.name}"
                        )
                        await self._detect_update_state(server, State.Broken)
