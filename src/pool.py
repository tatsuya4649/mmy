import base64
import hashlib
import json
import os
from typing import NewType

import httpx
from loguru import logger

from .server import Server, State

ETCD_SCHEME = "http"
ETCD_HOST = "172.16.0.4"
ETCD_PORT = 12379
ETCD_ADDRESS = "%s://%s:%d" % (ETCD_SCHEME, ETCD_HOST, ETCD_PORT)

STATUS_TIMEOUT = 3
ServerID = NewType("ServerID", str)


class ServerPool:
    def __init__(
        self,
        _logger=None,
    ):
        self._servers: list[Server] = list()
        self._state_map: dict[State, list[Server]] = {
            State.Run: list(),
            State.Move: list(),
            State.Broken: list(),
            State.Unknown: list(),
        }
        self._id_map: dict[ServerID, Server] = dict()
        self.logger = _logger if _logger is not None else logger

    def add(self, server: Server) -> ServerID:
        _sid: ServerID = self.sid_from_address(server.host, server.port)
        if _sid in self._id_map:
            raise ValueError(
                "The server is already exists(%s:%d)" % (server.host, server.port)
            )

        self._state_map[server.state].append(server)
        self._servers.append(server)
        self._id_map[_sid] = server
        return _sid

    @classmethod
    def sid_from_address(cls, host: str, port: int) -> ServerID:
        hp: str = "%s_%d" % (host, port)
        _ssid: str = hashlib.md5(hp.encode("utf-8")).hexdigest()
        return ServerID(_ssid)

    def delete(self, sid: ServerID):
        _s: Server | None = self._id_map.get(sid)
        if _s is None:
            raise ValueError

        self._state_map[_s.state].remove(_s)
        del self._id_map[sid]
        self._servers = list(
            filter(
                lambda x: sid != self.sid_from_address(x.host, x.port), self._servers
            ),
        )


class MySQLServerPool(ServerPool):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)


async def ping():
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                ETCD_ADDRESS + "/v3/maintenance/status",
                timeout=STATUS_TIMEOUT,
            )
        except httpx.ConnectTimeout:
            logger.error("Connect timeout: %s" % (ETCD_ADDRESS))
            os._exit(1)

        logger.debug("PING %s" % (ETCD_ADDRESS))
        logger.info(resp.json())


def _parse_kv(kv: dict[str, str]):
    _value: str | None = kv.get("value")
    if _value is None:
        logger.error("Unknown etcd format")
        raise ValueError

    _bytes: bytes = _value.encode("utf-8")
    _vbytes: bytes = base64.b64decode(_bytes)
    jdict = json.loads(_vbytes)
    logger.info("Received value: %s" % jdict)
    pass


async def watch():
    async with httpx.AsyncClient() as client:
        await ping()
        key = base64.b64encode(b"foo")
        logger.debug("Watching... %s" % (ETCD_ADDRESS))
        async with client.stream(
            "POST",
            ETCD_ADDRESS + "/v3/watch",
            timeout=None,
            json={
                "create_request": {
                    "key": key.decode("utf-8"),
                },
            },
        ) as resp:
            async for chunk in resp.aiter_bytes():
                logger.info("Receive event from %s" % (ETCD_ADDRESS))
                jchunk = json.loads(chunk)
                logger.info(jchunk)
                if jchunk.get("error") is not None:
                    logger.error("Occurred error: %s" % (jchunk))
                    break

                result = jchunk["result"]
                events = result.get("events", [])
                for event in events:
                    logger.info(event)
                    kv = event.get("kv")
                    try:
                        _parse_kv(kv)
                    except Exception as e:
                        logger.exception(e)
