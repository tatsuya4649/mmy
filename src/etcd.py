import abc
import base64
import json
import os
from dataclasses import asdict, dataclass

import httpx
from dacite import Config, from_dict
from loguru import logger

ETCD_SCHEME = "http"
ETCD_HOST = "172.16.0.4"
ETCD_PORT = 12379
ETCD_ADDRESS = "%s://%s:%d" % (ETCD_SCHEME, ETCD_HOST, ETCD_PORT)

STATUS_TIMEOUT = 3

from typing import Generic, TypeVar


@dataclass
class EtcdData:
    """
    super class for etcd data
    """


_EtcdData = TypeVar("_EtcdData", bound=EtcdData)


class EtcdError(RuntimeError):
    pass


class EtcdHttpResponseError(EtcdError):
    def __init__(
        self,
        status_code: int,
        body: bytes,
    ):
        self._status_code = status_code
        self._body = body

    pass


class EtcdClient(abc.ABC, Generic[_EtcdData]):
    def __init__(
        self,
        scheme: str = ETCD_SCHEME,
        host: str = ETCD_HOST,
        port: int = ETCD_PORT,
        timeout: int = STATUS_TIMEOUT,
    ):
        self._scheme = scheme
        self._host = host
        self._port = port
        self._address = "%s://%s:%d" % (scheme, host, port)
        self._timeout = timeout

    async def ping(self):
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(
                    self._address + "/v3/maintenance/status",
                    timeout=self._timeout,
                )
            except httpx.ConnectTimeout:
                logger.error("Connect timeout: %s" % (self._address))
                os._exit(1)

            logger.debug("PING %s" % (self._address))
            logger.info(resp.json())

    def _parse_kv(self, kv: dict[str, str]):
        _value: str | None = kv.get("value")
        if _value is None:
            logger.error("Unknown etcd format")
            raise ValueError

        _bytes: bytes = _value.encode("utf-8")
        _vbytes: bytes = base64.b64decode(_bytes)
        jdict = json.loads(_vbytes)
        logger.info("Received value: %s" % jdict)
        pass

    @abc.abstractmethod
    async def put(self, data: _EtcdData):
        """
        Put new data into etcd
        """

    @abc.abstractmethod
    async def get(self) -> _EtcdData:
        """
        Get etcd data
        """

    async def watch(self):
        async with httpx.AsyncClient() as client:
            await self.ping()
            key = base64.b64encode(b"foo")
            logger.debug("Watching... %s" % (self._address))
            async with client.stream(
                "POST",
                self._address + "/v3/watch",
                timeout=None,
                json={
                    "create_request": {
                        "key": key.decode("utf-8"),
                    },
                },
            ) as response:
                if not response.is_success:
                    raise EtcdHttpResponseError(
                        status_code=response.status_code,
                        body=response.content,
                    )
                async for chunk in response.aiter_bytes():
                    logger.info("Receive event from %s" % (self._address))
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
                            self._parse_kv(kv)
                        except Exception as e:
                            logger.exception(e)


from .server import Server, State, _Server


@dataclass
class MySQLEtcdData(EtcdData):
    nodes: list[Server]


class MySQLEtcdNotFoundNode(ValueError):
    pass


class MySQLEtcdClient(EtcdClient[MySQLEtcdData]):
    KEYNAME: bytes = b"/core/mysql"

    def __init__(self):
        super().__init__()

    async def put(self, data: MySQLEtcdData) -> None:
        _ddata = asdict(data)
        _jdata: str = json.dumps(_ddata)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self._address + "/v3/kv/put",
                timeout=10,
                json={
                    "key": base64.b64encode(self.KEYNAME).decode("utf-8"),
                    "value": base64.b64encode(_jdata.encode("utf-8")).decode("utf-8"),
                },
            )
            if not response.is_success:
                raise EtcdHttpResponseError(
                    status_code=response.status_code,
                    body=response.content,
                )

    async def get(self) -> MySQLEtcdData:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self._address + "/v3/kv/range",
                timeout=10,
                json={
                    "key": base64.b64encode(self.KEYNAME).decode("utf-8"),
                },
            )
            if not response.is_success:
                raise EtcdHttpResponseError(
                    status_code=response.status_code,
                    body=response.content,
                )

            jdata = response.json()
            _kvs = jdata.get("kvs")
            if _kvs is None:
                return MySQLEtcdData(
                    nodes=[],
                )
            if len(_kvs) != 1:
                raise RuntimeError

            _kv = _kvs[0]
            # Value must be JSON format
            _value = _kv["value"]
            value = json.loads(base64.b64decode(_value))
            dacite_config = Config(
                type_hooks={
                    State: State,
                },
            )
            etcd_data: MySQLEtcdData = from_dict(
                data_class=MySQLEtcdData,
                data=value,
                config=dacite_config,
            )
            return etcd_data

    async def update_state(self, server: _Server, state: State) -> None:
        data: MySQLEtcdData = await self.get()

        for node in data.nodes:
            if node.host == server.host and node.port == server.port:
                # Update new state
                node.state = state
                break
        else:
            raise MySQLEtcdNotFoundNode

        await self.put(data)
