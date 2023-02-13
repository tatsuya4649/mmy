import abc
import base64
import copy
import hashlib
import ipaddress
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, AsyncGenerator, TypeAlias

import httpx
from dacite import Config, from_dict
from loguru import logger

ETCD_SCHEME = "http"
ETCD_HOST = "172.16.0.4"
ETCD_PORT = 12379
ETCD_ADDRESS = "%s://%s:%d" % (ETCD_SCHEME, ETCD_HOST, ETCD_PORT)

STATUS_TIMEOUT = 3

from typing import Generic, TypeVar

from .server import address_from_server


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


EtcdJson: TypeAlias = dict[str, Any]


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

    def _parse_kv(self, kv: dict[str, str]) -> EtcdJson:
        _value: str | None = kv.get("value")
        if _value is None:
            logger.error("Unknown etcd format")
            raise ValueError

        _bytes: bytes = _value.encode("utf-8")
        _vbytes: bytes = base64.b64decode(_bytes)
        jdict = json.loads(_vbytes)
        logger.info("Received value: %s" % jdict)
        return jdict

    @abc.abstractmethod
    async def put(self, data: _EtcdData) -> None:
        """
        Put new data into etcd
        """

    @abc.abstractmethod
    async def get(self) -> _EtcdData:
        """
        Get etcd data
        """

    @abc.abstractmethod
    async def delete(self) -> None:
        """
        Delete etcd data
        """

    async def watch(self, key: bytes) -> AsyncGenerator[Any, None]:
        async with httpx.AsyncClient() as client:
            await self.ping()
            key = base64.b64encode(key)
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
                            yield self._parse_kv(kv)
                        except Exception as e:
                            logger.exception(e)

    @property
    def relation_hash(self) -> str:
        tnb: bytes = "%d".encode("utf-8") % (time.time_ns())
        _hash: str = hashlib.md5(tnb).hexdigest()
        return _hash[:10]

    def base64_str(self, value: bytes) -> str:
        return base64.b64encode(value).decode("utf-8")


from .server import Server, State, _Server


@dataclass
class MySQLEtcdData(EtcdData):
    nodes: list[Server]


class MySQLEtcdNotFoundNode(ValueError):
    pass


class MySQLEtcdDuplicateNode(ValueError):
    pass


class JSONEncoderForMySQLEtcd(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            return str(o)
        else:
            return super().defualt(o)


class MySQLEtcdClient(EtcdClient[MySQLEtcdData]):
    KEYNAME: bytes = b"/core/mysql"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def put(self, data: MySQLEtcdData) -> None:
        _ddata = asdict(data)
        _jdata: str = json.dumps(_ddata, cls=JSONEncoderForMySQLEtcd)
        async with httpx.AsyncClient() as client:
            _api: str = self._address + "/v3/kv/put"
            _hash: str = self.relation_hash
            logger.debug("[%s] Put data about MySQL into etcd via %s" % (_hash, _api))
            response = await client.post(
                _api,
                timeout=10,
                json={
                    "key": self.base64_str(self.KEYNAME),
                    "value": self.base64_str(_jdata.encode("utf-8")),
                },
            )
            logger.debug(
                "[%s] Response status code %d for put" % (_hash, response.status_code)
            )
            if not response.is_success:
                raise EtcdHttpResponseError(
                    status_code=response.status_code,
                    body=response.content,
                )

    def _from_etcd_data(self, value: EtcdJson) -> MySQLEtcdData:
        dacite_config = Config(
            type_hooks={
                State: State,
                ipaddress.IPv4Address: ipaddress.ip_address,
                ipaddress.IPv6Address: ipaddress.ip_address,
            },
        )
        etcd_data: MySQLEtcdData = from_dict(
            data_class=MySQLEtcdData,
            data=value,
            config=dacite_config,
        )
        return etcd_data

    async def get(self) -> MySQLEtcdData:
        async with httpx.AsyncClient() as client:
            _api: str = self._address + "/v3/kv/range"
            _hash: str = self.relation_hash
            logger.debug("[%s] Get data about MySQL from etcd via %s" % (_hash, _api))
            response = await client.post(
                _api,
                timeout=10,
                json={
                    "key": self.base64_str(self.KEYNAME),
                },
            )
            logger.debug(
                "[%s] Response status code %d for get" % (_hash, response.status_code)
            )
            if not response.is_success:
                raise EtcdHttpResponseError(
                    status_code=response.status_code,
                    body=response.content,
                )

            jdata = response.json()
            _kvs = jdata.get("kvs")
            if _kvs is None:
                logger.info(
                    "[%s] Not found MySQL etcd data (key: %s)"
                    % (_hash, self.KEYNAME.decode("utf-8"))
                )
                return MySQLEtcdData(
                    nodes=[],
                )
            if len(_kvs) != 1:
                raise RuntimeError

            _kv = _kvs[0]
            value = self._parse_kv(_kv)
            return self._from_etcd_data(value)

    async def delete(self) -> None:
        async with httpx.AsyncClient() as client:
            _api: str = self._address + "/v3/kv/deleterange"
            _hash: str = self.relation_hash
            logger.debug(
                "[%s] Delete data about MySQL from etcd via %s" % (_hash, _api)
            )
            response = await client.post(
                _api,
                timeout=10,
                json={
                    "key": self.base64_str(self.KEYNAME),
                    "range_end": self.base64_str(b"0"),
                },
            )
            logger.debug(
                "[%s] Response status code %d for delete"
                % (_hash, response.status_code)
            )
            if not response.is_success:
                raise EtcdHttpResponseError(
                    status_code=response.status_code,
                    body=response.content,
                )

    async def update_state(self, server: _Server, state: State) -> None:
        data: MySQLEtcdData = await self.get()

        for node in data.nodes:
            if node.host == server.host and node.port == server.port:
                _addr: str = "%s:%d" % (server.host, server.port)
                logger.debug(
                    'Update node state of %s from "%s" to "%s"'
                    % (_addr, node.state, state)
                )
                # Update new state
                node.state = state
                break
        else:
            raise MySQLEtcdNotFoundNode

        await self.put(data)

    async def add_new_node(
        self, server: _Server, init_state: State = State.Unknown
    ) -> tuple[MySQLEtcdData, MySQLEtcdData]:
        data: MySQLEtcdData = await self.get()
        old_data: MySQLEtcdData = copy.deepcopy(data)
        if (
            len(
                list(
                    filter(
                        lambda x: x.host == server.host and x.port == server.port,
                        data.nodes,
                    )
                )
            )
            > 0
        ):
            raise MySQLEtcdDuplicateNode(
                "Already have %s" % (address_from_server(server))
            )
        data.nodes.append(
            Server(
                host=server.host,
                port=server.port,
                state=init_state,
            )
        )
        await self.put(data)
        return data, old_data

    async def delete_node(self, server: _Server) -> None:
        data: MySQLEtcdData = await self.get()

        _n: list[Server] = list(
            filter(
                lambda x: x.host == server.host and x.port == server.port,
                data.nodes,
            )
        )
        if len(_n) == 0:
            raise MySQLEtcdNotFoundNode("Not found %s" % (address_from_server(server)))

        new_nodes: list[Server] = list(
            filter(
                lambda x: not (x.host == server.host and x.port == server.port),
                data.nodes,
            )
        )
        data.nodes = new_nodes
        await self.put(data)

    async def watch_mysql(self) -> AsyncGenerator[MySQLEtcdData, None]:
        _w = super().watch(key=self.KEYNAME)
        async for value in _w:
            yield self._from_etcd_data(value)
