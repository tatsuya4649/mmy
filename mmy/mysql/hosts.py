from typing import Any, Generator

from loguru import logger
from uhashring import HashRing

from ..ring import MySQLMetaRing, Node
from ..server import Server, State, _Server


class MySQLHostBroken(RuntimeError):
    pass


class MySQLHosts(MySQLMetaRing):
    def __init__(
        self,
    ):
        self._ring = HashRing()
        self._address_map: dict[str, State] = dict()

    def __str__(self):
        ins = self._ring.get_instances()
        return f'[{", ".join([i.address_format() for i in ins])}]'

    def __len__(self):
        # How many MySQL servers in this hash ring.
        return len(self._ring.get_instances())

    def update(self, nodes: list[Server]):
        _nodes: dict[str, Any] = dict()
        self._address_map.clear()
        for _n in nodes:
            _node = Node(host=_n.host, port=_n.port)
            _nodes[self.nodename_from_node(_node)] = {
                "instance": _n,
                "vnodes": self.RING_VNODES,
            }
            self._address_map[_n.address_format()] = _n.state
        self._ring = HashRing(nodes=_nodes)

    def get_host(self, key: str) -> _Server:
        if self._ring.size == 0:
            raise RuntimeError("No hosts")

        _h = self._ring.get(key)
        logger.debug(f"Get server from key: {key}")
        logger.debug(_h)
        _s = _h["instance"]

        if _s.state == State.Broken:
            logger.debug("Detect MySQL is broken")
            raise MySQLHostBroken

        return _Server(
            host=_s.host,
            port=_s.port,
        )

    def gen_hosts(self) -> Generator[str, None, None]:
        for i in self._ring.get_instances():
            yield i

    def get_hosts(self) -> list[_Server]:
        return self._ring.get_instances()

    def get_address_format(self) -> list[str]:
        return [i.address_format() for i in self.get_hosts()]

    @property
    def address_map(self):
        return self._address_map
