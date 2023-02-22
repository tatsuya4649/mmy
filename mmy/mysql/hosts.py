import logging
from typing import Any, Generator

from uhashring import HashRing

from ..ring import MySQLMetaRing
from ..server import Server, State, _Server


class MySQLHostBroken(RuntimeError):
    pass


class MySQLHosts(MySQLMetaRing):
    def __init__(
        self,
    ):
        self.logger = logging.getLogger(__name__)
        self._ring = HashRing()
        self._ring_witout_move = HashRing()
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
        self._ring_witout_move = HashRing()
        for _n in nodes:
            _node = {
                "instance": _n,
                "vnodes": self.RING_VNODES,
            }
            _nodes[self._ring.nodename_from_node(_n)] = _node
            self._address_map[_n.address_format()] = _n.state
            if _n.state is not State.Move:
                self._ring_witout_move.add_node(_node)
        self._ring = HashRing(nodes=_nodes)

    def get_host(self, key: str) -> Server:
        if self._ring.size == 0:
            raise RuntimeError("No hosts")

        _h = self._ring.get(key)
        self.logger.debug(f"Get server from key: {key}")
        self.logger.debug(_h)
        _s: Server = _h["instance"]

        if _s.state == State.Broken:
            self.logger.debug("Detect MySQL is broken")
            raise MySQLHostBroken

        return _s

    def get_host_without_move(self, key: str) -> Server:
        if self._ring_witout_move.size == 0:
            raise RuntimeError("No hosts")

        _h = self._ring_witout_move.get(key)
        self.logger.debug(f"Get server from key: {key}")
        self.logger.debug(_h)
        _s: Server = _h["instance"]

        if _s.state == State.Broken:
            self.logger.debug("Detect MySQL is broken")
            raise MySQLHostBroken

        return _s

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
