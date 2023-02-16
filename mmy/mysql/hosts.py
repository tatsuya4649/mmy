from typing import Any

from uhashring import HashRing

from ..ring import MySQLMetaRing, Node
from ..server import Server


class MySQLHosts(MySQLMetaRing):
    def __init__(
        self,
    ):
        self._ring = HashRing()

    def update(self, nodes: list[Server]):
        _nodes: dict[str, Any] = dict()
        for _n in nodes:
            _node = Node(host=_n.host, port=_n.port)
            _nodes[self.nodename_from_node(_node)] = {
                "instance": _n,
                "vnodes": self.RING_VNODES,
            }
        self._ring = HashRing(nodes=_nodes)

    def get_host(self, key: str):
        if self._ring.size == 0:
            raise RuntimeError("No hosts")
        _h = self._ring.get(key)
        return _h["instance"]
