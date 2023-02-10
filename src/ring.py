import bisect
import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import NewType

from loguru import logger
from uhashring import HashRing

from .server import _Server

_Point = NewType("_Point", int)


@dataclass
class Node(_Server):
    pass


@dataclass
class Point:
    have_node: Node
    point: _Point
    index: int


@dataclass
class Md:
    node: Node
    point: _Point


@dataclass
class MoveData:
    _from: Md
    _to: Md


class RingHandler(ABC):
    @abstractmethod
    def logring(self):
        """
        Log ring state
        """

    @abstractmethod
    async def move(self, data: MoveData):
        """
        Move data
        """

    @abstractmethod
    async def add(self, node: Node):
        """
        add new node
        """

    @abstractmethod
    async def delete(self, node: Node):
        """
        delete node
        """


class Ring(RingHandler):
    def __init__(
        self,
        default_vnodes: int = 80,
    ):
        self._hr = HashRing()
        self._node_hash: dict[str, Node] = dict()
        self._default_vnodes = default_vnodes

    def logring(self):
        DELIMITER_COUNT = 50
        DELIMITER = "=" * DELIMITER_COUNT
        DELIMITER_CONTENT = "-" * DELIMITER_COUNT
        NODEMARK = "*"
        logger.info(DELIMITER)
        logger.info(f"Ring nodes count: {self._hr.size}")
        logger.info(DELIMITER_CONTENT)
        logger.info(f"Ring distribution")
        for nodename, count in self._hr.distribution.items():
            logger.info(f"{nodename}: {count}")

        logger.info(DELIMITER_CONTENT)
        for point, nodename in self._hr.get_points():
            logger.info(f"{NODEMARK} {nodename} -- {point}({len(str(point))})")

        logger.info(DELIMITER)
        pass

    async def add(self, node: Node):
        points: list[tuple[int, str]] = self._hr.get_points()
        _nodename: str = node.host
        keys: list[int] = self._hr._keys
        keys.sort()

        _prev_hr = copy.deepcopy(self._hr)
        self._hr.add_node(
            _nodename,
            {
                "instance": node,
                "vnodes": self._default_vnodes,
            },
        )
        self._node_hash[_nodename] = node
        if len(points) == 0:
            logger.info(f"Add first node {node.host}:{node.port} as {_nodename} ")
            return

        items: list[int] = list()
        added_points: list[tuple[int, str]] = self._hr.get_points()
        added_keys: list[int] = self._hr._keys
        added_keys.sort()

        for item, nodename in added_points:
            if _nodename == nodename:
                items.append(item)

        def get_add_point() -> Point:
            _index = added_keys.index(item)
            point, nodename = added_points[_index]
            node: Node = self._hr[nodename]
            return Point(
                have_node=node,
                point=_Point(point),
                index=_index,
            )

        def get_next_point() -> Point:
            _next_index = bisect.bisect_right(keys, item)
            if _next_index == len(points):
                _next_index = 0

            point, nodename = points[_next_index]
            node: Node = _prev_hr[nodename]
            np = Point(
                have_node=node,
                point=_Point(point),
                index=_next_index,
            )
            return np

        def get_prev_point() -> Point:
            _prev_index = bisect.bisect_left(keys, item)
            _pi = _prev_index - 1
            point, nodename = points[_pi]
            node: Node = _prev_hr[nodename]
            pp = Point(
                have_node=node,
                point=_Point(point),
                index=_pi,
            )
            return pp

        logger.info(f"Add node {node.host}:{node.port} as {_nodename} ")
        for item in items:
            p = get_add_point()
            np = get_next_point()
            pp = get_prev_point()

            _from: Md = Md(
                node=np.have_node,
                point=pp.point,
            )
            _to: Md = Md(
                node=p.have_node,
                point=p.point,
            )
            await self._move(_from, _to, pp.point, p.point)

    async def delete(self, node: Node):
        points: list[tuple[int, str]] = self._hr.get_points()
        items = list()
        _nodename: str = node.host
        for item, nodename in points:
            if _nodename == nodename:
                items.append(item)

        keys = self._hr._keys
        keys.sort()

        _prev_hr = copy.deepcopy(self._hr)
        del self._node_hash[_nodename]
        self._hr.remove_node(node)

        deleted_points: list[tuple[int, str]] = self._hr.get_points()
        deleted_keys: list[int] = self._hr._keys
        deleted_keys.sort()

        def get_delete_point() -> Point:
            _index = keys.index(item)
            point, nodename = points[_index]
            node: Node = _prev_hr[nodename]
            return Point(
                have_node=node,
                point=_Point(point),
                index=_index,
            )

        def get_next_point() -> Point:
            _next_index = bisect.bisect_right(deleted_keys, item)
            if _next_index == len(deleted_points):
                _next_index = 0

            point, nodename = deleted_points[_next_index]
            node: Node = self._hr[nodename]
            np = Point(
                have_node=node,
                point=_Point(point),
                index=_next_index,
            )
            return np

        def get_prev_point() -> Point:
            _prev_index = bisect.bisect_left(deleted_keys, item)
            _pi = _prev_index - 1
            point, nodename = deleted_points[_pi]
            node: Node = self._hr[nodename]
            pp = Point(
                have_node=node,
                point=_Point(point),
                index=_pi,
            )
            return pp

        logger.info(f"Delete node: {node}")
        for item in items:
            p = get_delete_point()
            np = get_next_point()
            pp = get_prev_point()

            _from: Md = Md(
                node=p.have_node,
                point=pp.point,
            )
            _to: Md = Md(
                node=np.have_node,
                point=p.point,
            )
            await self._move(_from, _to, pp.point, p.point)

    async def _move(self, _from: Md, _to: Md, start: _Point, end: _Point):
        if _from.node.host == _to.node.host:
            logger.info("From node and To node is same")
            return

        _from_address: str = "%s:%d" % (_from.node.host, _from.node.port)
        _to_address: str = "%s:%d" % (_to.node.host, _to.node.port)
        logger.info(f"Move data: {_from_address} ==> {_to_address} ")
        logger.info(f"\tStart: {start}")
        logger.info(f"\tEnd:   {end}")
        await self.move(
            MoveData(
                _from=_from,
                _to=_to,
            )
        )


class MySQLRing(Ring):
    def __init__(
        self,
    ):
        super().__init__()

    async def move(self, data: MoveData):
        logger.info("Move MySQL data")
        # TODO: move data
        import asyncio

        await asyncio.sleep(0.1)
        self.logring()
