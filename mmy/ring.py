import bisect
import copy
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Coroutine, NewType

from loguru import logger
from uhashring import HashRing

from .mysql.client import (
    MySQLAuthInfoByTable,
    MySQLClient,
    MySQLColumns,
    MySQLFetchAll,
    TableName,
)
from .server import _Server, address_from_server

_Point = NewType("_Point", str)


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


@dataclass
class MDP(MoveData):
    start: _Point
    end: _Point


class Ring(RingHandler):
    def __init__(
        self,
        init_nodes: list[Node] = list(),
        default_vnodes: int = 80,
    ):
        self._default_vnodes = default_vnodes
        nodes = dict()
        for _in in init_nodes:
            nodes[self.nodename_from_node(_in)] = {
                "instance": _in,
                "vnodes": self._default_vnodes,
            }
        self._hr = HashRing(
            nodes=nodes,
            hash_fn=lambda key: hashlib.md5(str(key).encode("utf-8")).hexdigest(),
        )
        self._node_hash: dict[str, Node] = dict()
        self._from_to: list[MDP] | None = None

    def nodename_from_node(self, node: Node) -> str:
        return str(node.host)

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

    async def add(self, node: Node) -> Coroutine[None, None, None]:
        async def _emp():
            return

        points: list[tuple[str, str]] = self._hr.get_points()
        _nodename: str = self.nodename_from_node(node)
        keys: list[str] = self._hr._keys
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

            return _emp()

        added_items: list[str] = list()
        added_points: list[tuple[str, str]] = self._hr.get_points()
        added_keys: list[str] = self._hr._keys
        added_keys.sort()

        for item, nodename in added_points:
            if _nodename == nodename:
                added_items.append(item)

        def get_add_point() -> Point:
            _index = added_keys.index(item)
            point, _ = added_points[_index]
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
        self._from_to = list()
        for item in added_items:
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
            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=pp.point,
                end=p.point,
            )
            self._from_to.append(mdp)
            return self._move(mdp)

        return _emp()

    async def delete(self, node: Node):
        points: list[tuple[str, str]] = self._hr.get_points()
        items = list()
        _nodename: str = self.nodename_from_node(node)
        for item, nodename in points:
            if _nodename == nodename:
                items.append(item)

        keys = self._hr._keys
        keys.sort()

        _prev_hr = copy.deepcopy(self._hr)
        del self._node_hash[_nodename]
        self._hr.remove_node(node)

        deleted_points: list[tuple[str, str]] = self._hr.get_points()
        deleted_keys: list[str] = self._hr._keys
        deleted_keys.sort()

        def get_delete_point() -> Point:
            _index = keys.index(item)
            point, _ = points[_index]
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

            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=pp.point,
                end=p.point,
            )
            await self._move(mdp)

    async def _move(self, mdp: MDP):
        _from: Md = mdp._from
        _to: Md = mdp._to
        start: _Point = mdp.start
        end: _Point = mdp.end
        if _from.node.host == _to.node.host:
            logger.info("From node and To node is same")
            return

        _from_address: str = "%s:%d" % (_from.node.host, _from.node.port)
        _to_address: str = "%s:%d" % (_to.node.host, _to.node.port)
        logger.info(f"Move data: {_from_address} ==> {_to_address} ")
        logger.info(f"  Start: {start}")
        logger.info(f"  End:   {end}")
        await self.move(
            MoveData(
                _from=_from,
                _to=_to,
            )
        )

    @property
    def from_to_data(self) -> list[MDP]:
        if self._from_to is None:
            raise RuntimeError("No data moved yet")
        return self._from_to


class MySQLMetaRing:
    RING_VNODES: int = 80

    def nodename_from_node(self, node: Node) -> str:
        return str(node.host)


class MySQLRing(
    Ring,
    MySQLMetaRing,
):
    def __init__(
        self,
        db: str,
        auth: MySQLAuthInfoByTable,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )
        self._db = db
        self._auth = auth

    def table_names(self):
        return list(self._auth.by_tables.keys())

    async def move(self, data: MoveData):
        logger.info("Move MySQL data")
        _from: Md = data._from
        _to: Md = data._to

        _from_client = MySQLClient(
            db=self._db,
            host=_from.node.host,
            port=_from.node.port,
            auth=self._auth,
        )
        _to_client = MySQLClient(
            db=self._db,
            host=_to.node.host,
            port=_to.node.port,
            auth=self._auth,
        )
        await _from_client.ping()
        await _to_client.ping()
        for table in self.table_names():
            tablename: TableName = TableName(table)
            _from_data = await _from_client.consistent_hashing_select(
                table=tablename,
                start=str(_from.point),
                end=str(_to.point),
            )
            columns: list[MySQLColumns] = await _from_client.columns_info(tablename)
            excluded_columns: list[
                MySQLColumns
            ] = _from_client.exclude_auto_increment_columns(columns)
            async for frag in _from_data:
                modified_frag: MySQLFetchAll = _from_client.exclude_data_by_columns(
                    excluded_columns, frag
                )
                await _to_client.insert(
                    table=tablename,
                    datas=modified_frag,
                )

    async def delete_from_old_nodes(self, new_node: Node):
        _ftd: list[MDP] = self.from_to_data

        for item in _ftd:
            from_node = item._from.node
            to_node = item._to.node
            if from_node == new_node:
                raise RuntimeError(
                    f"From node({address_from_server(from_node)}) must not be new node({address_from_server(new_node)})"
                )
            if not (to_node == new_node):
                raise RuntimeError(
                    f"To node({address_from_server(to_node)}) must be new node({address_from_server(new_node)})"
                )

            logger.info(f"Old data delete from {address_from_server(from_node)}")
            logger.info(f"  Start: {item.start}")
            logger.info(f"  End:   {item.end}")
            _from_client = MySQLClient(
                db=self._db,
                host=from_node.host,
                port=from_node.port,
                auth=self._auth,
            )
            for table in self.table_names():
                await _from_client.consistent_hashing_delete(
                    table=table,
                    start=item.start,
                    end=item.end,
                )

        return

    async def optimize_table_old_nodes(self, new_node: Node):
        _ftd: list[MDP] = self.from_to_data
        _from_nodes: list[Node] = list()
        for item in _ftd:
            from_node = item._from.node
            if from_node not in _from_nodes:
                _from_nodes.append(from_node)

        for from_node in _from_nodes:
            if from_node == new_node:
                raise RuntimeError(
                    f"From node({address_from_server(from_node)}) must not be new node({address_from_server(new_node)})"
                )

            _from_client = MySQLClient(
                db=self._db,
                host=from_node.host,
                port=from_node.port,
                auth=self._auth,
            )
            await _from_client.optimize_table()
            logger.info(f"Optimize MySQL node: {address_from_server(from_node)}")