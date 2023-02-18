import bisect
import copy
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Coroutine, NewType

from uhashring import HashRing

from .mysql.client import (
    MmyMySQLInfo,
    MySQLAuthInfoByTable,
    MySQLClient,
    MySQLColumns,
    MySQLFetchAll,
    TableName,
)
from .mysql.sql import SQLPoint, _Point, _SQLPoint
from .server import _Server


@dataclass
class Node(_Server):
    pass


@dataclass
class Point:
    have_node: Node
    point: _Point
    index: int

    def __eq__(self, other):
        return (
            self.have_node == other.have_node
            and self.point == other.point
            and self.index == other.index
        )

    def __gt__(self, other):
        return self.point > other.point

    def __ge__(self, other):
        return self.point >= other.point

    def __lt__(self, other):
        return not self.__ge__(other)

    def __le__(self, other):
        return not self.__gt__(other)


@dataclass
class Md:
    node: Node
    start_point: _Point
    start_equal: bool
    end_point: _Point
    end_equal: bool


@dataclass
class MoveData:
    _from: Md
    _to: Node


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


class RingNoMoveYet(RuntimeError):
    """
    Not have moved data
    """


@dataclass
class MovedTableData:
    tablename: TableName
    moved_count: int


@dataclass
class MovedData:
    tables: list[MovedTableData]


logger: logging.Logger = logging.getLogger(__name__)


def reset_from_to_parameter(func):
    async def _wrap(*args, **kwargs):
        ring: Ring = args[0]
        assert isinstance(ring, Ring) is True
        ring._from_to = list()
        return await func(*args, **kwargs)

    return _wrap


class Ring(RingHandler):
    def __init__(
        self,
        init_nodes: list[Node] = list(),
        default_vnodes: int = 80,
    ):
        self._default_vnodes = default_vnodes
        self._node_hash: dict[str, Node] = dict()
        nodes = dict()
        for _in in init_nodes:
            _nodename = self.nodename_from_node(_in)
            nodes[_nodename] = {
                "instance": _in,
                "vnodes": self._default_vnodes,
            }
            self._node_hash[_nodename] = _in
        self._hr = HashRing(
            nodes=nodes,
            hash_fn=lambda key: hashlib.md5(str(key).encode("utf-8")).hexdigest(),
        )
        self._from_to: list[MDP] | None = None

    def __len__(self) -> int:
        return len(self._node_hash)

    def nodename_from_node(self, node: Node) -> str:
        return f"%s:%d" % (str(node.host), node.port)

    def ring_node_count(self) -> int:
        return self._hr.size

    def ring_points(self):
        return self._hr.get_points()

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

    @reset_from_to_parameter
    async def add(self, node: Node) -> list[Coroutine[None, None, MovedData | None]]:

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
            return []

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

        def get_prev_add_point() -> Point:
            _index = added_keys.index(item)
            point, _ = added_points[_index - 1]
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

        logger.info(f"Add node {_nodename} ")
        self._from_to = list()
        _cos: list[Coroutine[None, None, MovedData | None]] = list()
        for item in added_items:
            ap = get_add_point()  # added point
            np = get_next_point()  # next point
            pp = get_prev_point()  # prev point
            pap: Point = get_prev_add_point()  # prev added point
            print("000000000000000")
            print("PP  ", pp.point)
            print("AP  ", ap.point)
            print("NP  ", np.point)
            print("PAP ", pap.point)
            """

            A: Added node
            |> : Greater than
            =| : Less than or equal

            Ex1.

            Before:

              prev                          next
                * --------------------------- *
                |>------------=|
                        Owner of "next" node.
                        |
                        |  Move to A
                        |______
                               |
            After:             |

              prev             A             next
                * -------------*-------------- *

            Ex2.

            Before:

              prev                          next
                * --------------------------- *
                |>------------=|
                        Owner of "next" node.
                        |
                        |  Move to A
                        |______
                               |
            After:             |

              prev      A       A            next
                * ------*-------*------------- *
            """

            _from: Md = Md(
                node=np.have_node,
                start_point=pp.point if pap <= pp else pap.point,
                start_equal=False,
                end_point=ap.point,
                end_equal=True,
            )
            print(f"{_from.start_point}~{_from.end_point}")
            _to: Node = ap.have_node
            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=pp.point,
                end=ap.point,
            )
            self._from_to.append(mdp)
            _cos.append(self._move(mdp))

        return _cos

    @reset_from_to_parameter
    async def delete(self, node: Node) -> list[Coroutine[None, None, MovedData | None]]:
        points: list[tuple[str, str]] = self._hr.get_points()
        items = list()
        _nodename: str = self.nodename_from_node(node)
        for item, nodename in points:
            if _nodename == nodename:
                items.append(item)

        keys = self._hr._keys
        keys.sort()

        del self._node_hash[_nodename]
        self._hr.remove_node(_nodename)

        if len(self) > 0 and len(items) == 0:
            raise RuntimeError("Unexpected error. Must have Deleted items...")
        elif len(self) == 0:
            # No node in this ring. So, do not anything.
            return []

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

        def get_prev_delete_point() -> Point:
            _index = keys.index(item)
            point, _ = points[_index - 1]
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

        logger.info(f"Delete node: {_nodename}")
        self._from_to = list()
        _cos: list[Coroutine[None, None, MovedData | None]] = list()

        for item in items:
            dp: Point = get_delete_point()
            np: Point = get_next_point()
            pp: Point = get_prev_point()
            pdp: Point = get_prev_delete_point()

            """

            D : Deleted node
            |> : Greater than
            =| : Less than or equal

        Ex1.

            Before:

                  prev      D       D     D      next
                    * ------*-------*-----*------- *
                    |>-------------------=|
                            Owner of "D" node.
                            |
                            |       Move to next
                            |____________________
                                                 |
            After:                               |

                  prev                          next
                    * --------------------------- *


        Ex2.

            Before:

              prev             D            next
                * -------------*------------- *
                |>------------=|
                        Owner of "D" node.
                        |
                        |       Move to next
                        |_____________________
                                              |
            After:                            |

              prev                          next
                * --------------------------- *

            """

            _from: Md = Md(
                node=dp.have_node,
                start_point=pp.point if pdp <= pp else pdp.point,
                start_equal=False,
                end_point=dp.point,
                end_equal=True,
            )
            _to: Node = np.have_node

            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=pp.point,
                end=dp.point,
            )
            self._from_to.append(mdp)
            _cos.append(self._move(mdp))

        return _cos

    async def _move(self, mdp: MDP) -> MovedData | None:
        _from: Md = mdp._from
        _to: Node = mdp._to
        start: _Point = mdp.start
        end: _Point = mdp.end
        if _from.node == _to:
            logger.info("From node and To node is same")
            return MovedData(
                tables=[],
            )

        logger.info(
            f"Move data: {_from.node.address_format()} ==> {_to.address_format()} "
        )
        logger.info(f"  Start: {start}")
        logger.info(f"  End:   {end}")
        return await self.move(
            MoveData(
                _from=_from,
                _to=_to,
            )
        )

    @property
    def from_to_data(self) -> list[MDP]:
        if self._from_to is None:
            raise RingNoMoveYet("No data moved yet")
        return self._from_to


class MySQLMetaRing:
    RING_VNODES: int = 80


class MySQLRing(
    Ring,
    MySQLMetaRing,
):
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        **kwargs,
    ):
        if "default_vnodes" in kwargs:
            del kwargs["default_vnodes"]
        super().__init__(
            default_vnodes=self.RING_VNODES,
            **kwargs,
        )
        self._mysql_info: MmyMySQLInfo = mysql_info

    def table_names(self):
        _auth_by_tables: MySQLAuthInfoByTable = self._mysql_info.auth_by_tables()
        return list(_auth_by_tables.tables.keys())

    async def move(self, data: MoveData) -> MovedData:
        logger.info("Move MySQL data")
        _from: Md = data._from
        _to: Node = data._to

        _from_client = MySQLClient(
            host=_from.node.host,
            port=_from.node.port,
            auth=self._mysql_info,
        )
        _to_client = MySQLClient(
            host=_to.host,
            port=_to.port,
            auth=self._mysql_info,
        )
        assert _from.node != _to
        await _from_client.ping()
        await _to_client.ping()
        _tables: list[MovedTableData] = list()
        for table in self.table_names():
            tablename: TableName = TableName(table)
            _from_data = await _from_client.consistent_hashing_select(
                table=tablename,
                start=SQLPoint(
                    point=_from.start_point,
                    equal=False,
                    greater=True,
                    less=False,
                ),
                end=SQLPoint(
                    point=_from.end_point,
                    equal=True,
                    greater=False,
                    less=True,
                ),
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
                _tables.append(
                    MovedTableData(
                        tablename=tablename,
                        moved_count=len(frag),
                    )
                )

        _moved: MovedData = MovedData(
            tables=_tables,
        )
        return _moved

    async def _delete_data_from_node(self, node: Node, start: SQLPoint, end: SQLPoint):
        cli = MySQLClient(
            host=node.host,
            port=node.port,
            auth=self._mysql_info,
        )
        for table in self.table_names():
            await cli.consistent_hashing_delete(
                table=table,
                start=start,
                end=end,
            )

    async def delete_from_old_nodes(self, new_node: Node):
        """

        Before added node:

        A                 B
        *-----------------*
        |>---------------=|
                 This range's ower is B node.

        After added node:

             Added node
        A        |        B
        *--------*--------*
        |>------=|
             |
             |__ This range's owner is added node.
                 So, The data of this range in B node is no longer needed.

        """
        _ftd: list[MDP] = self.from_to_data

        logger.info(f"Move data points: {len(_ftd)}")

        for item in _ftd:
            from_node = item._from.node
            to_node = item._to
            if from_node == new_node:
                raise RuntimeError(
                    f"From node({from_node.address_format()}) must not be new node({new_node.address_format()})"
                )
            if to_node != new_node:
                raise RuntimeError(
                    f"To node({to_node.address_format()}) must be new node({new_node.address_format()})"
                )

            logger.info(f"Old data delete from {from_node.address_format()}")
            logger.info(f"  Start: {item.start}")
            logger.info(f"  End:   {item.end}")
            await self._delete_data_from_node(
                node=from_node,
                start=SQLPoint(
                    point=item.start,
                    equal=False,
                    greater=True,
                    less=False,
                ),
                end=SQLPoint(
                    point=item.end,
                    equal=True,
                    greater=False,
                    less=True,
                ),
            )

        return

    async def delete_data_from_deleted_node(self, node: Node):
        _ftd: list[MDP] = self.from_to_data
        logger.info(f"Move data points: {len(_ftd)}")
        assert len(_ftd) == self._default_vnodes

        for item in _ftd:
            from_node: Node = item._from.node
            to_node: Node = item._to
            """

                Deleted node
            A        |        B
            *--------*--------*
            |>------=| 
                 |
                 |__ This range's owner is deleted node.

                     The data of range that is from *(A) to *(Deleted node) is moved to "B" node.

            """
            if from_node == node:
                """

                from_node is A node in above comment

                """
                raise RuntimeError(
                    f"From node({from_node.address_format()}) must not be deleted node({node.address_format()})"
                )
            if to_node != node:
                """

                to_node is B node in above comment

                """
                raise RuntimeError(
                    f"To node({to_node.address_format()}) must not be deleted node({node.address_format()})"
                )

            logger.info(f"Old data delete from {from_node.address_format()}")
            logger.info(f"  Start: {item.start}")
            logger.info(f"  End:   {item.end}")
            await self._delete_data_from_node(
                node=from_node,
                start=SQLPoint(
                    point=item.start,
                    equal=False,
                    greater=True,
                    less=False,
                ),
                end=SQLPoint(
                    point=item.end,
                    equal=True,
                    greater=False,
                    less=True,
                ),
            )

        return

    async def _optimize_table(self, node: Node):
        for table in self.table_names():
            _from_client = MySQLClient(
                host=node.host,
                port=node.port,
                auth=self._mysql_info,
            )
            await _from_client.optimize_table(
                table=table,
            )
            logger.info(f'Optimize MySQL table "{table}" on : {node.address_format()}')
        logger.info(f"Optimize MySQL node: {node.address_format()}")

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
                    f"From node({from_node.address_format()}) must not be new node({new_node.address_format()})"
                )

            await self._optimize_table(from_node)

    async def optimize_table_deleted_node(self, node: Node):
        await self._optimize_table(node)
