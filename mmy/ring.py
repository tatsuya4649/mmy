import asyncio
import bisect
import copy
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Coroutine

from pymysql.err import IntegrityError
from rich import print
from uhashring import HashRing

from .mysql.client import (
    MmyMySQLInfo,
    MySQLAuthInfoByTable,
    MySQLClient,
    MySQLColumns,
    MySQLFetchAll,
    MySQLInsertDuplicateBehavior,
    TableName,
)
from .mysql.errcode import MySQLErrorCode
from .mysql.sql import SQLPoint, _Point, _SQLPoint
from .server import Server, State, _Server


@dataclass
class Point:
    have_node: Server
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
    node: Server
    start_point: _Point
    start_equal: bool
    end_point: _Point
    end_equal: bool
    """
    If true, SQL statement is MD(key)>"start_point" OR MD(key)<"end_point", otherwise change "OR" to "AND" in it.
    """
    _or: bool


@dataclass
class MoveData:
    _from: Md
    _to: Server
    insert_ignore: bool | None


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
    async def add(self, node: Server):
        """
        add new node
        """

    @abstractmethod
    async def delete(self, node: Server):
        """
        delete node
        """


@dataclass
class MDP(MoveData):
    start: _Point
    end: _Point


@dataclass
class MDPWithMoved(MDP):
    moved_data: int


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
    _from: Md
    _to: Server


logger: logging.Logger = logging.getLogger(__name__)


def reset_from_to_parameter(func):
    _f = asyncio.iscoroutinefunction(func)

    if _f:

        async def _inner(*args, **kwargs):
            ring: Ring = args[0]
            assert isinstance(ring, Ring) is True
            ring._from_to = list()
            return await func(*args, **kwargs)

    else:

        def _inner(*args, **kwargs):
            ring: Ring = args[0]
            assert isinstance(ring, Ring) is True
            ring._from_to = list()
            return func(*args, **kwargs)

    return _inner


class Ring(RingHandler):
    LENGTH: int = 4

    def __init__(
        self,
        init_nodes: list[Server] = list(),
        default_vnodes: int = 80,
        debug: bool = True,
    ):
        self._default_vnodes = default_vnodes
        self._node_hash: dict[str, Server] = dict()
        self.init(nodes=init_nodes)
        self._from_to: list[MDP] | None = None
        self._debug = debug

    def ring_hash(self, key):
        return hashlib.md5(str(key).encode("utf-8")).hexdigest()

    def __len__(self) -> int:
        return len(self._node_hash)

    def nodename_from_node(self, node: Server) -> str:
        return f"%s:%d" % (str(node.host), node.port)

    def ring_node_count(self) -> int:
        return self._hr.size

    def ring_points(self):
        return self._hr.get_points()

    def get_without_move(self, data: Any):
        return self._hr_without_move.get(data)

    def get_instances(self):
        return self._hr.get_instances()

    def get(self, data: Any):
        return self._hr.get(data)

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

    def regenerate_move_coroutines(
        self,
        allow_duplicate: bool,
    ) -> list[Coroutine[None, None, MovedData | None]]:
        if self._from_to is None:
            return []

        """
        Regenerate coroutines from prev "moved data" info.
        """
        _res = list()
        for mdp in self._from_to:
            _res.append(
                MDP(
                    _from=mdp._from,
                    _to=mdp._to,
                    start=mdp.start,
                    end=mdp.end,
                    insert_ignore=allow_duplicate,
                )
            )

        mdps = [self._move(mdp) for mdp in _res]
        return mdps

    @reset_from_to_parameter
    def init(self, nodes: list[Server]):
        _nodes = dict()
        self._node_hash.clear()
        for _in in nodes:
            _nodename = self.nodename_from_node(_in)
            _nodes[_nodename] = {
                "instance": _in,
                "vnodes": self._default_vnodes,
            }
            self._node_hash[_nodename] = _in

        self._hr = HashRing(
            nodes=_nodes,
            hash_fn=self.ring_hash,
        )
        self._hr_without_move = HashRing(
            nodes=_nodes,
            hash_fn=self.ring_hash,
        )
        return

    @reset_from_to_parameter
    async def add(self, node: Server) -> list[Coroutine[None, None, MovedData | None]]:

        points: list[tuple[str, str]] = self._hr.get_points()
        _nodename: str = self.nodename_from_node(node)
        old_keys: list[str] = self._hr._keys
        old_keys.sort()

        # Actually add
        self._hr.add_node(
            _nodename,
            {
                "instance": node,
                "vnodes": self._default_vnodes,
            },
        )
        if node.state is not State.Move:
            self._hr_without_move.add_node(
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

        added_items: list[str] = [
            i[0] for i in self._hr.get_points() if i[1] == node.address_format()
        ]
        new_keys: list[str] = self._hr._keys
        new_keys.sort()

        def get_add_point(item) -> Point:
            return Point(
                have_node=node,
                point=_Point(item),
                index=new_keys.index(item),
            )

        def get_prev_add_point(index) -> Point:
            point: str = added_items[index - 1]
            _index: int = new_keys.index(point)
            return Point(
                have_node=node,
                point=_Point(point),
                index=_index,
            )

        def get_next_point(item) -> Point:
            _next_index = bisect.bisect_right(old_keys, item)
            if _next_index == len(points):
                _next_index = 0

            point, nodename = points[_next_index]
            _node: Server = self._node_hash[nodename]
            np = Point(
                have_node=_node,
                point=_Point(point),
                index=_next_index,
            )
            return np

        def get_prev_point(item) -> tuple[int, Point]:
            _prev_index = bisect.bisect_left(old_keys, item)
            _pi = _prev_index - 1
            point, nodename = points[_pi]
            _node: Server = self._node_hash[nodename]
            pp = Point(
                have_node=_node,
                point=_Point(point),
                index=_pi,
            )
            return _pi, pp

        logger.debug(f"Add node {_nodename} ")
        self._from_to = list()
        _cos: list[Coroutine[None, None, MovedData | None]] = list()
        for index, item in enumerate(added_items):
            ap: Point = get_add_point(item)  # added point
            np: Point = get_next_point(item)  # next point
            prev_index, pp = get_prev_point(item)  # prev point
            pap: Point = get_prev_add_point(index)  # prev added point
            assert pp.have_node != node
            assert np.have_node != node
            assert ap.have_node == node
            if pap is not None:
                assert pap.have_node == node

            """

            A: Added node
            |> : Greater than
            =| : Less than or equal

            Pattern 1.

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


            """
            first_added: _Point = _Point(added_items[0])
            first_points: _Point = _Point(points[0][0])
            """

            fa: first_added
            fp: first_points
            np: next_points

            0x00..00 --=|                0xff..ff
                *-------*-----* ....  *------*
                        fa   fp    pap or pp

            0x00..00    |<---=|
                *-------*-*-*-*----*
                        fp   fa    np

            """
            if index == 0:
                if first_points < first_added:
                    _from: Md = Md(
                        node=np.have_node,
                        start_point=pp.point,
                        start_equal=False,
                        end_point=first_added,
                        end_equal=True,
                        _or=index == 0 and prev_index < 0,
                    )
                else:
                    _from = Md(
                        node=pp.have_node,
                        start_point=pap.point if pap >= pp else pp.point,
                        start_equal=False,
                        end_point=first_added,
                        end_equal=True,
                        _or=index == 0 and prev_index < 0,
                    )
            else:
                _from = Md(
                    node=np.have_node,
                    start_point=pap.point if pap >= pp else pp.point,
                    start_equal=False,
                    end_point=ap.point,
                    end_equal=True,
                    _or=False,
                )

            _to: Server = ap.have_node
            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=_from.start_point,
                end=_from.end_point,
                insert_ignore=False,
            )
            assert _to == node
            assert _from.node != node
            self._from_to.append(mdp)
            _cos.append(self._move(mdp))

        return _cos

    @reset_from_to_parameter
    async def delete(
        self, node: Server
    ) -> list[Coroutine[None, None, MovedData | None]]:
        _nodename: str = self.nodename_from_node(node)
        keys: list[str] = self._hr._keys
        keys.sort()

        deleted_items: list[str] = [
            i[0] for i in self._hr.get_points() if i[1] == node.address_format()
        ]
        # Before deleting hash ring keys
        old_keys: list[str] = self._hr._keys
        old_keys.sort()

        # Actually remove from Hash ring
        del self._node_hash[_nodename]
        self._hr.remove_node(_nodename)

        points: list[tuple[str, str]] = self._hr.get_points()
        if self._debug is True:
            _f = list(filter(lambda x: x[0] == _nodename, self._hr.get_points()))
            assert len(_f) == 0

        new_keys: list[str] = self._hr._keys
        new_keys.sort()
        if len(self) == 0:
            # No node in this ring. So, do not anything.
            return []

        def get_delete_point(item: str) -> Point:
            return Point(
                have_node=node,
                point=_Point(item),
                index=old_keys.index(item),
            )

        def get_prev_delete_point(index: int) -> Point:
            point: str = deleted_items[index - 1]
            _index: int = old_keys.index(point)
            return Point(
                have_node=node,
                point=_Point(point),
                index=_index,
            )

        def get_next_point(item: str) -> Point:
            _next_index = bisect.bisect_right(new_keys, item)
            if _next_index == len(points):
                _next_index = 0

            point, nodename = points[_next_index]
            _node: Server = self._node_hash[nodename]
            np = Point(
                have_node=_node,
                point=_Point(point),
                index=_next_index,
            )
            return np

        def get_prev_point(item: str) -> tuple[int, Point]:
            _prev_index = bisect.bisect_left(new_keys, item)
            _pi = _prev_index - 1
            point, nodename = points[_pi]
            _node: Server = self._node_hash[nodename]
            pp = Point(
                have_node=_node,
                point=_Point(point),
                index=_pi,
            )
            return _pi, pp

        logger.info(f"Delete node: {_nodename}")

        self._from_to = list()
        _cos: list[Coroutine[None, None, MovedData | None]] = list()

        for index, item in enumerate(deleted_items):
            dp: Point = get_delete_point(item)
            np: Point = get_next_point(item)
            prev_index, pp = get_prev_point(item)
            pdp: Point = get_prev_delete_point(index)  # prev deleted point

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

        Ex3.

            Before:

            0x00..00  D    N        P       0xff..ff
                * ----*----*--------*--------- *
                 ----=|             |>--------

                    Owner of "D" node.
                        |
                        |       Move to next
                        |___
                           |
            After:         |

            0x00..00       N       P       0xff..ff
                * ---------*--------*--------- *

            """
            first_deleted: _Point = _Point(deleted_items[0])
            first_points: _Point = _Point(points[0][0])
            """

            fd: first_deleted
            fp: first_points

            0x00..00 --=|                0xff..ff
                *-------*-----* ....  *------*
                        fd   fp    pdp or pp

            0x00..00    |<---=|
                *-------*-*-*-* 
                        fp   fd

            """
            if index == 0:
                if first_points < first_deleted:
                    _from: Md = Md(
                        node=dp.have_node,
                        start_point=pp.point,
                        start_equal=False,
                        end_point=first_deleted,
                        end_equal=True,
                        _or=index == 0 and prev_index < 0,
                    )
                else:
                    _from = Md(
                        node=dp.have_node,
                        start_point=pdp.point if pdp >= pp else pp.point,
                        start_equal=False,
                        end_point=first_deleted,
                        end_equal=True,
                        _or=index == 0 and prev_index < 0,
                    )
            else:
                _from = Md(
                    node=dp.have_node,
                    start_point=pdp.point if pdp >= pp else pp.point,
                    start_equal=False,
                    end_point=dp.point,
                    end_equal=True,
                    _or=False,
                )
            _to: Server = np.have_node
            assert _to != node

            mdp: MDP = MDP(
                _from=_from,
                _to=_to,
                start=_from.start_point,
                end=_from.end_point,
                insert_ignore=None,
            )
            self._from_to.append(mdp)
            logger.info(
                f"{_from.start_point[:self.LENGTH]}~{_from.end_point[:self.LENGTH]}"
            )
            _cos.append(self._move(mdp))

        return _cos

    async def _move(self, mdp: MDP) -> MovedData | None:
        _from: Md = mdp._from
        _to: Server = mdp._to
        start: _Point = mdp.start
        end: _Point = mdp.end
        if _from.node == _to:
            logger.info("From node and To node is same")
            return MovedData(
                tables=[],
                _from=_from,
                _to=_to,
            )
        logger.info(
            f"Move data({start[:self.LENGTH]}~{end[:self.LENGTH]}): {_from.node.address_format()} ==> {_to.address_format()} "
        )
        return await self.move(
            MoveData(
                _from=_from,
                _to=_to,
                insert_ignore=mdp.insert_ignore,
            )
        )

    def not_owner_points(self, node: Server) -> list[Md]:
        points: list[tuple[str, str]] = self._hr.get_points()
        res: list[Md] = list()

        for index, point in enumerate(points):
            _prev_point, _ = points[index - 1]
            _point, nodename = point
            _node: Server = self._node_hash[nodename]
            if _node != node:
                _from: Md = Md(
                    node=_node,
                    start_point=_Point(_prev_point),
                    start_equal=False,
                    end_point=_Point(_point),
                    end_equal=True,
                    _or=index == 0,
                )
                res.append(_from)

        return res

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
        insert_duplicate_behavior: MySQLInsertDuplicateBehavior = MySQLInsertDuplicateBehavior.Raise,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )
        self._insert_duplicate_behavior: MySQLInsertDuplicateBehavior = (
            insert_duplicate_behavior
        )
        self._mysql_info: MmyMySQLInfo = mysql_info

    def table_names(self):
        _auth_by_tables: MySQLAuthInfoByTable = self._mysql_info.auth_by_tables()
        return list(_auth_by_tables.tables.keys())

    async def move(
        self,
        data: MoveData,
    ) -> MovedData:
        _from: Md = data._from
        _to: Server = data._to

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
                _or=_from._or,
            )
            columns: list[MySQLColumns] = await _from_client.columns_info(tablename)
            excluded_columns: list[
                MySQLColumns
            ] = _from_client.exclude_auto_increment_columns(columns)

            async for frag in _from_data:
                try:
                    affected: int = await _to_client.insert(
                        table=tablename,
                        datas=frag,
                        insert_ignore=data.insert_ignore,
                    )
                except IntegrityError as e:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                    if MySQLErrorCode.ER_DUP_ENTRY.value.error_code == err_code:
                        match self._insert_duplicate_behavior:
                            case MySQLInsertDuplicateBehavior.Raise:
                                raise e
                            case MySQLInsertDuplicateBehavior.DeleteAutoIncrement:
                                logger.warning("Detect Duplicate entry: %s", err_msg)
                                modified_frag: MySQLFetchAll = (
                                    _from_client.exclude_data_by_columns(
                                        excluded_columns, frag
                                    )
                                )
                                assert len(frag) == len(modified_frag)
                                await _to_client.insert(
                                    table=tablename,
                                    datas=modified_frag,
                                    insert_ignore=data.insert_ignore,
                                )

                _tables.append(
                    MovedTableData(
                        tablename=tablename,
                        moved_count=affected,
                    )
                )
        _moved: MovedData = MovedData(
            tables=_tables,
            _from=_from,
            _to=_to,
        )
        return _moved

    async def _delete_data_from_node(
        self, node: Server, start: SQLPoint, end: SQLPoint, _or: bool
    ):
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
                _or=_or,
            )

    async def check_rows_0(self, node: Server):
        cli = MySQLClient(
            host=node.host,
            port=node.port,
            auth=self._mysql_info,
        )
        for table in self.table_names():
            rows: int = await cli.get_table_rows(
                table=table,
            )
            logger.info(
                f"The number of {table} rows must be 0 on deleted node: {rows}rows"
            )
            assert rows == 0

    async def delete_from_old_nodes(self, new_node: Server):
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

            logger.info(
                f"Old data delete({item.start[:self.LENGTH]}~{item.end[:self.LENGTH]}) from {from_node.address_format()}"
            )
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
                _or=item._from._or,
            )

        return

    async def delete_data_from_deleted_node(self, node: Server):
        _ftd: list[MDP] = self.from_to_data
        assert len(_ftd) == self._default_vnodes

        for item in _ftd:
            from_node: Server = item._from.node
            to_node: Server = item._to
            """

                Deleted node
            A        |        B
            *--------*--------*
            |>------=| 
                 |
                 |__ This range's owner is deleted node.

                     The data of range that is from *(A) to *(Deleted node) is moved to "B" node.

            """
            if from_node != node:
                """

                from_node is A node in above comment

                """
                raise RuntimeError(
                    f"From node({from_node.address_format()}) must be delete({node.address_format()})"
                )
            if to_node == node:
                """

                to_node is B node in above comment

                """
                raise RuntimeError(
                    f"To node({to_node.address_format()}) must not be deleted node({node.address_format()})"
                )

            logger.info(
                f"Old data delete({item.start[:self.LENGTH]}~{item.end[:self.LENGTH]}) from {from_node.address_format()}"
            )
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
                _or=item._from._or,
            )

        await self.check_rows_0(node=node)
        return

    async def _optimize_table(self, node: Server):
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

    async def optimize_table_old_nodes(self, new_node: Server):
        _ftd: list[MDP] = self.from_to_data
        _from_nodes: list[Server] = list()
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

    async def optimize_table_deleted_node(self, node: Server):
        await self._optimize_table(node)
