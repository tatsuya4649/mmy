import asyncio
import logging
from typing import Coroutine

from mmy.etcd import (
    EtcdConnectError,
    MmyEtcdInfo,
    MySQLEtcdClient,
    MySQLEtcdData,
    MySQLEtcdDuplicateNode,
)
from mmy.mysql.client import (
    MmyMySQLInfo,
    MySQLClient,
    MySQLInsertDuplicateBehavior,
    TableName,
)
from mmy.ring import MovedData, MovedTableData, MySQLRing, RingNoMoveYet
from mmy.server import Server, State, _Server, address_from_server, state_rich_str
from rich import print

from .add import PingType

logger = logging.getLogger(__name__)


class MySQLDeleterNoServer(ValueError):
    pass


from enum import auto

from .step import Step, Stepper, StepType


class DeleterStep(Step):
    # Step1. Start to delete existed node
    Init = auto()
    # Step2. Fetch current MySQL nodes status from etcd cluster
    FetchNodesInfo = auto()
    # Step3. Check deleted node be exsited on current MySQL nodes of etcd
    CheckServerIsValid = auto()
    # Step4. Calculate new hash ring with MySQL nodes excluding deleted node
    HashRingCalculate = auto()
    # Step5. Ping with MySQL nodes that is related to moving data
    Ping = auto()
    # Step6. Actually data move
    DataMove = auto()
    # Step7. Update state of deleted node to Move
    UpdateDeletedNodeState = auto()
    # Step8. Inconsistency test about moved data
    TestMovedData = auto()
    # Step9. Move data again since there may be data inserted between "Step6" and "Step7"
    DataMoveAgain = auto()
    # Optional: Delete data from deleted node
    DeleteDataFromDeletedNode = auto()
    # Step10. Delete node from MySQL nodes info on etcd
    DeleteNode = auto()
    # Step11. Done
    Done = auto()


SLEEP_SECS_BEFORE_DATAMOVE_AGAIN: int = 10


class MySQLDeleterNotExistError(ValueError):
    pass


class MySQLDeleter(Stepper):
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
    ):
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info
        self._step: DeleterStep = DeleterStep.Init
        self._ring: MySQLRing = MySQLRing(
            mysql_info=self._mysql_info,
            init_nodes=[],
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
        )
        self._prev_ring: MySQLRing = MySQLRing(
            mysql_info=self._mysql_info,
            init_nodes=[],
        )
        self._rows_by_table_before_move: dict[TableName, int] = dict()

    def update_step(self, new: StepType):
        if not isinstance(new, DeleterStep):
            raise TypeError
        self._step = new

    @property
    def step(self) -> DeleterStep:
        return self._step

    @property
    def ring(self):
        return self._ring

    @property
    def prev_ring(self):
        return self._prev_ring

    async def ping_mysql(self, _s: _Server):
        try:
            server_cli = MySQLClient(
                host=_s.host,
                port=_s.port,
                auth=self._mysql_info,
            )
            await server_cli.ping()
        except asyncio.TimeoutError:
            logger.error(f"PING error with new MySQL server: {_s.address_format()}")

    async def actually_move_data(
        self, moves: list[Coroutine[None, None, MovedData | None]]
    ) -> list[MovedData]:
        moved_datas: list[MovedData] = list()
        logger.info(f"Moves length: {len(moves)}")
        for _, move in enumerate(moves):
            moved_data = await move
            assert moved_data is not None
            moved_datas.append(moved_data)
        return moved_datas

    async def move_data_again(self):
        moves = self.ring.regenerate_move_coroutines(
            allow_duplicate=True,
        )
        logger.info(f"Again moves count: {len(moves)}")
        for _, move in enumerate(moves):
            await move

        return

    async def must_exist_deleted_node_etcd_nodes(self, data: MySQLEtcdData):
        for node in data.nodes:
            if node == self.server:
                return
        else:
            raise MySQLDeleterNotExistError(
                f"Not exists deleted node({self.server.address_format()}) on etcd"
            )

    async def ping(
        self,
        ping_type: PingType,
        etcd_data: MySQLEtcdData,
    ):
        match ping_type:
            case PingType.OnlyTargetServer:
                ping_nodes: list[Server] = list()
                for mdp in self._ring.from_to_data:
                    _from = mdp._from.node
                    _to = mdp._to

                    if _from not in ping_nodes:
                        ping_nodes.append(_from)
                    if _to not in ping_nodes:
                        ping_nodes.append(_to)

                for ping_node in ping_nodes:
                    await self.ping_mysql(ping_node)
            case PingType.AllServer:
                for _node in etcd_data.nodes:
                    await self.ping_mysql(_node)
            case PingType.NonPing:
                pass
            case _:
                raise ValueError

    async def fetch_rows_by_tables(self):
        cli = MySQLClient(
            host=self.server.host,
            port=self.server.port,
            auth=self._mysql_info,
        )
        for table in self._ring.table_names():
            rows = await cli.get_table_rows(table)
            self._rows_by_table_before_move[table] = rows

        return

    async def do(
        self,
        server: _Server,
        ping_type: PingType = PingType.AllServer,
        delete_data: bool = True,
        sleep_secs_before_datamove_again: int | float = 10,
        not_update_while_moving: bool = False,
    ):
        """
        @params:
            not_update_while_moving: Will not operate like INSERT, UPDATE, DELETE, etc that affect rows while moving data

        """
        self.server = server
        try:
            for etcd in self._etcd_info.nodes:
                self.update_step(DeleterStep.Init)
                try:
                    etcd_cli = MySQLEtcdClient(
                        host=etcd.host,
                        port=etcd.port,
                    )
                    self.update_step(DeleterStep.FetchNodesInfo)
                    now_data: MySQLEtcdData = await etcd_cli.get()
                except EtcdConnectError:
                    continue

                self.update_step(DeleterStep.CheckServerIsValid)
                await self.must_exist_deleted_node_etcd_nodes(now_data)

                self._prev_ring = MySQLRing(
                    mysql_info=self._mysql_info,
                    init_nodes=now_data.nodes,
                )
                self._ring = MySQLRing(
                    mysql_info=self._mysql_info,
                    init_nodes=now_data.nodes,
                    insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
                )
                node = Server(
                    host=server.host,
                    port=server.port,
                    state=State.Run,
                )
                prev_nodes_length: int = len(self._ring)
                self.update_step(DeleterStep.HashRingCalculate)
                moves: list[
                    Coroutine[None, None, MovedData | None]
                ] = await self._ring.delete(
                    node=node,
                )
                new_nodes_length: int = len(self._ring)
                if new_nodes_length == 0:
                    raise MySQLDeleterNoServer(
                        "If delete this server, there won't be one left"
                    )
                logger.info(f"Nodes count: {prev_nodes_length} => {new_nodes_length}")
                assert new_nodes_length == prev_nodes_length - 1

                self.update_step(DeleterStep.Ping)
                await self.ping(
                    ping_type=ping_type,
                    etcd_data=now_data,
                )

                try:
                    self.update_step(DeleterStep.DataMove)
                    await self._log_table_size()
                    await self.fetch_rows_by_tables()
                    # Actually moving data
                    moved_datas: list[MovedData] = await self.actually_move_data(moves)

                    # The reason why not updating state to Move before moving data,
                    # If set "Move" state, rejecting the data from the mmy proxy.
                    # So, Do moving data, and update state, and moving data again.
                    self.update_step(DeleterStep.UpdateDeletedNodeState)
                    logger.info("Update state of deleted MySQL node on etcd")
                    await etcd_cli.update_state(
                        server=server,
                        state=State.Move,
                    )
                    logger.info("New MySQL state: %s" % (State.Run.value))
                    self.update_step(DeleterStep.TestMovedData)
                    await self.test_moved_data(
                        moved_datas,
                        not_update_while_moving=not_update_while_moving,
                    )

                    # Wait for node information to propagate in all etcd nodes
                    self.update_step(DeleterStep.DataMoveAgain)
                    await asyncio.sleep(sleep_secs_before_datamove_again)

                    await self.move_data_again()

                    if delete_data:
                        self.update_step(DeleterStep.DeleteDataFromDeletedNode)
                        logger.info(
                            "Delete unnecessary data(non owner of hashed) from deleted MySQL node"
                        )
                        try:
                            await self._ring.delete_data_from_deleted_node(
                                node=node,
                            )
                            logger.info("Optimize MySQL nodes with data moved")
                            await self._ring.optimize_table_deleted_node(
                                node=node,
                            )
                        except RingNoMoveYet:
                            logger.info("No moved data")

                    self.update_step(DeleterStep.DeleteNode)
                    await etcd_cli.delete_node(
                        server=server,
                    )
                    self.update_step(DeleterStep.Done)
                    logger.info("Done !")
                    return
                except EtcdConnectError:
                    continue
            else:
                raise EtcdConnectError("No alive etcd nodes")
        except MySQLEtcdDuplicateNode:
            logger.error("This IP address already exists on etcd")

    async def test_moved_data(
        self,
        datas: list[MovedData],
        not_update_while_moving: bool,
    ):
        _d: dict[TableName, int] = dict()
        logger.info("TEST MOVED DATA")
        for data in datas:
            for _table in data.tables:
                logger.info(
                    f"{_table}: {data._from.start_point[:4]}~{data._from.end_point[:4]}: {_table.moved_count}"
                )
                _d.setdefault(_table.tablename, 0)
                _d[_table.tablename] += _table.moved_count

        for table, count in _d.items():
            _e = self._rows_by_table_before_move[table]
            logger.info("Moved data info:")
            if not_update_while_moving:
                logger.info(
                    f"{table} - Expected count: {_e}, Actually delete count: {count}"
                )
                assert count == _e
            else:
                logger.info(f"{table} - Delete count: {count}")

    async def _log_table_size(self):
        for table in self._ring.table_names():
            server_cli = MySQLClient(
                host=self.server.host,
                port=self.server.port,
                auth=self._mysql_info,
            )
            mb: int = await server_cli.get_table_size_in_kb(
                table=table,
            )
            rows: int = await server_cli.get_table_rows(
                table=table,
            )
            logger.info(f"Table size {table}: {rows}Rows {mb}KB")
