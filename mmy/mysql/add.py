import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum, auto
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
from mmy.mysql.errcode import co_retry
from mmy.ring import MovedData, MySQLRing, RingNoMoveYet
from mmy.server import Server, State, _Server, address_from_server
from rich import print

from .step import Step, Stepper, StepType

logger = logging.getLogger(__name__)


class PingType(Enum):
    OnlyTargetServer = auto()
    AllServer = auto()
    NonPing = auto()


class AdderStep(Step):
    # Step1. Start to add new node
    Init = auto()
    # Step2. Fetch current MySQL nodes status from etcd cluster
    FetchNodesInfo = auto()
    # Step3. Calculate new hash ring with MySQL nodes from "Step2" and new node
    HashRingCalculate = auto()
    # Step4. Ping with MySQL nodes that is related to moving data
    Ping = auto()
    # Step5. Put new node info into etcd cluster
    PutNewNodeInfo = auto()
    # Step6. Actually data is moved
    DataMove = auto()
    # Step7. Update state of "Step5" to Run
    UpdateNewNodeState = auto()
    # Step8. Move data again since there may be data inserted between "Step5" and "Step7"
    DataMoveAgain = auto()
    # Step9. Delete necassary data from old nodes
    DeleteFromOldNodes = auto()
    # Step9. Optimize table for reducing usage of disk
    OptimizeTable = auto()
    # Step10. Done
    Done = auto()


def rollback(func):
    async def _wrap_rollback(*args, **kwargs):
        adder = args[0]
        assert isinstance(adder, MySQLAdder)

        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Do rollback
            await adder._rollback()
            raise e

    return _wrap_rollback


def _get_ping_retry_count() -> int | float:
    return PING_RETRY_COUNT


def _get_ping_retry_interval_secs() -> int | float:
    return PING_RETRY_INTERVAL_SECS


def multiple_retry(cor):
    async def _wrap_rollback(*args, **kwargs):
        adder = args[0]
        assert isinstance(adder, MySQLAdder)
        _retry_count = _get_ping_retry_count()
        _retry_interval_count = _get_ping_retry_interval_secs()
        return await co_retry(
            co=cor,
            retry_count=_retry_count,
            retry_interval_secs=_retry_interval_count,
            args=args,
            kwargs=kwargs,
        )

    return _wrap_rollback


PING_RETRY_COUNT = 3
PING_RETRY_INTERVAL_SECS: int = 10

SLEEP_SECS_BEFORE_DATAMOVE_AGAIN: int = 10
SLEEP_SECS_AFTER_ETCD_ADD_NEW_NODE: int = 10


class MySQLAdder(Stepper):
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
    ):
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info
        self._step: AdderStep = AdderStep.Init
        self._ring: MySQLRing = MySQLRing(
            mysql_info=self._mysql_info,
            init_nodes=[],
            insert_duplicate_behavior=MySQLInsertDuplicateBehavior.DeleteAutoIncrement,
        )

    def update_step(self, new: StepType):
        if not isinstance(new, AdderStep):
            raise TypeError
        self._step = new

    @property
    def step(self) -> AdderStep:
        return self._step

    @property
    def ring(self):
        return self._ring

    @multiple_retry
    async def ping_mysql(self, _s: _Server):
        try:
            server_cli = MySQLClient(
                host=_s.host,
                port=_s.port,
                auth=self._mysql_info,
            )
            await server_cli.ping()
        except asyncio.TimeoutError as e:
            logger.error(f"PING error with new MySQL server: {_s.address_format()}")
            raise e

    async def datamove_rollback(
        self,
    ):
        server_cli = MySQLClient(
            host=self.server.host,
            port=self.server.port,
            auth=self._mysql_info,
        )
        for table in self._mysql_info.tables:
            await server_cli.delete_all_table_data(table=TableName(table.name))
            await server_cli.optimize_table(table=TableName(table.name))

        for etcd in self._etcd_info.nodes:
            try:
                etcd_cli = MySQLEtcdClient(
                    host=etcd.host,
                    port=etcd.port,
                )
                await etcd_cli.delete_node(
                    server=self.server,
                )
                return
            except EtcdConnectError:
                continue
        else:
            raise EtcdConnectError("Connection error with etcd cluster")

    async def update_new_node_state_rollback(self):
        await self.datamove_rollback()

    async def delete_from_old_nodes_rollback(self):
        await self.update_new_node_state_rollback()

    async def optimize_table_rollback(self):
        return

    async def _rollback(self):
        match self._step:
            case AdderStep.Init:
                return
            case AdderStep.FetchNodesInfo:
                return
            case AdderStep.HashRingCalculate:
                logger.info("Failed to calculate hash ring")
                return
            case AdderStep.Ping:
                logger.info("Failed to ping with MySQL server")
                return
            case AdderStep.PutNewNodeInfo:
                logger.info("Failed to put new node info into etcd cluster")
                return
            case AdderStep.DataMove:
                logger.info("Failed to actually move data")
                await self.datamove_rollback()
                return
            case AdderStep.UpdateNewNodeState | AdderStep.DataMoveAgain:
                logger.info("Failed to update new node state")
                await self.update_new_node_state_rollback()
                return
            case AdderStep.DeleteFromOldNodes:
                logger.info("Failed to delete unnecessary data from old nodes")
                await self.delete_from_old_nodes_rollback()
                return
            case AdderStep.OptimizeTable:
                logger.info("Failed to optimize table")
                await self.optimize_table_rollback()
                return
            case AdderStep.Done:
                return

            case _:
                raise RuntimeError

    def init_ring(self, now_etcd: MySQLEtcdData):
        # Initialize Hash ring with MySQL nodes info on etcd
        self._ring.init(nodes=now_etcd.nodes)

    async def actually_move_data(
        self, moves: list[Coroutine[None, None, MovedData | None]]
    ) -> list[MovedData]:
        moved_datas: list[MovedData] = list()
        for index, move in enumerate(moves):
            logger.info(f"Move count: {index}/{len(moves)}")
            moved_data: MovedData | None = await move
            if moved_data is not None:
                moved_datas.append(moved_data)

        moved_datas = sorted(
            moved_datas,
            key=lambda x: x._from.start_point,
        )
        return moved_datas

    async def move_data_again(self):
        moves = self.ring.regenerate_move_coroutines(
            allow_duplicate=True,
        )
        for index, move in enumerate(moves):
            logger.info(f"Again move count: {index}/{len(moves)}")
            await move

        return

    def must_exist_deleted_node_etcd_nodes(
        self,
        etcd_data: MySQLEtcdData,
    ):
        logger.info([i.address_format() for i in etcd_data.nodes])
        for node in etcd_data.nodes:
            if node == self.server:
                raise MySQLEtcdDuplicateNode

        return

    @rollback
    async def do(
        self,
        server: _Server,
        ping_type: PingType = PingType.AllServer,
        delete_from_old: bool = True,
        sleep_secs_before_datamove_again: int = SLEEP_SECS_BEFORE_DATAMOVE_AGAIN,
        sleep_secs_after_etcd_add_new_node: int = SLEEP_SECS_AFTER_ETCD_ADD_NEW_NODE,
        raise_duplicate: bool = True,
    ):
        self.server = server
        try:
            for etcd in self._etcd_info.nodes:
                self.update_step(AdderStep.Init)
                try:
                    etcd_cli = MySQLEtcdClient(
                        host=etcd.host,
                        port=etcd.port,
                    )
                    self.update_step(AdderStep.FetchNodesInfo)
                    now_data: MySQLEtcdData = await etcd_cli.get()
                except EtcdConnectError:
                    continue

                if len(now_data.nodes) == 0:
                    self.update_step(AdderStep.PutNewNodeInfo)
                    _state = State.Run
                    await etcd_cli.add_new_node(
                        server,
                        init_state=_state,
                    )
                    node = Server(
                        host=server.host,
                        port=server.port,
                        state=_state,
                    )
                    await self.ring.add(
                        node=node,
                    )
                    self.update_step(AdderStep.Done)
                    return

                logger.info(now_data)
                self.update_step(AdderStep.HashRingCalculate)
                self.init_ring(now_etcd=now_data)
                prev_nodes_length: int = len(self.ring)
                node = Server(
                    host=server.host,
                    port=server.port,
                    state=State.Move,
                )
                self.must_exist_deleted_node_etcd_nodes(
                    etcd_data=now_data,
                )
                moves: list[
                    Coroutine[None, None, MovedData | None]
                ] = await self.ring.add(
                    node=node,
                )
                new_nodes_length: int = len(self.ring)
                logger.info(f"Nodes count: {prev_nodes_length} => {new_nodes_length}")
                assert new_nodes_length == prev_nodes_length + 1

                self.update_step(AdderStep.Ping)
                match ping_type:
                    case PingType.OnlyTargetServer:
                        ping_nodes: list[Server] = list()
                        for mdp in self.ring.from_to_data:
                            _from = mdp._from.node
                            _to = mdp._to

                            if _from not in ping_nodes:
                                ping_nodes.append(_from)
                            if _to not in ping_nodes:
                                ping_nodes.append(_to)

                        for node in ping_nodes:
                            await self.ping_mysql(node)
                    case PingType.AllServer:
                        etcd_cli = MySQLEtcdClient(
                            host=etcd.host,
                            port=etcd.port,
                        )
                        data: MySQLEtcdData = await etcd_cli.get()
                        for _node in data.nodes:
                            await self.ping_mysql(_node)
                    case PingType.NonPing:
                        pass
                    case _:
                        raise ValueError

                logger.info("Add new node: %s..." % (address_from_server(server)))
                _ft = self.ring._from_to
                try:
                    self.update_step(AdderStep.PutNewNodeInfo)
                    await etcd_cli.add_new_node(
                        server,
                        init_state=State.Move,
                    )
                    # Wait for node information to propagate
                    await asyncio.sleep(sleep_secs_after_etcd_add_new_node)

                    logger.info(f"Moving data fragments count: {len(moves)}")

                    self.update_step(AdderStep.DataMove)
                    moved_datas: list[MovedData] = await self.actually_move_data(moves)

                    self.update_step(AdderStep.UpdateNewNodeState)
                    logger.info("Inserted MySQL data from existed node into new node")
                    logger.info("Update state of new MySQL node on etcd")
                    await etcd_cli.update_state(
                        server=server,
                        state=State.Run,
                    )
                    logger.info("New MySQL state: %s" % (State.Run.value))
                    # Wait for node information to propagate
                    await asyncio.sleep(sleep_secs_before_datamove_again)
                    self.update_step(AdderStep.DataMoveAgain)

                    node = Server(
                        host=server.host,
                        port=server.port,
                        state=State.Move,
                    )
                    assert _ft == self.ring._from_to
                    await self.move_data_again()

                    logger.info(
                        "Delete unnecessary data(non owner of hashed) from existed MySQL node"
                    )
                    if delete_from_old is True:
                        try:
                            self.update_step(AdderStep.DeleteFromOldNodes)
                            await self.ring.delete_from_old_nodes(
                                new_node=node,
                            )
                            self.update_step(AdderStep.OptimizeTable)
                            logger.info("Optimize MySQL nodes with data moved")
                            await self.ring.optimize_table_old_nodes(
                                new_node=node,
                            )
                        except RingNoMoveYet:
                            logger.info("No moved data")

                    self.update_step(AdderStep.Done)
                    logger.info("Done !")
                    return
                except EtcdConnectError:
                    continue
            else:
                raise EtcdConnectError("No alive etcd nodes")
        except MySQLEtcdDuplicateNode as e:
            logger.error("This IP address already exists on etcd")
            if raise_duplicate:
                raise e
