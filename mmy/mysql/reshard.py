import logging
import time
from dataclasses import dataclass
from enum import auto
from typing import Any

from ..const import BENCHMARK_ROUND_DECIMAL_PLACES
from ..etcd import MmyEtcdInfo, MySQLEtcdClient, MySQLEtcdData
from ..ring import Md, MySQLRing, RingMeta
from ..server import Server
from .client import (
    MmyMySQLInfo,
    MySQLClient,
    MySQLInsertDuplicateBehavior,
    MySQLKeys,
    TableName,
)
from .sql import SQLPoint
from .step import Step, Stepper, StepType

logger = logging.getLogger(__name__)


class ReshardStep(Step):
    # Step1. Start to reshard data
    Init = auto()
    # Step2. Fetch current MySQL node's info from etcd
    FetchNodeInfo = auto()
    # Step3. Do actually reshard
    DoReshard = auto()
    # Step4. Delete unnecessary data from MySQL node of "Step3"
    DeleteUnnecessary = auto()
    # Step. Done
    Done = auto()


def rollback(func):
    async def _wrap_rollback(*args, **kwargs):
        adder = args[0]
        assert isinstance(adder, MySQLReshard)

        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Do rollback
            await adder._rollback()
            raise e

    return _wrap_rollback


@dataclass
class _Reshard:
    moved_rows: int
    elapsed: float


@dataclass
class ReshardResult:
    result_by_table: dict[
        TableName,
        _Reshard,
    ]
    mds: list[Md]
    tables: list[TableName]


NodeByResult = list[ReshardResult]


class MySQLReshard(Stepper, RingMeta):
    def __init__(
        self,
        mysql_info: MmyMySQLInfo,
        etcd_info: MmyEtcdInfo,
        insert_duplicate_behavior: MySQLInsertDuplicateBehavior = MySQLInsertDuplicateBehavior.Raise,
    ):
        self._mysql_info: MmyMySQLInfo = mysql_info
        self._etcd_info: MmyEtcdInfo = etcd_info
        self._ring: MySQLRing | None = None
        self._insert_duplicate_behavior = insert_duplicate_behavior
        self._step: ReshardStep = ReshardStep.Init
        self._reshard_server: Server | None = None

    def update_step(self, new: StepType):
        if not isinstance(new, ReshardStep):
            raise TypeError
        self._step = new

    @property
    def step(self) -> ReshardStep:
        return self._step

    async def get_mysql_nodes(self) -> MySQLEtcdData:
        for node in self._etcd_info.nodes:
            cli = MySQLEtcdClient(
                host=node.host,
                port=node.port,
            )
            mysql_info: MySQLEtcdData = await cli.get()
            return mysql_info
        else:
            raise RuntimeError("Can't get MySQL info from etcd cluster")

    @property
    def ring(self) -> MySQLRing:
        assert self._ring is not None
        return self._ring

    async def _rollback(self):
        match self.step:
            case ReshardStep.Init:
                logger.warning("Failed on Init step")
                return
            case ReshardStep.FetchNodeInfo:
                logger.warning("Failed on fetching MySQL node info")
                return
            case ReshardStep.DoReshard:
                logger.warning("Failed on actually doing resharging")
                return
            case ReshardStep.DeleteUnnecessary:
                logger.warning("Failed on deleting unnecessary data")
                return
            case ReshardStep.Done:
                logger.warning("Failed on Done step")
                return
            case _:
                return

    async def insert_frag(
        self,
        from_server: Server,
        table: TableName,
        frag: list[dict[str, Any]],
        keys: list[MySQLKeys],
    ):
        primary_key: str = keys[0].Column_name
        data_by_nodes: dict[str, list[dict[str, Any]]] = dict()
        server_from_address_format: dict[str, Server] = dict()
        for f in frag:
            _i: Server = self.ring.get_instance(f[primary_key])
            _nodename: str = _i.address_format()
            data_by_nodes.setdefault(_nodename, list())
            data_by_nodes[_nodename].append(f)
            server_from_address_format[_nodename] = _i

        for nodename, frag in data_by_nodes.items():
            server = server_from_address_format[nodename]
            assert server != from_server

            client = MySQLClient(
                host=server.host,
                port=server.port,
                auth=self._mysql_info,
            )
            await client.insert(
                table=table,
                datas=frag,
                insert_ignore=False,
            )
        return

    async def _do_reshard_node(self, mysql_node: Server) -> ReshardResult:
        client = MySQLClient(
            host=mysql_node.host,
            port=mysql_node.port,
            auth=self._mysql_info,
        )
        mds: list[Md] = self.ring.not_owner_points(mysql_node)
        for item in mds:
            item.node = mysql_node

        assert len(mds) == self.ring.vnodes * (
            len(self.ring) - 1
        )  # -1 is for this node

        logger.info(f"Not owned point: {len(mds)} points")

        @dataclass
        class MovedByReshard:
            elapsed: float
            rows: int

        MovedByTable = dict[TableName, list[MovedByReshard]]

        mbt: MovedByTable = MovedByTable()
        keys_by_table: dict[TableName, list[MySQLKeys]] = dict()
        for table in self.ring.table_names():
            keys: list[MySQLKeys] = await client.primary_keys_info(table)
            keys_by_table[table] = keys

        for index, md in enumerate(mds):
            # Not owner point must not be in this node
            assert md.node == mysql_node
            if index % 10 == 0:
                logger.info(f"Reshared complete: {index}/{len(mds)}")

            for table in self.ring.table_names():
                _ts = time.perf_counter()
                logger.info(
                    f'Move "{table}": {md.start_point[:self.LENGTH]}~{md.end_point[:self.LENGTH]} from {mysql_node.address_format()}'
                )
                _keys: list[MySQLKeys] = keys_by_table[table]
                not_owner = await client.consistent_hashing_select(
                    table=table,
                    start=SQLPoint(
                        point=md.start_point,
                        equal=md.start_equal,
                        greater=True,
                        less=False,
                    ),
                    end=SQLPoint(
                        point=md.end_point,
                        equal=md.end_equal,
                        greater=False,
                        less=True,
                    ),
                    _or=md._or,
                )
                _total_rows: int = 0
                async for frag in not_owner:
                    _total_rows += len(frag)
                    logger.info(f"Not owned data count: {len(frag)}")
                    await self.insert_frag(
                        from_server=mysql_node,
                        table=table,
                        frag=frag,
                        keys=_keys,
                    )

                _te = time.perf_counter()
                mbt.setdefault(table, list())
                _mbr = MovedByReshard(
                    elapsed=round(_te - _ts, BENCHMARK_ROUND_DECIMAL_PLACES),
                    rows=_total_rows,
                )
                mbt[table].append(_mbr)

        # Log summary
        result_by_table: dict[TableName, _Reshard] = dict()
        for table, moved in mbt.items():
            sum_rows: int = sum(item.rows for item in moved)
            sum_elapsed: float = sum(item.elapsed for item in moved)
            logger.info(
                f'Summary: moved by reshard from "{table}" on {mysql_node.address_format()}: {sum_rows} rows ({sum_elapsed}s)'
            )
            result_by_table[table] = _Reshard(
                moved_rows=sum_rows,
                elapsed=sum_elapsed,
            )

        return ReshardResult(
            mds=mds,
            result_by_table=result_by_table,
            tables=list(result_by_table.keys()),
        )

    async def do_delete_unnecessary(
        self,
        mds_node: dict[str, list[Md]],
        nodename_map: dict[str, Server],
    ):
        logger.info("Delete unnecessary data")

        @dataclass
        class DeletedRowsElapsed:
            rows: int
            elapsed: float

        DeleteTable = dict[TableName, DeletedRowsElapsed]

        for nodename, mds in mds_node.items():
            mysql_node = nodename_map[nodename]
            client = MySQLClient(
                host=mysql_node.host,
                port=mysql_node.port,
                auth=self._mysql_info,
            )
            _s = time.perf_counter()
            deleted_info_by_tables: DeleteTable = dict()

            for table in self.ring.table_names():
                _total_rows: int = 0
                _ts = time.perf_counter()
                for md in mds:
                    assert mysql_node == md.node
                    deleted_count: int = await client.consistent_hashing_delete(
                        table=table,
                        start=SQLPoint(
                            point=md.start_point,
                            equal=md.start_equal,
                            greater=True,
                            less=False,
                        ),
                        end=SQLPoint(
                            point=md.end_point,
                            equal=md.end_equal,
                            greater=False,
                            less=True,
                        ),
                        _or=md._or,
                    )
                    logger.info(
                        f'Delete unnecessary "{table}": {md.start_point[:self.LENGTH]}~{md.end_point[:self.LENGTH]} from {mysql_node.address_format()}: {deleted_count}rows'
                    )

                _te = time.perf_counter()
                dre = DeletedRowsElapsed(
                    rows=_total_rows,
                    elapsed=round(_te - _ts, BENCHMARK_ROUND_DECIMAL_PLACES),
                )
                deleted_info_by_tables[table] = dre

            _e = time.perf_counter()
            logger.info(
                f"Total time for deleting unnecessary data from {mysql_node.address_format()} is {round(_e-_s, BENCHMARK_ROUND_DECIMAL_PLACES)}s"
            )
            for table, dre in deleted_info_by_tables.items():
                logger.info(
                    f'Summary: Delete unnecessary {dre.rows}rows({dre.elapsed}s) from "{table}" on {mysql_node.address_format()}'
                )
        return

    @rollback
    async def do(
        self,
        delete_unnecessary: bool = False,
    ) -> NodeByResult:
        self.update_step(ReshardStep.Init)
        self.update_step(ReshardStep.FetchNodeInfo)
        mysql_data: MySQLEtcdData = await self.get_mysql_nodes()
        self._ring = MySQLRing(
            init_nodes=mysql_data.nodes,
            mysql_info=self._mysql_info,
        )
        mds_node: dict[str, list[Md]] = dict()
        nodename_map: dict[str, Server] = dict()
        node_by_result: NodeByResult = NodeByResult()
        self.update_step(ReshardStep.DoReshard)
        for mysql_node in mysql_data.nodes:
            self._reshard_server = mysql_node
            _s = time.perf_counter()
            logger.info(f"Reshard MySQL node: {mysql_node.address_format()}")
            reshard_result: ReshardResult = await self._do_reshard_node(mysql_node)
            _nodename = mysql_node.address_format()
            node_by_result.append(reshard_result)
            mds_node[_nodename] = reshard_result.mds
            nodename_map[_nodename] = mysql_node
            _e = time.perf_counter()
            logger.info(
                f"Time {round(_e-_s, BENCHMARK_ROUND_DECIMAL_PLACES)}s: Reshard {mysql_node.address_format()}"
            )
        logger.info("Done")

        if delete_unnecessary:
            self.update_step(ReshardStep.DeleteUnnecessary)
            await self.do_delete_unnecessary(
                mds_node=mds_node,
                nodename_map=nodename_map,
            )
            logger.info("Done deleting unnecessary data")

        self.update_step(ReshardStep.Done)
        return node_by_result
