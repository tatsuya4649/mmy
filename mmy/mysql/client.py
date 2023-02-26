import abc
import asyncio
import ipaddress
import logging
import time
from copy import copy
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Literal,
    NewType,
    Type,
    TypeAlias,
    TypeVar,
)

import aiomysql
from aiomysql.cursors import DictCursor
from dacite import Config, from_dict
from rich import print

from .errcode import asyncgen_retry, co_retry
from .sql import SQLPoint


class _AbcGetAsyncIter(abc.ABC):
    @abc.abstractmethod
    def __init__(self, table, limit_once, **kwargs):
        """ """

    @abc.abstractmethod
    def __aiter__(self):
        """"""

    @abc.abstractmethod
    async def __anext__(self):
        """"""


GetAsyncIter = TypeVar("GetAsyncIter", bound=_AbcGetAsyncIter)


MySQLFetchOne: TypeAlias = dict[str, Any]
MySQLFetchAll: TypeAlias = list[MySQLFetchOne]


class YesNo(Enum):
    YES = "YES"
    NO = "NO"


class _Extra(Enum):
    EMPTY = ""
    AUTO_INCREMENT = "auto_increment"
    ON_UPDATE_CURRENT_TIMESTAMP = "on update CURRENT_TIMESTAMP"


@dataclass
class MySQLColumns:
    Field: str
    Type: str
    Collation: Literal["A", "D", "NULL"] | None
    Null: YesNo
    Key: str
    Default: Any | None
    Extra: str


@dataclass
class MySQLKeys:
    Table: str
    Non_unique: bool
    Key_name: str
    Seq_in_index: int  # Start at 1
    Column_name: str
    Collation: Literal["A", "D", "NULL"]
    Cardinality: int
    Sub_part: int | None
    Packed: Literal[0, 1, "DEFAULT"] | None
    Null: str | None
    Index_type: Literal["BTREE", "FULLTEXT", "HASH", "RTREE"]
    Comment: str
    Index_comment: str
    Visible: YesNo
    Expression: str | None


class MySQLInsertDuplicateBehavior(Enum):
    Raise = auto()
    DeleteAutoIncrement = auto()


class _GetAsyncIter(_AbcGetAsyncIter):
    def __init__(
        self,
        cursor: DictCursor,
        table: str,
        limit_once: int,
        **kwargs,
    ):
        self._cursor = cursor
        self._table = table
        self._limit_once = limit_once
        self._i = 0
        self._rows_count: int | None = None
        self.logger = logging.getLogger(__name__)

    def __aiter__(self):
        self._i = 0
        return self

    async def get_table_rows(
        self,
    ) -> int:
        async with self._cursor:
            await self._cursor.execute(
                f"SELECT COUNT(*) as rows_count FROM {self._table}",
            )
            res = await self._cursor.fetchone()
            assert res["rows_count"] is not None
            rows_count: int = int(res["rows_count"])
            return rows_count

    async def __anext__(self):
        if self._rows_count is None:
            self._rows_count = await self.get_table_rows()

        while self._i < self._rows_count:
            await self._cursor.execute(
                f"SELECT * FROM {self._table} LIMIT %(limit)s OFFSET %(offset)s",
                {
                    "limit": self._limit_once,
                    "offset": self._i,
                },
            )
            _frag = await self._cursor.fetchall()
            self._i += len(_frag)
            return _frag

        raise StopAsyncIteration


def log_elapsed_time(title: str):
    def _log_elapsed_time(func):
        async def _w(*args, **kwargs):
            self: MySQLClient = args[0]
            assert isinstance(self, MySQLClient) is True
            _s = time.perf_counter()
            await func(*args, **kwargs)
            _e = time.perf_counter()
            _elapsed: float = round(_e - _s, 4)
            self.logger.debug(f"Elapsed time({title}): {_elapsed}s: {func.__name__}")

        return _w

    return _log_elapsed_time


CONNECT_TIMEOUT = 10
INSERT_ONCE_LIMIT = 10000


class MySQLClientTooManyInsertAtOnce(ValueError):
    pass


@dataclass
class MySQLAuthInfo:
    user: str
    password: str


TableName = NewType("TableName", str)
_MySQLAuthInfoByTable: TypeAlias = dict[TableName, MySQLAuthInfo]


@dataclass
class MySQLAuthInfoByTable:
    default_userinfo: MySQLAuthInfo
    tables: _MySQLAuthInfoByTable


@dataclass
class MySQLTableInfo(MySQLAuthInfo):
    name: str


@dataclass
class MmyMySQLInfo:
    db: str
    tables: list[MySQLTableInfo]
    default_userinfo: MySQLAuthInfo

    def auth_by_tables(self) -> MySQLAuthInfoByTable:
        _ts: _MySQLAuthInfoByTable = dict()
        for t in self.tables:
            _ts[TableName(t.name)] = MySQLAuthInfo(
                user=t.user,
                password=t.password,
            )

        return MySQLAuthInfoByTable(
            default_userinfo=self.default_userinfo,
            tables=_ts,
        )


def _get_retry_count() -> int | float:
    return RETRY_COUNT


def _get_retry_interval_secs() -> int | float:
    return RETRY_INTERVAL_SECS


def multiple_retry_mysql_when_connection_error(cor):
    async def _wrap_rollback(*args, **kwargs):
        client = args[0]
        assert isinstance(client, MySQLClient)

        _retry_count = _get_retry_count()
        _retry_interval_count = _get_retry_interval_secs()
        return await co_retry(
            co=cor,
            retry_count=_retry_count,
            retry_interval_secs=_retry_interval_count,
            args=args,
            kwargs=kwargs,
        )

    return _wrap_rollback


def asyncgen_multiple_retry_mysql_when_connection_error(cor):
    async def _wrap_rollback(*args, **kwargs):
        client = args[0]
        assert isinstance(client, MySQLClient)

        _retry_count = _get_retry_count()
        _retry_interval_count = _get_retry_interval_secs()
        _c = asyncgen_retry(
            asyncgen=cor,
            retry_count=_retry_count,
            retry_interval_secs=_retry_interval_count,
            args=args,
            kwargs=kwargs,
        )
        try:
            yield await _c.__anext__()
        except StopAsyncIteration:
            return

    return _wrap_rollback


RETRY_COUNT: int = 3
RETRY_INTERVAL_SECS: int = 10
LENGTH: int = 4


class MySQLClient:
    IDENTIFY_PRIMARY: str = "PRIMARY"

    def __init__(
        self,
        host: ipaddress.IPv4Address | ipaddress.IPv6Address,
        port: int,
        auth: MmyMySQLInfo,
        connect_timeout: int = CONNECT_TIMEOUT,
        debug: bool = True,
    ):
        self._host = host
        self._port = port
        self._db = auth.db
        self._auth: MySQLAuthInfoByTable = auth.auth_by_tables()
        self._connect_timeout = connect_timeout
        self.logger = logging.getLogger(__name__)
        self._debug = debug

    def _default_connect(self):
        return aiomysql.connect(
            host=str(self._host),
            port=self._port,
            user=self._auth.default_userinfo.user,
            password=self._auth.default_userinfo.password,
            db=self._db,
            cursorclass=DictCursor,
            charset="utf8mb4",
            connect_timeout=self._connect_timeout,
            autocommit=False,
        )

    def _connect(self, table: TableName):
        return aiomysql.connect(
            host=str(self._host),
            port=self._port,
            user=self._auth.tables[table].user,
            password=self._auth.tables[table].password,
            db=self._db,
            cursorclass=DictCursor,
            charset="utf8mb4",
            connect_timeout=self._connect_timeout,
            autocommit=False,
        )

    async def get_table_size_in_kb(self, table: TableName) -> int:
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    f"SELECT round(((data_length + index_length) / 1024), 2) as MB \
                        FROM information_schema.TABLES \
                            WHERE table_schema=%(table_schema)s AND table_name=%(table_name)s",
                    {
                        "table_schema": self._db,
                        "table_name": str(table),
                    },
                )
                res = await cursor.fetchone()
                assert res["MB"] is not None
                _mb: int = int(res["MB"])
                return _mb

    async def get_table_rows(
        self,
        table: TableName,
    ) -> int:
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    f"SELECT COUNT(*) as rows_count FROM {table}",
                )
                res = await cursor.fetchone()
                assert res["rows_count"] is not None
                rows_count: int = int(res["rows_count"])
                return rows_count

    @multiple_retry_mysql_when_connection_error
    async def insert(
        self,
        table: TableName,
        datas: list[dict[str, Any]],
        insert_ignore: bool,
    ) -> int:
        if len(datas) == 0:
            return 0
        if len(datas) > INSERT_ONCE_LIMIT:
            raise MySQLClientTooManyInsertAtOnce

        first_item = datas[0]
        affected: int = 0
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                columns: str = ", ".join(first_item.keys())
                values: str = ", ".join(list(map(lambda _: "%s", first_item.keys())))
                if insert_ignore:
                    update_columns: str = ", ".join(
                        list(map(lambda x: f"{x}=_f.{x}", first_item.keys()))
                    )
                    affected = await cursor.executemany(
                        f"INSERT INTO {table}({columns}) VALUES ({values}) as _f ON DUPLICATE KEY UPDATE {update_columns}",
                        [tuple(i.values()) for i in datas],
                    )
                else:
                    affected = await cursor.executemany(
                        f"INSERT INTO {table}({columns}) VALUES ({values})",
                        [tuple(i.values()) for i in datas],
                    )
            await connect.commit()

        return affected

    @asyncgen_multiple_retry_mysql_when_connection_error
    async def select(
        self,
        table: TableName,
        limit_once: int = 10000,
        iter_type: Type[GetAsyncIter] | None = None,
        **kwargs,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        _nclass: Type[_AbcGetAsyncIter] = _GetAsyncIter
        if iter_type is not None:
            _nclass = iter_type
        _gai: _AbcGetAsyncIter = _nclass(
            limit_once=limit_once,
            table=table,
            debug=self._debug,
            client=self,
            **kwargs,
        )
        async for frag in _gai:
            yield frag

    async def consistent_hashing_select(
        self,
        table: TableName,
        start: SQLPoint,
        end: SQLPoint,
        _or: bool,
        limit_once: int = 10000,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        keys: list[MySQLKeys] = await self.primary_keys_info(table)

        return self.select(
            table=table,
            limit_once=limit_once,
            iter_type=CHSIter,
            primary_key=keys[0],
            start=start,
            end=end,
            _or=_or,
        )

    @multiple_retry_mysql_when_connection_error
    async def consistent_hashing_delete(
        self,
        table: TableName,
        start: SQLPoint,
        end: SQLPoint,
        _or: bool,
        hash_fn: str | None = "MD5",
    ) -> int:
        keys: list[MySQLKeys] = await self.primary_keys_info(table)
        primary_key: MySQLKeys = keys[0]
        primary_keyname: str = primary_key.Column_name
        and_or = "OR" if _or else "AND"

        def _hash_fn(primary_keyname: str):
            if hash_fn is None:
                return primary_keyname
            else:
                return f"{hash_fn}({primary_keyname})"

        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    f"SELECT COUNT(*) as rows_count FROM {table} WHERE {_hash_fn(primary_keyname)}{start.greater_less}%(start)s {and_or} {_hash_fn(primary_keyname)}{end.greater_less}%(end)s",
                    {
                        "start": start.point,
                        "end": end.point,
                    },
                )
                res = await cursor.fetchone()
                assert res["rows_count"] is not None
                _rows_count = int(res["rows_count"])
                if _rows_count == 0:
                    self.logger.debug(
                        f'No deleted rows "{start.point[:LENGTH]}" to "{end.point[:LENGTH]}"'
                    )
                    return 0

                async def _actually_delete():
                    affected_rows = await cursor.execute(
                        f"DELETE FROM {table} WHERE {_hash_fn(primary_keyname)}{start.greater_less}%(start)s {and_or} {_hash_fn(primary_keyname)}{end.greater_less}%(end)s",
                        {
                            "start": start.point,
                            "end": end.point,
                        },
                    )
                    self.logger.info(f"Deleted rows: {affected_rows}")
                    assert _rows_count == affected_rows

                await _actually_delete()

            await connect.commit()
            return _rows_count

    async def map_per_row(
        self,
        table: TableName,
        fn: Callable[[MySQLFetchAll], None | Awaitable[None]],
        limit_once: int = 10000,
    ) -> None:
        import inspect

        async for fram in self.select(table, limit_once):
            res: Awaitable[None] | None = fn(fram)
            if inspect.isawaitable(res):
                await res

            return

    def exclude_auto_increment_columns(
        self,
        columns: list[MySQLColumns],
    ) -> list[MySQLColumns]:
        for index, column in enumerate(columns):
            if column.Extra is _Extra.AUTO_INCREMENT:
                columns.pop(index)

        return columns

    def exclude_data_by_columns(
        self,
        columns: list[MySQLColumns],
        datas: MySQLFetchAll,
    ):
        _c: list[str] = [i.Field for i in columns]

        def _check_key(x: dict[str, Any]) -> dict[str, Any]:
            _x: dict[str, Any] = copy(x)
            for key in x.keys():
                if key not in _c:
                    _x.pop(key)
            return _x

        return list(map(_check_key, datas))

    @multiple_retry_mysql_when_connection_error
    async def columns_info(self, table: TableName) -> list[MySQLColumns]:
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f"SHOW COLUMNS FROM {table}")
                res = await cursor.fetchall()
                config = Config(
                    type_hooks={
                        YesNo: YesNo,
                    }
                )

                columns: list[MySQLColumns] = list()
                for item in res:
                    column = from_dict(
                        data_class=MySQLColumns,
                        data=item,
                        config=config,
                    )
                    columns.append(column)

                return columns

    @multiple_retry_mysql_when_connection_error
    async def primary_keys_info(
        self,
        table: TableName,
    ) -> list[MySQLKeys]:
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f'SHOW KEYS FROM {table} WHERE Key_name="PRIMARY"')
                res = await cursor.fetchall()
                config = Config(
                    type_hooks={
                        bool: bool,
                        YesNo: YesNo,
                    }
                )

                keys: list[MySQLKeys] = list()
                for item in res:
                    key = from_dict(
                        data_class=MySQLKeys,
                        data=item,
                        config=config,
                    )
                    assert key.Key_name == self.IDENTIFY_PRIMARY
                    keys.append(key)

                return keys

    @log_elapsed_time("Optimize table")
    @multiple_retry_mysql_when_connection_error
    async def optimize_table(self, table: TableName):
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f"OPTIMIZE TABLE {table}")
        return

    @log_elapsed_time("Ping")
    async def ping(self):
        self.logger.debug(f"PING with {self._host}:{self._port}")
        async with self._default_connect() as connect:
            await asyncio.wait_for(
                connect.ping(reconnect=False),
                timeout=10,
            )

    @log_elapsed_time("Delete all table data")
    @multiple_retry_mysql_when_connection_error
    async def delete_all_table_data(self, table: TableName):
        async with self._connect(table) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f"DELETE FROM {table}")
            await connect.commit()
        return


class CHSIter(_AbcGetAsyncIter):
    def __init__(
        self,
        table: TableName,
        limit_once: int,
        debug: bool,
        client: MySQLClient,
        primary_key: MySQLKeys,
        _or: str,
        start: SQLPoint,
        end: SQLPoint,
        hash_fn: str | None = "MD5",
    ):
        self._table = table
        self._limit_once = limit_once
        self._i = 0
        self._hash_fn = hash_fn
        self._rows_count: int | None = None
        self.logger = logging.getLogger(__name__)
        self._debug: bool = debug
        self._client = client
        self._primary_key = primary_key
        self._or = _or
        self._start = start
        self._end = end

    def __aiter__(self):
        self._i = 0
        return self

    async def __anext__(self):
        def _hash_fn_exp(primary_keyname: str):
            if self._hash_fn is None:
                return primary_keyname
            else:
                return f"{self._hash_fn}({primary_keyname})"

        primary_keyname: str = self._primary_key.Column_name
        and_or = "OR" if self._or else "AND"
        if self._rows_count is None:
            async with self._client._connect(self._table) as connect:
                cursor = await connect.cursor()
                async with cursor:
                    await cursor.execute(
                        f"SELECT COUNT(*) as rows_count FROM {self._table} WHERE {_hash_fn_exp(primary_keyname)}{self._start.greater_less}%(start)s {and_or} {_hash_fn_exp(primary_keyname)}{self._end.greater_less}%(end)s",
                        {
                            "start": self._start.point,
                            "end": self._end.point,
                        },
                    )
                    res = await cursor.fetchone()
                    assert res["rows_count"] is not None
                    self._rows_count = int(res["rows_count"])

        while self._i < self._rows_count:
            async with self._client._connect(self._table) as connect:
                cursor = await connect.cursor()
                async with cursor:
                    await cursor.execute(
                        f"SELECT * FROM {self._table} WHERE {_hash_fn_exp(primary_keyname)}{self._start.greater_less}%(start)s {and_or} {_hash_fn_exp(primary_keyname)}{self._end.greater_less}%(end)s LIMIT %(limit)s OFFSET %(offset)s",
                        {
                            "start": self._start.point,
                            "end": self._end.point,
                            "limit": self._limit_once,
                            "offset": self._i,
                        },
                    )
                    _frag = await cursor.fetchall()
                    self._i += len(_frag)
                    return _frag

        raise StopAsyncIteration
