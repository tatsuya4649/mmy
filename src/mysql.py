import abc
import asyncio
import ipaddress
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Literal, Type, TypeVar

import aiomysql
from aiomysql.cursors import DictCursor
from dacite import Config, from_dict
from loguru import logger
from rich import print


class _AbcGetAsyncIter(abc.ABC):
    @abc.abstractmethod
    def __init__(self, cursor, table, limit_once):
        """ """

    @abc.abstractmethod
    def __aiter__(self):
        """"""

    @abc.abstractmethod
    async def __anext__(self):
        """"""


GetAsyncIter = TypeVar("GetAsyncIter", bound=_AbcGetAsyncIter)


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
    Extra: _Extra


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


class _GetAsyncIter(_AbcGetAsyncIter):
    def __init__(
        self,
        cursor: DictCursor,
        table: str,
        limit_once: int,
    ):
        self._cursor = cursor
        self._table = table
        self._limit_once = limit_once
        self._i = 0
        self._rows_count: int | None = None

    def __aiter__(self):
        self._i = 0
        return self

    async def get_table_rows(
        self,
    ) -> int:
        async with self._cursor:
            logger.debug(
                self._cursor.mogrify(
                    f"SELECT COUNT(*) as rows_count FROM {self._table}",
                )
            )
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
            logger.debug(
                self._cursor.mogrify(
                    f"SELECT * FROM {self._table} LIMIT %(limit)s OFFSET %(offset)s",
                    {
                        "limit": self._limit_once,
                        "offset": self._i,
                    },
                )
            )
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
            _s = time.perf_counter()
            await func(*args, **kwargs)
            _e = time.perf_counter()
            _elapsed: float = round(_e - _s, 2)
            logger.info(f"Elapsed time({title}): {_elapsed}s")

        return _w

    return _log_elapsed_time


CONNECT_TIMEOUT = 10
INSERT_ONCE_LIMIT = 10000


class MySQLClientTooManyInsertAtOnce(ValueError):
    pass


class MySQLClient:
    IDENTIFY_PRIMARY: str = "PRIMARY"

    def __init__(
        self,
        host: ipaddress.IPv4Address | ipaddress.IPv6Address,
        port: int,
        user: str,
        password: str,
        db: str,
        connect_timeout: int = CONNECT_TIMEOUT,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = db
        self._connect_timeout = connect_timeout
        pass

    @property
    def _connect(self):
        return aiomysql.connect(
            host=str(self._host),
            port=self._port,
            user=self._user,
            password=self._password,
            db=self._db,
            cursorclass=DictCursor,
            charset="utf8mb4",
            connect_timeout=self._connect_timeout,
            autocommit=False,
        )

    async def get_table_rows(
        self,
        table: str,
    ) -> int:
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    f"SELECT COUNT(*) as rows_count FROM {table}",
                )
                res = await cursor.fetchone()
                assert res["rows_count"] is not None
                rows_count: int = int(res["rows_count"])
                return rows_count

    async def insert(
        self,
        table: str,
        datas: list[dict[str, Any]],
    ):
        if len(datas) == 0:
            return
        if len(datas) > INSERT_ONCE_LIMIT:
            raise MySQLClientTooManyInsertAtOnce

        first_item = datas[0]
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                columns: str = ", ".join(first_item.keys())
                values: str = ", ".join(
                    list(map(lambda x: "%(" + x + ")s", first_item.keys()))
                )
                await cursor.executemany(
                    f"INSERT INTO {table}({columns}) VALUES ({values})", datas
                )
            await connect.commit()

    async def select(
        self,
        table: str,
        limit_once: int = 10000,
        iter_type: Type[GetAsyncIter] | None = None,
    ):
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                _nclass: Type[_AbcGetAsyncIter] = _GetAsyncIter
                if iter_type is not None:
                    _nclass = iter_type
                _gai: _AbcGetAsyncIter = _nclass(
                    cursor=cursor,
                    limit_once=limit_once,
                    table=table,
                )
                async for frag in _gai:
                    yield frag

    async def consistent_hashing_select(
        self,
        table: str,
        start: str,
        end: str,
        limit_once: int = 10000,
    ):
        keys: list[MySQLKeys] = await self.primary_keys_info(table)

        class CHSIter(_AbcGetAsyncIter):
            def __init__(
                self,
                cursor: DictCursor,
                table: str,
                limit_once: int,
            ):
                self._cursor = cursor
                self._table = table
                self._limit_once = limit_once
                self._i = 0
                self._hash_fn = "MD5"
                self._rows_count: int | None = None

            def __aiter__(self):
                self._i = 0
                return self

            async def __anext__(self):
                primary_key: MySQLKeys = keys[0]
                primary_keyname: str = primary_key.Column_name
                if self._rows_count is None:
                    logger.debug(
                        self._cursor.mogrify(
                            f"SELECT COUNT(*) as rows_count FROM {self._table} WHERE {self._hash_fn}({primary_keyname})>%(start)s AND {self._hash_fn}({primary_keyname})<%(end)s",
                            {
                                "start": start,
                                "end": end,
                            },
                        )
                    )
                    await self._cursor.execute(
                        f"SELECT COUNT(*) as rows_count FROM {self._table} WHERE {self._hash_fn}({primary_keyname})>%(start)s AND {self._hash_fn}({primary_keyname})<%(end)s",
                        {
                            "start": start,
                            "end": end,
                        },
                    )
                    res = await self._cursor.fetchone()
                    assert res["rows_count"] is not None
                    self._rows_count = int(res["rows_count"])

                while self._i < self._rows_count:
                    logger.debug(
                        self._cursor.mogrify(
                            f"SELECT * FROM {self._table} WHERE {self._hash_fn}({primary_keyname})>%(start)s AND {self._hash_fn}({primary_keyname})<%(end)s LIMIT %(limit)s OFFSET %(offset)s",
                            {
                                "start": start,
                                "end": end,
                                "limit": self._limit_once,
                                "offset": self._i,
                            },
                        )
                    )
                    await self._cursor.execute(
                        f"SELECT * FROM {self._table} WHERE {self._hash_fn}({primary_keyname})>%(start)s AND {self._hash_fn}({primary_keyname})<%(end)s LIMIT %(limit)s OFFSET %(offset)s",
                        {
                            "start": start,
                            "end": end,
                            "limit": self._limit_once,
                            "offset": self._i,
                        },
                    )
                    _frag = await self._cursor.fetchall()
                    self._i += len(_frag)
                    return _frag

                raise StopAsyncIteration

        return self.select(
            table=table,
            limit_once=limit_once,
            iter_type=CHSIter,
        )

    async def consistent_hashing_delete(
        self,
        table: str,
        start: str,
        end: str,
    ):
        keys: list[MySQLKeys] = await self.primary_keys_info(table)
        primary_key: MySQLKeys = keys[0]
        primary_keyname: str = primary_key.Column_name
        _hash_fn: str = "MD5"
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                logger.debug(
                    cursor.mogrify(
                        f"SELECT COUNT(*) as rows_count FROM {table} WHERE {_hash_fn}({primary_keyname})>%(start)s AND {_hash_fn}({primary_keyname})<%(end)s",
                        {
                            "start": start,
                            "end": end,
                        },
                    )
                )
                await cursor.execute(
                    f"SELECT COUNT(*) as rows_count FROM {table} WHERE {_hash_fn}({primary_keyname})>%(start)s AND {_hash_fn}({primary_keyname})<%(end)s",
                    {
                        "start": start,
                        "end": end,
                    },
                )
                res = await cursor.fetchone()
                assert res["rows_count"] is not None
                _rows_count = int(res["rows_count"])
                logger.info(
                    f'Number of rows included in range("{start}"~"{end}") to be deleted by hash value of primary key by "{_hash_fn}": {_rows_count}'
                )
                if _rows_count == 0:
                    logger.info(
                        f'No deleted rows that is included in "{start}" to "{end}"'
                    )
                    return

                @log_elapsed_time("Delete rows")
                async def _actually_delete():
                    logger.debug(
                        cursor.mogrify(
                            f"DELETE FROM {table} WHERE {_hash_fn}({primary_keyname})>%(start)s AND {_hash_fn}({primary_keyname})<%(end)s",
                            {
                                "start": start,
                                "end": end,
                            },
                        )
                    )
                    affected_rows = await cursor.execute(
                        f"DELETE FROM {table} WHERE {_hash_fn}({primary_keyname})>%(start)s AND {_hash_fn}({primary_keyname})<%(end)s",
                        {
                            "start": start,
                            "end": end,
                        },
                    )
                    logger.info(f"Deleted rows: {affected_rows}")
                    assert _rows_count == affected_rows

                await _actually_delete()

            await connect.commit()
            logger.info(f"Do commit")
            return

    async def map_per_row(
        self,
        table: str,
        fn: Callable[[list[dict[str, Any]]], None | Awaitable[None]],
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

    async def columns_info(self, table: str) -> list[MySQLColumns]:
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f"SHOW COLUMNS FROM {table}")
                res = await cursor.fetchall()
                config = Config(
                    type_hooks={
                        _Extra: _Extra,
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

    async def primary_keys_info(
        self,
        table: str,
    ) -> list[MySQLKeys]:
        async with self._connect as connect:
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
    async def optimize_table(self, table: str):
        async with self._connect as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(f"OPTIMIZE TABLE {table}")
        return

    @log_elapsed_time("Ping")
    async def ping(self):
        async with self._connect as connect:
            await asyncio.wait_for(
                connect.ping(reconnect=False),
                timeout=1,
            )
