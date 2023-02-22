from dataclasses import dataclass
from enum import Enum

from pymysql.err import OperationalError


class MYSQLSQLState(Enum):
    S_08S01 = "08S01"
    S_42000 = "42000"
    S_HY000 = "HY000"
    S_23000 = "23000"
    S_08004 = "08004"


@dataclass
class MySQLErrorCodeItem:
    error_code: int
    sql_state: MYSQLSQLState


class MySQLErrorCode(Enum):
    """

    This error code is only used in mmy.

    """

    ER_UNKNOWN_ERROR = MySQLErrorCodeItem(
        error_code=1105,
        sql_state=MYSQLSQLState.S_HY000,
    )
    ER_NEW_ABORTING_CONNECTION = MySQLErrorCodeItem(
        error_code=1184,
        sql_state=MYSQLSQLState.S_08S01,
    )
    ER_HANDSHAKE_ERROR = MySQLErrorCodeItem(
        error_code=1043,
        sql_state=MYSQLSQLState.S_08S01,
    )
    ER_NET_READ_ERROR = MySQLErrorCodeItem(
        error_code=1158,
        sql_state=MYSQLSQLState.S_08S01,
    )
    ER_NET_WRITE_ERROR = MySQLErrorCodeItem(
        error_code=1160,
        sql_state=MYSQLSQLState.S_08S01,
    )
    ER_NOT_SUPPORTED_YET = MySQLErrorCodeItem(
        error_code=1235,
        sql_state=MYSQLSQLState.S_42000,
    )
    ER_DUP_ENTRY = MySQLErrorCodeItem(
        error_code=1062,
        sql_state=MYSQLSQLState.S_23000,
    )
    ER_CON_COUNT_ERROR = MySQLErrorCodeItem(
        error_code=1040,
        sql_state=MYSQLSQLState.S_08004,
    )
    ER_OUT_OF_RESOURCES = MySQLErrorCodeItem(
        error_code=1041,
        sql_state=MYSQLSQLState.S_HY000,
    )
    ER_TOO_MANY_USER_CONNECTIONS = MySQLErrorCodeItem(
        error_code=1203,
        sql_state=MYSQLSQLState.S_42000,
    )
    ER_CR_CONNECTION_ERROR = MySQLErrorCodeItem(
        error_code=2002,
        sql_state=MYSQLSQLState.S_HY000,
    )


def is_connection_error(e: OperationalError) -> bool:
    err_code = e.args[0]
    if (
        err_code is MySQLErrorCode.ER_CON_COUNT_ERROR
        or err_code is MySQLErrorCode.ER_OUT_OF_RESOURCES
        or err_code is MySQLErrorCode.ER_TOO_MANY_USER_CONNECTIONS
        or err_code is MySQLErrorCode.ER_CR_CONNECTION_ERROR,
    ):
        return True
    else:
        return False


import asyncio
import logging
from typing import Any, AsyncGenerator, Awaitable, Callable, Coroutine


class MySQLRetryConnectionError(RuntimeError):
    pass


async def co_retry(
    co: Callable[[Any], Awaitable[Any]],
    retry_count: int,
    retry_interval_secs: int,
    args: tuple[Any],
    kwargs: dict[str, Any],
):
    logger = logging.getLogger(__name__)
    for i in range(retry_count):
        try:
            return await co(*args, **kwargs)
        except OperationalError as e:
            err_msg = e.args[1]
            if is_connection_error(e):
                logger.error(f"{i} try: Connect error with MySQL: {err_msg}")
                logger.info(f"{i} try: Sleep and retry")
                await asyncio.sleep(retry_interval_secs)
                continue
            else:
                raise e
    else:
        raise MySQLRetryConnectionError(f"{retry_count} times error")


async def asyncgen_retry(
    asyncgen: Callable[[Any], AsyncGenerator[Any, Any]],
    retry_count: int,
    retry_interval_secs: int,
    args: tuple[Any],
    kwargs: dict[str, Any],
):
    logger = logging.getLogger(__name__)
    for i in range(retry_count):
        try:
            _gen: AsyncGenerator[Any, Any] = asyncgen(*args, **kwargs)
            yield await _gen.__anext__()
        except StopAsyncIteration:
            return
        except OperationalError as e:
            err_msg = e.args[1]
            if is_connection_error(e):
                logger.error(f"{i} try: Connect error with MySQL: {err_msg}")
                logger.info(f"{i} try: Sleep and retry")
                await asyncio.sleep(retry_interval_secs)
                continue
            else:
                raise e
    else:
        raise MySQLRetryConnectionError(f"{retry_count} times error")
