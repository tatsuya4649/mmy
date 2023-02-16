from dataclasses import dataclass
from enum import Enum


class MYSQLSQLState(Enum):
    S_08S01 = "08S01"
    S_42000 = "42000"
    S_HY000 = "HY000"


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
