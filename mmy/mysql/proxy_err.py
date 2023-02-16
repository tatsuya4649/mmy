import abc
import re
import struct

from pymysql.err import InternalError, OperationalError, error_map

from .errcode import MySQLErrorCode


class _MmyError(abc.ABC):
    @abc.abstractmethod
    def errno(self) -> int:
        """
        Mmy error code
        """

    @abc.abstractmethod
    def mysql_error(self) -> MySQLErrorCode:
        """
        MySQL error code
        """


class MmyError(ValueError, _MmyError):
    def errno(self):
        return 0

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_UNKNOWN_ERROR


class MmyProtocolError(MmyError):
    def errno(self):
        return 1

    def mysql_error(self) -> MySQLErrorCode:
        return super().mysql_error()


class MmyTLSUnsupportError(MmyProtocolError):
    def errno(self):
        return 2

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_NOT_SUPPORTED_YET


class MmyUnmatchServerError(MmyProtocolError):
    def errno(self):
        return 3

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_UNKNOWN_ERROR


class MmyReadTimeout(MmyError):
    def errno(self):
        return 4

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_NET_READ_ERROR


class MmyWriteTimeout(MmyError):
    def errno(self):
        return 5

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_NET_WRITE_ERROR


class MmyLocalInfileUnsupportError(MmyProtocolError):
    def errno(self):
        return 6

    def mysql_error(self) -> MySQLErrorCode:
        return MySQLErrorCode.ER_NOT_SUPPORTED_YET


from typing import Type

mmy_errmap: dict[int, Type[MmyError]] = {
    0: MmyError,
    1: MmyProtocolError,
    2: MmyTLSUnsupportError,
    3: MmyUnmatchServerError,
    4: MmyReadTimeout,
    5: MmyWriteTimeout,
    6: MmyLocalInfileUnsupportError,
}


def raise_mysql_exception(data: bytes):
    errno = struct.unpack("<h", data[1:3])[0]
    errval = data[9:].decode("utf-8", "replace")
    errorclass = error_map.get(errno)
    if errorclass is None:
        errorclass = InternalError if errno < 1000 else OperationalError

    _mmy_err = re.match(r"^\[mmy\(([0-9]{,4})\)\]\s*(.*)", errval)
    if _mmy_err is not None:
        mmy_errno = int(_mmy_err.group(1))
        mmy_errval = _mmy_err.group(2)
        raise mmy_errmap[mmy_errno](mmy_errval)
    else:
        raise errorclass(errno, errval)
