from pymysql.connections import MysqlPacket

from .client_log import client_logger
from .proxy_err import raise_mysql_exception


class MmyPacket(MysqlPacket):
    def raise_for_error(self):
        self.rewind()
        self.advance(1)  # field_count == error (we already know that)
        errno = self.read_uint16()
        client_logger.debug("errno = %d", errno)
        raise_mysql_exception(self._data)
