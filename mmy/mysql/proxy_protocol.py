from pymysql.connections import MysqlPacket

from .proxy_err import raise_mysql_exception


class MmyPacket(MysqlPacket):
    def raise_for_error(self):
        self.rewind()
        self.advance(1)  # field_count == error (we already know that)
        raise_mysql_exception(self._data)
