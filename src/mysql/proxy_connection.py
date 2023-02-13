import asyncio

from aiomysql.connection import Connection, _open_connection, _open_unix_connection
from aiomysql.cursors import Cursor
from aiomysql.utils import _ConnectionContextManager
from pymysql.constants import CR
from pymysql.converters import decoders
from pymysql.err import OperationalError

from .proxy import KeyData


def connect(
    host="localhost",
    user=None,
    password="",
    db=None,
    port=3306,
    unix_socket=None,
    charset="",
    sql_mode=None,
    read_default_file=None,
    conv=decoders,
    use_unicode=None,
    client_flag=0,
    cursorclass=Cursor,
    init_command=None,
    connect_timeout=None,
    read_default_group=None,
    autocommit=False,
    echo=False,
    local_infile=False,
    loop=None,
    ssl=None,
    auth_plugin="",
    program_name="",
    server_public_key=None,
    **kwargs,
):
    """See connections.Connection.__init__() for information about
    defaults."""
    coro = _connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        unix_socket=unix_socket,
        charset=charset,
        sql_mode=sql_mode,
        read_default_file=read_default_file,
        conv=conv,
        use_unicode=use_unicode,
        client_flag=client_flag,
        cursorclass=cursorclass,
        init_command=init_command,
        connect_timeout=connect_timeout,
        read_default_group=read_default_group,
        autocommit=autocommit,
        echo=echo,
        local_infile=local_infile,
        loop=loop,
        ssl=ssl,
        auth_plugin=auth_plugin,
        program_name=program_name,
        **kwargs,
    )
    return _ConnectionContextManager(coro)


async def _connect(*args, **kwargs):
    conn = ProxyConnection(*args, **kwargs)
    await conn._connect()
    return conn


class ProxyConnection(Connection):
    def __init__(
        self,
        key: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._key = key
        self._bkey: bytes = key.encode("utf-8")
        self._key_data = KeyData(
            length=len(self._bkey),
            data=self._bkey,
        )

    async def _write_key_data(self):
        _b: bytes = self._key_data.to_bytes()
        self._writer.write(_b)
        await self._writer.drain()

    async def _connect(self):
        try:
            if self._unix_socket:
                self._reader, self._writer = await asyncio.wait_for(
                    _open_unix_connection(self._unix_socket),
                    timeout=self.connect_timeout,
                )
                self.host_info = "Localhost via UNIX socket: " + self._unix_socket
                self._secure = True
            else:
                self._reader, self._writer = await asyncio.wait_for(
                    _open_connection(self._host, self._port),
                    timeout=self.connect_timeout,
                )
                self._set_keep_alive()
                self._set_nodelay(True)
                self.host_info = "socket %s:%d" % (self._host, self._port)

            await self._write_key_data()

            self._next_seq_id = 0

            await self._get_server_information()
            await self._request_authentication()

            self.connected_time = self._loop.time()

            if self.sql_mode is not None:
                await self.query("SET sql_mode=%s" % (self.sql_mode,))

            if self.init_command is not None:
                await self.query(self.init_command)
                await self.commit()

            if self.autocommit_mode is not None:
                await self.autocommit(self.autocommit_mode)
        except Exception as e:
            if self._writer:
                self._writer.transport.close()
            self._reader = None
            self._writer = None

            # As of 3.11, asyncio.TimeoutError is a deprecated alias of
            # OSError. For consistency, we're also considering this an
            # OperationalError on earlier python versions.
            if isinstance(e, (IOError, OSError, asyncio.TimeoutError)):
                raise OperationalError(
                    CR.CR_CONN_HOST_ERROR,
                    "Can't connect to MySQL server on %r" % self._host,
                ) from e

            # If e is neither IOError nor OSError, it's a bug.
            # Raising AssertionError would hide the original error, so we just
            # reraise it.
            raise
