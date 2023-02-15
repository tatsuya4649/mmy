import asyncio
import copy
import logging
import struct
import sys
import warnings

from aiomysql.connection import Connection, _open_connection, _open_unix_connection
from aiomysql.cursors import Cursor
from aiomysql.utils import _ConnectionContextManager, _lenenc_int
from pymysql import _auth
from pymysql.charset import charset_by_name
from pymysql.connections import MAX_PACKET_LEN, MysqlPacket
from pymysql.constants import CLIENT, CR
from pymysql.converters import decoders
from pymysql.err import InternalError, OperationalError
from rich import print

from ..const import SYSTEM_NAME
from .proxy import KeyData


def _init_client_logger() -> logging.Logger:
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        f"[{SYSTEM_NAME} Client] %(asctime)s %(levelname)s %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


client_logger = _init_client_logger()


def proxy_connect(
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
        #
        #        if "ssl" in kwargs:
        #            raise ValueError("Unsupport TLS/SSL")

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

    async def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.
        """
        buff = b""
        while True:
            try:
                packet_header = await self._read_bytes(4)
            except asyncio.CancelledError:
                self._close_on_cancel()
                raise

            btrl, btrh, packet_number = struct.unpack("<HBB", packet_header)
            bytes_to_read = btrl + (btrh << 16)
            client_logger.debug(
                f"Receive packet: {bytes_to_read}bytes, ID: {packet_number}"
            )

            # Outbound and inbound packets are numbered sequentialy, so
            # we increment in both write_packet and read_packet. The count
            # is reset at new COMMAND PHASE.
            if packet_number != self._next_seq_id:
                self.close()
                if packet_number == 0:
                    # MySQL 8.0 sends error packet with seqno==0 when shutdown
                    raise OperationalError(
                        CR.CR_SERVER_LOST,
                        "Lost connection to MySQL server during query",
                    )

                raise InternalError(
                    "Packet sequence number wrong - got %d expected %d"
                    % (packet_number, self._next_seq_id)
                )
            self._next_seq_id = (self._next_seq_id + 1) % 256

            try:
                recv_data = await self._read_bytes(bytes_to_read)
            except asyncio.CancelledError:
                self._close_on_cancel()
                raise

            buff += recv_data
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if bytes_to_read == 0xFFFFFF:
                continue
            if bytes_to_read < MAX_PACKET_LEN:
                break

        packet = packet_type(buff, self._encoding)
        if packet.is_error_packet():
            if self._result is not None and self._result.unbuffered_active is True:
                self._result.unbuffered_active = False
            packet.raise_for_error()
        return packet

    async def _request_authentication(self):
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        if int(self.server_version.split(".", 1)[0]) >= 5:
            self.client_flag |= CLIENT.MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        data_init = struct.pack(
            "<iIB23s", self.client_flag, MAX_PACKET_LEN, charset_id, b""
        )

        client_logger.debug(f"MySQL serevr: capabilities: {self.server_capabilities}")
        client_logger.debug(
            f"\tSupport SSL: {bool(self.server_capabilities & CLIENT.SSL)}"
        )
        if self._ssl_context and self.server_capabilities & CLIENT.SSL:
            client_logger.debug(f"TLS/SSL")
            self.write_packet(data_init)

            # Stop sending events to data_received
            self._writer.transport.pause_reading()

            # Get the raw socket from the transport
            raw_sock = self._writer.transport.get_extra_info("socket", default=None)
            if raw_sock is None:
                raise RuntimeError("Transport does not expose socket instance")

            raw_sock = raw_sock.dup()
            self._writer.transport.close()
            # MySQL expects TLS negotiation to happen in the middle of a
            # TCP connection not at start. Passing in a socket to
            # open_connection will cause it to negotiate TLS on an existing
            # connection not initiate a new one.
            self._reader, self._writer = await _open_connection(
                sock=raw_sock, ssl=self._ssl_context, server_hostname=self._host
            )

            self._secure = True

        if isinstance(self.user, str):
            _user = self.user.encode(self.encoding)
        else:
            _user = self.user

        data = data_init + _user + b"\0"

        authresp = b""

        auth_plugin = self._client_auth_plugin
        if not self._client_auth_plugin:
            # Contains the auth plugin from handshake
            auth_plugin = self._server_auth_plugin

        client_logger.debug(auth_plugin)
        if auth_plugin in ("", "mysql_native_password"):
            authresp = _auth.scramble_native_password(
                self._password.encode("latin1"), self.salt
            )
        elif auth_plugin == "caching_sha2_password":
            if self._password:
                authresp = _auth.scramble_caching_sha2(
                    self._password.encode("latin1"), self.salt
                )
            # Else: empty password
        elif auth_plugin == "sha256_password":
            if self._ssl_context and self.server_capabilities & CLIENT.SSL:
                authresp = self._password.encode("latin1") + b"\0"
            elif self._password:
                authresp = b"\1"  # request public key
            else:
                authresp = b"\0"  # empty password

        elif auth_plugin in ("", "mysql_clear_password"):
            authresp = self._password.encode("latin1") + b"\0"

        if self.server_capabilities & CLIENT.PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += _lenenc_int(len(authresp)) + authresp
        elif self.server_capabilities & CLIENT.SECURE_CONNECTION:
            data += struct.pack("B", len(authresp)) + authresp
        else:  # pragma: no cover
            # not testing against servers without secure auth (>=5.0)
            data += authresp + b"\0"

        if self._db and self.server_capabilities & CLIENT.CONNECT_WITH_DB:

            if isinstance(self._db, str):
                db = self._db.encode(self.encoding)
            else:
                db = self._db
            data += db + b"\0"

        if self.server_capabilities & CLIENT.PLUGIN_AUTH:
            name = auth_plugin
            if isinstance(name, str):
                name = name.encode("ascii")
            data += name + b"\0"

        self._auth_plugin_used = auth_plugin

        # Sends the server a few pieces of client info
        if self.server_capabilities & CLIENT.CONNECT_ATTRS:
            connect_attrs = b""
            for k, v in self._connect_attrs.items():
                k, v = k.encode("utf8"), v.encode("utf8")
                connect_attrs += struct.pack("B", len(k)) + k
                connect_attrs += struct.pack("B", len(v)) + v
            data += struct.pack("B", len(connect_attrs)) + connect_attrs

        client_logger.debug("Send to Handshake Response packet")
        self.write_packet(data)
        client_logger.debug("Wait for response from MySQL server")
        auth_packet = await self._read_packet()
        client_logger.debug(f"Received auth packet: {auth_packet}")

        # if authentication method isn't accepted the first byte
        # will have the octet 254
        if auth_packet.is_auth_switch_request():
            # https://dev.mysql.com/doc/internals/en/
            # connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            auth_packet.read_uint8()  # 0xfe packet identifier
            plugin_name = auth_packet.read_string()
            if (
                self.server_capabilities & CLIENT.PLUGIN_AUTH
                and plugin_name is not None
            ):
                await self._process_auth(plugin_name, auth_packet)
            else:
                # send legacy handshake
                data = (
                    _auth.scramble_old_password(
                        self._password.encode("latin1"), auth_packet.read_all()
                    )
                    + b"\0"
                )
                self.write_packet(data)
                await self._read_packet()
        elif auth_packet.is_extra_auth_data():
            if auth_plugin == "caching_sha2_password":
                await self.caching_sha2_password_auth(auth_packet)
            elif auth_plugin == "sha256_password":
                await self.sha256_password_auth(auth_packet)
            else:
                raise OperationalError(
                    "Received extra packet " "for auth method %r", auth_plugin
                )

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

    def __del__(self):
        if hasattr(self, "_writer") and self._writer:
            warnings.warn("Unclosed connection {!r}".format(self), ResourceWarning)
            self.close()
