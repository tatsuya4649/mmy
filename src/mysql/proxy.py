import asyncio
import struct
from dataclasses import dataclass
from typing import Any

from loguru import logger
from rich import print

from ..server import _Server, address_from_server
from .hosts import MySQLHosts

PROTOCOL_ID: bytes = b"_MMY"

KEY_DATA_TIMEOUT: int = 3
KEY_DATA_HEADER_LENGTH: int = 4 + 4 + 1


@dataclass
class KeyData:
    length: int  # 4bytes
    data: bytes
    protocol_id: bytes = PROTOCOL_ID  # 4bytes
    version: int = 0  # 1byte

    def to_bytes(self) -> bytes:
        return struct.pack(
            f"<4sii{self.length}s",
            self.protocol_id,
            self.version,
            self.length,
            self.data,
        )


async def select_mysql_host(mysql_hosts: MySQLHosts, key_data: KeyData) -> _Server:
    import ipaddress

    return _Server(
        host=ipaddress.ip_address("127.0.0.1"),
        port=10001,
    )


from enum import Enum, auto


class ProxyInvalidRequest(RuntimeError):
    pass


class MySQLConnectionLifecycle(Enum):
    Connetion = auto()
    Command = auto()


class MySQLProxyDirection(Enum):
    CtoS = auto()
    StoC = auto()


class MySQLQueryPhase(Enum):
    Metadata = auto()
    Rows = auto()


class MySQLCommandType(Enum):
    COM_SLEEP = 0x00
    COM_QUIT = 0x01
    COM_INIT_DB = 0x02
    COM_QUERY = 0x03
    COM_FIELD_LIST = 0x04
    COM_CREATE_DB = 0x05
    COM_DROP_DB = 0x06
    COM_REFRESH = 0x07
    COM_STATISTICS = 0x08
    COM_PROCESS_INFO = 0x0A
    COM_CONNECT = 0x0B
    COM_PROCESS_KILL = 0x0C
    COM_DEBUG = 0x0D
    COM_PING = 0x0E
    COM_TIME = 0x0F
    COM_DELAYES_INSERT = 0x10
    COM_CHANGE_USER = 0x11
    COM_BINLOG_DUMP = 0x12
    COM_TABLE_DUMP = 0x13
    COM_CONNECT_OUT = 0x14
    COM_REGISTER_SLAVE = 0x15
    COM_STMT_PREPARE = 0x16
    COM_STMT_EXECUTE = 0x17
    COM_STMT_SEND_LONG_DATA = 0x18
    COM_STMT_CLOSE = 0x19
    COM_STMT_RESET = 0x1A
    COM_SET_OPTION = 0x1B
    COM_STMT_FETCH = 0x1C
    COM_DAEMON = 0x1D
    COM_BINLOG_DUMP_GTID = 0x1E
    COM_RESET_CONNECTION = 0x1F
    NONE = -1


@dataclass
class MySQLPacket:
    payload_length: int
    sequence_id: int
    header: bytes
    payload: bytes | None
    auth_more: bool  # This is Protocol::AuthMoreData: packet
    ok: bool  # This is OK_Packet
    error: bool  # This is ERR_Packet
    local_infile: bool
    empty_packet: bool
    phase: MySQLConnectionLifecycle
    first_payload: bytes | None
    command_type: MySQLCommandType | None

    def pack_to_bytes(self):
        return self.header + self.payload

    def payload_offset(self, offset) -> bytes:
        """ """
        if self.payload is None:
            raise ValueError
        return self.payload[offset:]


class EOF(ValueError):
    pass


async def received_packet(
    reader: asyncio.StreamReader,
    phase: MySQLConnectionLifecycle,
    direction: MySQLProxyDirection,
) -> MySQLPacket:

    logger.debug("Waiting for receiving data")
    _header: bytes = await reader.read(4)
    if reader.at_eof():
        raise EOF

    logger.debug("Received header data(4bytes)")
    _modified_header: bytes = _header[:3] + b"\x00" + _header[3:]
    header: tuple[Any, ...] = struct.unpack("<iB", _modified_header)
    payload_length: int = header[0]
    sequence_id: int = header[1]

    _payload: bytes = await reader.read(payload_length)
    logger.debug(f"Received payload data({payload_length}bytes)")
    _first_payload: bytes | None = None
    _fp_int: int | None = None
    if payload_length > 0:
        _first_payload = _payload[:1]
        if phase is MySQLConnectionLifecycle.Command:
            _p: tuple[Any, ...] = struct.unpack("<B", _first_payload)
            _fp_int = _p[0]

    _command_type: MySQLCommandType = MySQLCommandType.NONE
    if (
        direction is MySQLProxyDirection.CtoS
        and payload_length > 0
        and phase is MySQLConnectionLifecycle.Command
    ):
        _command_type = MySQLCommandType(_fp_int)

    packet: MySQLPacket = MySQLPacket(
        payload_length=payload_length,
        sequence_id=sequence_id,
        header=_header,
        payload=_payload,
        empty_packet=payload_length == 0,
        auth_more=_first_payload == b"\x01",
        ok=_first_payload == b"\x00" or _payload[:1] == b"\xfe",
        error=_first_payload == b"\xff",
        local_infile=_first_payload == b"\xfb",
        phase=phase,
        first_payload=_first_payload,
        command_type=_command_type,
    )
    return packet


async def send_package(writer: asyncio.StreamWriter, packet: MySQLPacket) -> None:
    writer.write(packet.pack_to_bytes())
    await writer.drain()


async def _from_to(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    phase: MySQLConnectionLifecycle,
    direction: MySQLProxyDirection,
):
    packet: MySQLPacket = await received_packet(reader, phase, direction)
    if phase is MySQLConnectionLifecycle.Connetion:
        logger.debug(f"[Connection] Received package: {packet}")
    else:
        logger.debug(f"[Command] Received package: {packet}")

    logger.debug(f"Header: %a" % (packet.header))
    logger.debug(f"Payload length: %a" % (packet.payload_length))
    logger.debug(f"Payload: %a" % (packet.payload))
    logger.debug(f"Sequence ID: {packet.sequence_id}")
    logger.debug(f"OK: {packet.ok}")
    logger.debug(f"ERR: {packet.error}")
    logger.debug(f"Phase: {packet.phase}")
    logger.debug(f"First payload: %a" % (packet.first_payload))
    logger.debug(f"Command Type: {packet.command_type}")

    await send_package(
        writer=writer,
        packet=packet,
    )
    return packet


def log_debug_direction(d: MySQLProxyDirection):
    def _log_debug_direction(asyn):
        async def _w(*args, **kwargs):
            if d is MySQLProxyDirection.CtoS:
                logger.debug(f"Server <== Client")
            else:
                logger.debug(f"Server ==> Client")

            return await asyn(*args, **kwargs)

        return _w

    return _log_debug_direction


@log_debug_direction(MySQLProxyDirection.StoC)
async def from_server_to_client(
    server_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    phase: MySQLConnectionLifecycle,
) -> MySQLPacket:
    return await _from_to(server_reader, client_writer, phase, MySQLProxyDirection.StoC)


@log_debug_direction(MySQLProxyDirection.CtoS)
async def from_client_to_server(
    client_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
    phase: MySQLConnectionLifecycle,
) -> MySQLPacket:
    return await _from_to(client_reader, server_writer, phase, MySQLProxyDirection.CtoS)


def is_auth_more_packet(packet: MySQLPacket):
    return packet.auth_more


def is_ok_packet(packet: MySQLPacket):
    return packet.ok


def is_error_packet(packet: MySQLPacket):
    return packet.error


def is_local_infile_packet(packet: MySQLPacket):
    return packet.local_infile


async def connection_phase(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    server_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
) -> None:
    phase = MySQLConnectionLifecycle.Connetion
    logger.debug("Initial Handshake Packet")
    await from_server_to_client(server_reader, client_writer, phase)
    logger.debug("Handshake Response Packet")
    await from_client_to_server(client_reader, server_writer, phase)

    logger.debug(
        "Client and server possibly exchange futher authentication method packets"
    )
    while True:
        packet: MySQLPacket = await from_server_to_client(
            server_reader, client_writer, phase
        )
        if is_ok_packet(packet):
            logger.debug("Received OK_Packet")
            # Ok packet or Err packet
            break
        if is_error_packet(packet):
            logger.debug("Received ERR_Packet")
            # Error packet or Err packet
            break

        if is_auth_more_packet(packet):
            # Magic numbers
            # 0x03: Fast auth
            # 0x04: Need full auth
            auth_result: bytes = packet.payload_offset(1)
            auth_result_int: int = struct.unpack("<B", auth_result)[0]
            match auth_result_int:
                case 0x03:
                    packet = await from_server_to_client(
                        server_reader, client_writer, phase
                    )
                    if is_error_packet(packet):
                        logger.debug("Received ERR_Packet")
                        # Error packet or Err packet
                        break
                case 0x04:
                    packet = await from_client_to_server(
                        client_reader, server_writer, phase
                    )
                    if is_error_packet(packet):
                        logger.debug("Received ERR_Packet")
                        # Error packet or Err packet
                        break
                    packet = await from_server_to_client(
                        server_reader, client_writer, phase
                    )
                    if is_error_packet(packet):
                        logger.debug("Received ERR_Packet")
                        # Error packet or Err packet
                        break
                case _:
                    break

            logger.debug("Received AuthMoreData packet")

        # If not, continue shaking
        await from_client_to_server(client_reader, server_writer, phase)
    return


async def command_phase(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    server_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
    command_request_timeout: int,
    command_response_timeout: int,
) -> None:
    phase = MySQLConnectionLifecycle.Command
    logger.debug("========== Command Phase =============")
    while True:
        request_packet: MySQLPacket = await asyncio.wait_for(
            from_client_to_server(client_reader, server_writer, phase),
            timeout=command_request_timeout,
        )
        if request_packet.command_type is MySQLCommandType.COM_QUIT:
            logger.debug("Received COM_QUIT from client. Close connection.")
            break

        response_packet = await asyncio.wait_for(
            from_server_to_client(server_reader, client_writer, phase),
            timeout=command_response_timeout,
        )
        if request_packet.command_type is MySQLCommandType.COM_QUERY:
            if is_local_infile_packet(response_packet):
                logger.debug("Received LOCAL INFILE Request")
                local_infile_packet: MySQLPacket = await from_client_to_server(
                    client_reader, server_writer, phase
                )
                while not local_infile_packet.empty_packet:
                    local_infile_packet = await from_client_to_server(
                        client_reader, server_writer, phase
                    )

                # Maybe OK packet
                await from_server_to_client(server_reader, client_writer, phase)
                continue

            _query_phase: MySQLQueryPhase = MySQLQueryPhase.Metadata
            while True:
                if is_ok_packet(response_packet):
                    logger.debug("Received OK_Packet from MySQL server")
                    if _query_phase is MySQLQueryPhase.Metadata:
                        _query_phase = MySQLQueryPhase.Rows
                        logger.debug(f"Update COM_QUERY phase: {_query_phase}")
                    elif _query_phase is MySQLQueryPhase.Rows:
                        logger.debug(f"End COM_QUERY")
                        break

                if is_error_packet(response_packet):
                    logger.debug("Received ERR_Packet from MySQL server")
                    break

                response_packet = await from_server_to_client(
                    server_reader, client_writer, phase
                )
        continue


async def _fetch_host_by_data(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> KeyData:
    logger.debug("+++++++++++++ KeyData phase +++++++++++++")

    _header: bytes = await asyncio.wait_for(
        client_reader.read(KEY_DATA_HEADER_LENGTH),
        timeout=KEY_DATA_TIMEOUT,
    )
    (protocol_id, version, length) = struct.unpack("<4sBi", _header)
    if client_reader.at_eof():
        raise EOF
    if protocol_id != PROTOCOL_ID:
        raise ProxyInvalidRequest

    data: bytes = await asyncio.wait_for(
        client_reader.read(length),
        timeout=KEY_DATA_TIMEOUT,
    )
    logger.debug(f"KeyData protocol ID: {protocol_id}")
    logger.debug(f"KeyData version: f{version}")
    logger.debug(f"KeyData length: {length}")
    logger.debug("KeyData data: %a" % (data))
    logger.debug("+++++++++++++++++++++++++++++++++++++++++")
    return KeyData(
        protocol_id=protocol_id,
        version=version,
        length=length,
        data=data,
    )


async def _proxy_handler(
    mysql_hosts: MySQLHosts,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    connection_timeout: int,
    command_request_timeout: int,
    command_response_timeout: int,
):
    logger.debug(f"Proxy handler")
    try:
        """
        This function is outside the range of MySQL Client/Server Protocol.
        """
        try:
            key_data: KeyData = await _fetch_host_by_data(
                client_reader=client_reader,
                client_writer=client_writer,
            )
        except asyncio.TimeoutError:
            logger.debug("Timeout error in KeyData")
            return
        except ProxyInvalidRequest:
            logger.debug("Invalid request")
            return

        _mysql: _Server = await select_mysql_host(
            mysql_hosts=mysql_hosts,
            key_data=key_data,
        )
        logger.debug(f"Create connection with {address_from_server(_mysql)}")
        # Connection Phase
        server_reader, server_writer = await asyncio.open_connection(
            str(_mysql.host), _mysql.port
        )

        try:
            await asyncio.wait_for(
                connection_phase(
                    client_reader=client_reader,
                    client_writer=client_writer,
                    server_reader=server_reader,
                    server_writer=server_writer,
                ),
                timeout=connection_timeout,
            )
            await command_phase(
                client_reader=client_reader,
                client_writer=client_writer,
                server_reader=server_reader,
                server_writer=server_writer,
                command_request_timeout=command_request_timeout,
                command_response_timeout=command_response_timeout,
            )
        except asyncio.TimeoutError:
            logger.debug("Timeout error in KeyData")
            return

        # Handshake Response Package
        logger.debug("End dummy handler")
    except EOF:
        logger.debug("Detect EOF")
    finally:
        if "server_writer" in locals():
            server_writer.close()
        client_writer.close()


async def run_proxy_server(
    mysql_hosts: MySQLHosts,
    host: str,
    port: int,
    connection_timeout: int,
    command_request_timeout: int,
    command_response_timeout: int,
):
    server = await asyncio.start_server(
        lambda r, w: _proxy_handler(
            mysql_hosts=mysql_hosts,
            client_reader=r,
            client_writer=w,
            connection_timeout=connection_timeout,
            command_request_timeout=command_request_timeout,
            command_response_timeout=command_response_timeout,
        ),
        host,
        port,
    )
    logger.info("Start proxy")
    async with server:
        await server.serve_forever()
