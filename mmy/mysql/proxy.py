import asyncio
import logging
import socketserver
import struct
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Coroutine, TypeAlias

from rich import print

from ..const import SYSTEM_NAME
from ..server import Server, State, _Server, address_from_server
from .hosts import MySQLHostBroken, MySQLHosts
from .packet import send_error_packet_to_client, send_host_broken_packet
from .proxy_err import (
    MmyLocalInfileUnsupportError,
    MmyProtocolError,
    MmyReadTimeout,
    MmyTLSUnsupportError,
    MmyUnmatchServerError,
)

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
            f"<4sBi{self.length}s",
            self.protocol_id,
            self.version,
            self.length,
            self.data,
        )


async def select_mysql_host(mysql_hosts: MySQLHosts, key_data: KeyData) -> Server:
    return mysql_hosts.get_host_without_move(key_data.data.decode("utf-8"))


from enum import Enum, auto


class ProxyInvalidRequest(RuntimeError):
    pass


class MySQLConnectionPhase(Enum):
    Init = auto()
    SendInitialHandShakePacket = auto()  # server => client
    ReceivedHandShakePacketResponse = auto()  # client <= server
    OK = auto()  # server => client
    ERR = auto()  # server => client
    SEND_0xFE = auto()  # server => client
    OLD_PASSWORD = auto()  # server <= client
    AuthenticationSwitchRequest = auto()  # server => client
    PluginCommunication = auto()  # server <=> client


class MySQLCommandPhase(Enum):
    pass


class MySQLConnectionLifecycle(Enum):
    Connetion = auto()
    Command = auto()


class MySQLProxyDirection(Enum):
    CtoS = auto()  # server ==> client
    StoC = auto()  # server <== client
    Bidirection = auto()  # server <==> client


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
    packet_length: int
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


@dataclass
class MySQLPacketWithDirection:
    direction: MySQLProxyDirection | None
    packet: MySQLPacket | None
    exception: Exception | None = None


class EOFWhere(Enum):
    client = auto()
    server = auto()


class EOF(ValueError):
    def __init__(
        self,
        where: EOFWhere,
    ):
        self.where: EOFWhere = where

    def __str__(self):
        return "Detect EOF from %s" % (
            "client" if self.where is EOFWhere.client else "server"
        )


class MySQLForceShutdownConnection(RuntimeError):
    pass


FromToHandler: TypeAlias = Callable[
    [MySQLPacket], MySQLPacket | Coroutine[None, None, MySQLPacket]
]


class CapabilitiesFlags(Enum):
    CLIENT_LONG_PASSWORD = 1 << 0
    CLIENT_FOUND_ROWS = 1 << 1
    CLIENT_LONG_FLAG = 1 << 2
    CLIENT_CONNECT_WITH_DB = 1 << 3
    CLIENT_NO_SCHEMA = 1 << 4
    CLIENT_COMPRESS = 1 << 5
    CLIENT_ODBC = 1 << 6
    CLIENT_LOCAL_FILES = 1 << 7
    CLIENT_IGNORE_SPACE = 1 << 8
    CLIENT_PROTOCOL_41 = 1 << 9
    CLIENT_INTERACTIVE = 1 << 10
    CLIENT_SSL = 1 << 11
    CLIENT_IGNORE_SIGPIPE = 1 << 12
    CLIENT_TRANSACTIONS = 1 << 13
    CLIENT_RESERVED = 1 << 14
    CLIENT_SECURE_CONNECTION = 1 << 15
    CLIENT_MULTI_STATEMENTS = 1 << 16
    CLIENT_MULTI_RESULTS = 1 << 17
    CLIENT_PS_MULTI_RESULTS = 1 << 18
    CLIENT_PLUGIN_AUTH = 1 << 19
    CLIENT_CONNECT_ATTRS = 1 << 20
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22
    CLIENT_PROGRESS = 1 << 29
    CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30
    CLIENT_REMEMBER_OPTIONS = 1 << 31


class HandShakeVersion(Enum):
    V9 = auto()
    V10 = auto()


@dataclass
class HandShakeV9:
    protocol_version: int
    server_version: str
    thread_id: int
    scramble: str


@dataclass
class HandShakeV10:
    protocol_version: int  # 1byte
    server_version: str  # string<NULL>
    thread_id: int  # 4bytes
    auth_plugin_data_part_1: bytes  # (scramble) 8bytes
    filter: int  # 1byte
    capability_flags_1: int  # 2bytes
    character_set: int  # 1byte
    status_flags: int  # 2bytes
    capability_flags_2: int  # 2bytes
    auth_plugin_data_len: int  # 1bytes
    reserved: bytes  # 10bytes
    auth_plugin_data_part_2: bytes  # MAX(13, auth_plugin_data_len - 8)
    auth_plugin_name: bytes  # string<NULL>


@dataclass
class connection_attribute_item:
    key: bytes
    value: bytes


@dataclass
class connection_attribute:
    size: int
    attrs: list[connection_attribute_item]


@dataclass
class HandshakeResponsePacket:
    client_capabilities: int
    max_packet_size: int
    client_character_collation: int
    username: str
    authentication_data: bytes | None = None
    authentication_response: bytes | None = None

    default_database_name: str | None = None
    authentication_plugin_name: bytes | None = None
    connection_attributes: connection_attribute | None = None


CapabilitiesFlagsSet: TypeAlias = set[CapabilitiesFlags]


class MySQLAuthPlugin(Enum):
    mysql_old_password = b"mysql_old_password"
    mysql_clear_password = b"mysql_clear_password"
    mysql_native_password = b"mysql_native_password"
    caching_sha2_password = b"caching_sha2_password"


def log_debug_direction(d: MySQLProxyDirection):
    def _log_debug_direction(asyn):
        async def _wlog_debug_direction(*args, **kwargs):
            self: MySQLProxyContext = args[0]
            assert isinstance(self, MySQLProxyContext) is True
            if d is MySQLProxyDirection.CtoS:
                logger.debug(f"Server <== Client")
            else:
                logger.debug(f"Server ==> Client")

            return await asyn(*args, **kwargs)

        return _wlog_debug_direction

    return _log_debug_direction


logger: logging.Logger = logging.getLogger(__name__)


class MySQLProxyContext:
    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        mysql_hosts: MySQLHosts,
    ):
        super().__init__()
        self._phase: MySQLConnectionLifecycle = MySQLConnectionLifecycle.Connetion
        self._client_capabilities: CapabilitiesFlagsSet | None = None
        self._server_capabilities: CapabilitiesFlagsSet | None = None
        self._client_reader: asyncio.StreamReader = client_reader
        self._client_writer: asyncio.StreamWriter = client_writer
        self._server_reader: asyncio.StreamReader | None = client_reader
        self._server_writer: asyncio.StreamWriter | None = client_writer
        self._key_data: KeyData | None = None
        self._mysql_hosts: MySQLHosts = mysql_hosts
        self._handshake: HandShakeV9 | HandShakeV10 | None = None
        self._handshake_version: HandShakeVersion | None = None
        self._direction: MySQLProxyDirection = MySQLProxyDirection.StoC
        self._connection_phase: MySQLConnectionPhase = MySQLConnectionPhase.Init
        self._auth_plugin: MySQLAuthPlugin | None = None
        self._handshake_response_packet: HandshakeResponsePacket | None = None
        self._server: _Server | None = None
        self._last_packet: MySQLPacket | None = None

    def update_connection_phase(self, new: MySQLConnectionPhase):
        self._connection_phase = new
        logger.debug("Update connection phase: " + new.name)
        match new:
            case MySQLConnectionPhase.Init:
                self.update_direction(MySQLProxyDirection.StoC)
            case MySQLConnectionPhase.SendInitialHandShakePacket:
                self.update_direction(MySQLProxyDirection.CtoS)
            case MySQLConnectionPhase.ReceivedHandShakePacketResponse:
                self.update_direction(MySQLProxyDirection.StoC)
            case MySQLConnectionPhase.OK | MySQLConnectionPhase.ERR:
                pass
            case MySQLConnectionPhase.AuthenticationSwitchRequest:
                self.update_direction(MySQLProxyDirection.Bidirection)
            case _:
                pass

    async def received_packet(
        self,
        reader: asyncio.StreamReader,
        direction: MySQLProxyDirection,
    ) -> MySQLPacket:
        logger.debug("Waiting for receiving data")
        _header: bytes = await reader.read(4)
        if reader.at_eof():
            raise EOF(EOFWhere.client if MySQLProxyDirection.CtoS else EOFWhere.server)

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
            if self._phase is MySQLConnectionLifecycle.Command:
                _p: tuple[Any, ...] = struct.unpack("<B", _first_payload)
                _fp_int = _p[0]

        _command_type: MySQLCommandType = MySQLCommandType.NONE
        if (
            direction is MySQLProxyDirection.CtoS
            and payload_length > 0
            and self._phase is MySQLConnectionLifecycle.Command
        ):
            _command_type = MySQLCommandType(_fp_int)

        packet: MySQLPacket = MySQLPacket(
            packet_length=len(_header) + payload_length,
            payload_length=payload_length,
            sequence_id=sequence_id,
            header=_header,
            payload=_payload,
            empty_packet=payload_length == 0,
            auth_more=self._phase is MySQLConnectionLifecycle.Connetion
            and _first_payload == b"\x01",
            ok=_first_payload == b"\x00" or _payload[:1] == b"\xfe",
            error=_first_payload == b"\xff",
            local_infile=_first_payload == b"\xfb",
            phase=self._phase,
            first_payload=_first_payload,
            command_type=_command_type,
        )
        self._last_packet = packet
        return packet

    async def send_package(
        self, writer: asyncio.StreamWriter, packet: MySQLPacket
    ) -> None:
        writer.write(packet.pack_to_bytes())
        await writer.drain()

    def _dump_payload(self, data: bytes):
        import sys

        def printable(data):
            if 32 <= data < 127:
                return chr(data)
            return "."

        try:
            logger.debug("packet length: %d", len(data))
            for i in range(1, 7):
                f = sys._getframe(i)
                logger.debug(
                    "call[%d]: %s (line %d)" % (i, f.f_code.co_name, f.f_lineno)
                )
        except ValueError:
            pass
        logger.debug("-" * 66)
        dump_data = [data[i : i + 16] for i in range(0, min(len(data), 256), 16)]
        for d in dump_data:
            logger.debug(
                " ".join("{:02X}".format(x) for x in d)
                + "   " * (16 - len(d))
                + " " * 2
                + "".join(printable(x) for x in d)
            )
        logger.debug("-" * 66)
        return

    async def _after_receive(
        self,
        packet: MySQLPacket,
        fn: FromToHandler | None,
    ) -> MySQLPacket:
        if self._phase is MySQLConnectionLifecycle.Connetion:
            logger.debug(f"[Connection] Received package")
        else:
            logger.debug(f"[Command] Received package:")

        logger.debug(f"\tPacket length: %d" % (packet.packet_length))
        logger.debug(f"\tHeader: %a" % (packet.header))
        logger.debug(f"\tPayload length: %a" % (packet.payload_length))
        if packet.payload:
            self._dump_payload(packet.payload)
        logger.debug(f"\tSequence ID: {packet.sequence_id}")
        logger.debug(f"\tOK: {packet.ok}")
        logger.debug(f"\tERR: {packet.error}")
        logger.debug(f"\tPhase: {packet.phase}")
        logger.debug(f"\tFirst payload: %a" % (packet.first_payload))
        logger.debug(f"\tCommand Type: {packet.command_type}")
        if fn is not None:
            logger.debug(f"With handler")
            _c: Coroutine[None, None, MySQLPacket] | MySQLPacket = fn(packet)
            import copy

            _packet = copy.deepcopy(packet)
            if isinstance(_c, MySQLPacket):
                packet = _c
            else:
                packet = await _c

            if packet.payload != _packet.payload:
                logger.debug(f"Detect rewriting packet")
            else:
                logger.debug(f"Not rewriting packet")

        return packet

    async def _from_to(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        direction: MySQLProxyDirection,
        fn: FromToHandler | None,
    ):
        packet: MySQLPacket = await self.received_packet(reader, direction)
        packet = await self._after_receive(packet=packet, fn=fn)

        if fn is not None:
            logger.debug(
                f"Send rewriting packet to {'server' if direction is MySQLProxyDirection.CtoS else 'client'}"
            )
        else:
            logger.debug(
                f"No handler. send original packet to {'server' if direction is MySQLProxyDirection.CtoS else 'client'}"
            )

        await self.send_package(
            writer=writer,
            packet=packet,
        )
        return packet

    async def _only_deliver(
        self,
        stoc_fn: FromToHandler | None,
        ctos_fn: FromToHandler | None,
    ) -> list[MySQLPacketWithDirection]:
        """

        I don't know to come packet from client or server.
        So, ready for coming it from one side and for passing it to the other side.
        If received OK or Error packet from server, finish.

        """

        async def _ctos():
            packet = await self.received_packet(
                self._client_reader,
                MySQLProxyDirection.CtoS,
            )
            return MySQLPacketWithDirection(
                direction=MySQLProxyDirection.CtoS,
                packet=packet,
            )

        async def _stoc():
            packet = await self.received_packet(
                self.server_reader,
                MySQLProxyDirection.StoC,
            )
            return MySQLPacketWithDirection(
                direction=MySQLProxyDirection.StoC,
                packet=packet,
            )

        async def _resource_release(pending):
            for p in pending:
                try:
                    p.cancel()
                    await p
                except asyncio.CancelledError as e:
                    logger.debug("Cancel pending task")

        ctos: asyncio.Task = asyncio.create_task(_ctos())
        stoc: asyncio.Task = asyncio.create_task(_stoc())
        _D = "_direction"
        setattr(ctos, _D, MySQLProxyDirection.CtoS)
        setattr(stoc, _D, MySQLProxyDirection.StoC)
        done, pending = await asyncio.wait(
            [ctos, stoc],
            return_when=asyncio.FIRST_COMPLETED,
        )
        await _resource_release(pending)

        packets: list[MySQLPacketWithDirection] = list()
        for d in done:
            assert hasattr(d, _D) is True
            direction: MySQLProxyDirection = getattr(d, _D)
            try:
                result: MySQLPacketWithDirection = d.result()
                packets.append(result)

                if result.direction is None or result.packet is None:
                    raise ValueError

                packet: MySQLPacket = result.packet
                if direction is MySQLProxyDirection.CtoS:
                    packet = await self._after_receive(packet=packet, fn=ctos_fn)
                    await self.send_package(
                        writer=self.server_writer,
                        packet=packet,
                    )
                else:
                    packet = await self._after_receive(packet=packet, fn=stoc_fn)
                    await self.send_package(
                        writer=self._client_writer,
                        packet=packet,
                    )

                logger.debug(
                    f"Send packet to {'server' if direction is MySQLProxyDirection.CtoS else 'client'}"
                )
            except EOF:
                packets.append(
                    MySQLPacketWithDirection(
                        direction=None,
                        packet=None,
                        exception=EOF(
                            EOFWhere.client
                            if direction is MySQLProxyDirection.CtoS
                            else EOFWhere.server
                        ),
                    )
                )

        return packets

    @log_debug_direction(MySQLProxyDirection.StoC)
    async def from_server_to_client(
        self,
        fn: FromToHandler | None,
    ) -> MySQLPacket:
        return await self._from_to(
            reader=self.server_reader,
            writer=self._client_writer,
            direction=MySQLProxyDirection.StoC,
            fn=fn,
        )

    @log_debug_direction(MySQLProxyDirection.CtoS)
    async def from_client_to_server(
        self,
        fn: FromToHandler | None,
    ) -> MySQLPacket:
        return await self._from_to(
            reader=self._client_reader,
            writer=self.server_writer,
            direction=MySQLProxyDirection.CtoS,
            fn=fn,
        )

    @log_debug_direction(MySQLProxyDirection.Bidirection)
    async def only_deliver(
        self,
        stoc_fn: FromToHandler | None,
        ctos_fn: FromToHandler | None,
    ) -> list[MySQLPacketWithDirection]:
        return await self._only_deliver(
            stoc_fn=stoc_fn,
            ctos_fn=ctos_fn,
        )

    def is_auth_more_packet(self, packet: MySQLPacket):
        if packet.auth_more:
            logger.debug("Received AuthMoreData packet")

        return packet.auth_more

    def is_ok_packet(self, packet: MySQLPacket):
        if packet.ok:
            logger.debug("Received OK_Packet")

        return packet.ok

    def is_error_packet(self, packet: MySQLPacket):
        if packet.error:
            logger.debug("Received ERR_Packet")

        return packet.error

    def is_local_infile_packet(self, packet: MySQLPacket):
        return packet.local_infile

    def _parse_capability_flags(self, flags: int) -> CapabilitiesFlagsSet:
        _flags: CapabilitiesFlagsSet = set()
        for flag in CapabilitiesFlags:
            if flags & flag.value != 0:
                _flags.add(flag)

        return _flags

    def parse_client_flag(self, packet: MySQLPacket) -> CapabilitiesFlagsSet:
        if packet.payload is None or len(packet.payload) < 4:
            return set()

        flags: int = struct.unpack("<i", packet.payload[:4])[0]
        return self._parse_capability_flags(flags)

    def parse_initial_handshake(
        self,
        initial_handshake_packet: MySQLPacket,
    ) -> HandShakeV9 | HandShakeV10:
        if initial_handshake_packet.first_payload is None:
            raise ValueError
        if initial_handshake_packet.payload is None:
            raise ValueError

        _payload: bytes = initial_handshake_packet.payload
        protocol_version: int = struct.unpack(
            "<B", initial_handshake_packet.first_payload
        )[0]
        logger.debug(f"Handshake version: {protocol_version}")
        if protocol_version == 9:
            datas: list[bytes] = _payload[1:].split(b"\x00", 1)
            server_version: str = datas[0].decode("utf-8")
            left: bytes = datas[1]
            thread_id: int = struct.unpack("<i", left[:4])[0]
            scramble: str = left[1:].decode("utf-8")

            v9: HandShakeV9 = HandShakeV9(
                protocol_version=protocol_version,
                server_version=server_version,
                thread_id=thread_id,
                scramble=scramble,
            )
            self._handshake = v9
            self._handshake_version = HandShakeVersion.V9
            return v9
        elif protocol_version == 10:
            v10_datas: list[bytes] = _payload[1:].split(b"\x00", 1)
            v10_server_version: str = v10_datas[0].decode("utf-8")
            splitx001: bytes = v10_datas[1]
            res = struct.unpack("<i8sBHBHHB10s", splitx001[:31])
            (
                thread_id,
                auth_plugin_data_part_1,
                filter,
                capability_flags_1,
                character_set,
                status_flags,
                capability_flags_2,
                auth_plugin_data_len,
                reserved,
            ) = res
            _plugin = splitx001[31:]
            length = max(13, auth_plugin_data_len - 8)
            auth_plugin_data_part_2: bytes = _plugin[:length]
            auth_plugin_name: bytes = _plugin[length:]
            v10: HandShakeV10 = HandShakeV10(
                protocol_version=protocol_version,
                server_version=v10_server_version,
                thread_id=thread_id,
                auth_plugin_data_part_1=auth_plugin_data_part_1,
                filter=filter,
                capability_flags_1=capability_flags_1,
                character_set=character_set,
                status_flags=status_flags,
                capability_flags_2=capability_flags_2,
                auth_plugin_data_len=auth_plugin_data_len,
                reserved=reserved,
                auth_plugin_data_part_2=auth_plugin_data_part_2,
                auth_plugin_name=auth_plugin_name,
            )
            self._handshake = v10
            self._handshake_version = HandShakeVersion.V10
            return v10
        else:
            raise ValueError

    def parse_server_flag(
        self,
        initial_handshake_packet: MySQLPacket,
    ) -> set[CapabilitiesFlags]:
        data = self.parse_initial_handshake(
            initial_handshake_packet=initial_handshake_packet
        )
        if isinstance(data, HandShakeV9):
            return set()
        elif isinstance(data, HandShakeV10):
            capability_flags: int = (
                data.capability_flags_2 << 32 | data.capability_flags_1
            )
            return self._parse_capability_flags(capability_flags)
        else:
            raise RuntimeError

    def get_server_capability_flags(
        self,
        handshake: HandShakeV9 | HandShakeV10,
    ) -> set[CapabilitiesFlags]:
        if isinstance(handshake, HandShakeV9):
            return set()

        _capability_flags: int = (
            handshake.capability_flags_2 << 16 | handshake.capability_flags_1
        )
        logger.debug(f"capability_flags_2 : {bin(handshake.capability_flags_2)}")
        logger.debug(f"capability_flags_1 : {bin(handshake.capability_flags_1)}")
        logger.debug(f"capability_flags   : {bin(_capability_flags)}")
        capability_flags: set[CapabilitiesFlags] = self._parse_capability_flags(
            _capability_flags
        )
        return capability_flags

    def rewrite_initial_handshake(self, packet: MySQLPacket) -> MySQLPacket:
        if self._handshake is None or packet.payload is None:
            raise RuntimeError

        if isinstance(self._handshake, HandShakeV9):
            return packet

        if CapabilitiesFlags.CLIENT_SSL not in self.server_capabilities:
            return packet

        # Server support TLS/SSL, but mmy not support TLS/SSL.
        # So, rewrite packet so that server is regarded as non supported it.
        capabilities_flags_1: int = self._handshake.capability_flags_1
        capabilities_flags_1 &= ~CapabilitiesFlags.CLIENT_SSL.value
        self.server_capabilities.remove(CapabilitiesFlags.CLIENT_SSL)
        if capabilities_flags_1 == self._handshake.capability_flags_1:
            raise RuntimeError("Unexpected result of capalbility flags")

        self._handshake.capability_flags_1 = capabilities_flags_1
        _capability_flags: int = (
            self._handshake.capability_flags_2 << 16
            | self._handshake.capability_flags_1
        )
        logger.debug(f"capability_flags_2 : {bin(self._handshake.capability_flags_2)}")
        logger.debug(f"capability_flags_1 : {bin(self._handshake.capability_flags_1)}")
        logger.debug(f"capability_flags   : {bin(_capability_flags)}")

        NULL_STR = b"\x00"
        logger.debug("Before rewriting Initial Handshake Packet")
        self._dump_payload(packet.payload)
        packet.payload = struct.pack(
            f"<B{len(self._handshake.server_version)+1}si8sBHBHHB10s{len(self._handshake.auth_plugin_data_part_2)}s{len(self._handshake.auth_plugin_name)}s",
            self._handshake.protocol_version,
            self._handshake.server_version.encode("utf-8") + NULL_STR,
            self._handshake.thread_id,
            self._handshake.auth_plugin_data_part_1,
            self._handshake.filter,
            self._handshake.capability_flags_1,
            self._handshake.character_set,
            self._handshake.status_flags,
            self._handshake.capability_flags_2,
            self._handshake.auth_plugin_data_len,
            self._handshake.reserved,
            self._handshake.auth_plugin_data_part_2,
            self._handshake.auth_plugin_name,
        )
        logger.debug("After rewriting Initial Handshake Packet")
        self._dump_payload(packet.payload)
        logger.debug(
            "Rewrite packet to be regarded MySQL server as non support TLS/SSL"
        )
        if len(packet.payload) != packet.payload_length:
            raise RuntimeError("Bug of rewriting Initial Handshake Packet")
        return packet

    async def handle_client_flags(self, packet: MySQLPacket) -> None:
        """

        SSL: not support

        """
        if (
            CapabilitiesFlags.CLIENT_SSL in self.client_capabilities
            and CapabilitiesFlags.CLIENT_SSL in self.server_capabilities
        ):
            logger.error(f"{SYSTEM_NAME} is not SSL/TLS support")
            await send_error_packet_to_client(
                client_writer=self._client_writer,
                sequence_id=packet.sequence_id + 1,
                err=MmyTLSUnsupportError("Not support SSL/TLS"),
            )
            raise MySQLForceShutdownConnection

        return

    async def proxy_handler_initial_handshake_packet(
        self,
        initial_handshake_packet: MySQLPacket,
    ) -> MySQLPacket:
        handshake: HandShakeV10 | HandShakeV9 = self.parse_initial_handshake(
            initial_handshake_packet
        )
        self._server_capabilities = self.get_server_capability_flags(handshake)
        logger.debug(
            f"Capabilities Flags of server: {','.join([i.name for i in self._server_capabilities])}"
        )
        return self.rewrite_initial_handshake(initial_handshake_packet)

    def rewrite_handler_handshake_response(
        self,
        packet: MySQLPacket,
    ) -> MySQLPacket:
        if CapabilitiesFlags.CLIENT_SSL not in self.client_capabilities:
            return packet

        if packet.payload is None:
            raise RuntimeError

        # Client support TLS/SSL, but mmy not support TLS/SSL.
        # So, rewrite packet so that client is regarded as non supported it.
        _client_capability_bytes: int = 4
        _client_capability: int = struct.unpack(
            "<i", packet.payload[:_client_capability_bytes]
        )[0]
        _new_client_capability = (
            _client_capability & ~CapabilitiesFlags.CLIENT_SSL.value
        )
        if _client_capability == _new_client_capability:
            raise RuntimeError("Client capability flag bug")
        logger.debug(f"Before rewriting capability_flags: {bin(_client_capability)}")
        logger.debug(
            f"After rewriting capability_flags:  {bin(_new_client_capability)}"
        )

        # Actually rewrite packet
        packet.payload = struct.pack(
            f"<i{packet.payload_length-_client_capability_bytes}s",
            _new_client_capability,
            packet.payload[_client_capability_bytes:],
        )
        return packet

    async def proxy_handler_handshake_response(
        self,
        packet: MySQLPacket,
    ) -> MySQLPacket:
        self._client_capabilities = self.parse_client_flag(packet)
        logger.debug(
            f"Capabilities Flags of client: {','.join([i.name for i in self._client_capabilities])}"
        )
        packet = self.rewrite_handler_handshake_response(packet)
        await self.handle_client_flags(packet)
        assert packet.payload is not None

        NULL_STR: bytes = b"\x00"
        pl: bytes = packet.payload
        _offset: int = 0
        (
            client_capabilities,
            max_packet_size,
            client_character_collation,
            _,
        ) = struct.unpack("<iiB23s", pl[_offset : _offset + 32])
        _offset += 32

        username: bytes = pl[_offset:].split(NULL_STR)[0]
        username_length: int = len(username)
        _offset += username_length + len(NULL_STR)
        authentication_data: bytes | None = None
        authentication_response: bytes | None = None
        if (
            CapabilitiesFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            in self.server_capabilities
        ):
            _ad_length: bytes = pl[_offset:].split(NULL_STR)[0]
            authentication_data_length: int = int.from_bytes(_ad_length, "little")
            _offset += len(_ad_length) + len(NULL_STR)
            authentication_data = pl[_offset : _offset + authentication_data_length]
            _offset += authentication_data_length
        elif CapabilitiesFlags.CLIENT_SECURE_CONNECTION in self.server_capabilities:
            authentication_response_length: int = struct.unpack(
                "<B", pl[_offset : _offset + 1]
            )[0]
            _offset += 1
            authentication_response = pl[
                _offset : _offset + authentication_response_length
            ]
            _offset += authentication_response_length
        else:
            authentication_response = pl[_offset:].split(NULL_STR)[0]
            _offset += len(authentication_response)

        default_database_name: bytes | None = None
        authentication_plugin_name: bytes | None = None
        connection_attributes: connection_attribute | None = None
        if CapabilitiesFlags.CLIENT_CONNECT_WITH_DB in self.server_capabilities:
            default_database_name = pl[_offset:].split(NULL_STR)[0]
            _offset += len(default_database_name) + len(NULL_STR)

        if (
            CapabilitiesFlags.CLIENT_PLUGIN_AUTH in self.client_capabilities
            or CapabilitiesFlags.CLIENT_PLUGIN_AUTH in self.server_capabilities
        ):
            authentication_plugin_name = pl[_offset:].split(NULL_STR)[0]
            _offset += len(authentication_plugin_name) + len(NULL_STR)
            pass

        if CapabilitiesFlags.CLIENT_CONNECT_ATTRS in self.server_capabilities:
            cas: bytes = pl[_offset:].split(NULL_STR)[0]
            cas_length: int = len(cas)
            connection_attribute_size: int = int.from_bytes(cas, "little")
            _offset += cas_length + len(NULL_STR)

            items: list[connection_attribute_item] = list()
            while _offset < packet.payload_length:
                _key: bytes = pl[_offset:].split(NULL_STR)[0]
                _key_length: int = int.from_bytes(_key, "little")
                _offset += len(_key) + len(NULL_STR)
                key = pl[_offset : _offset + _key_length]

                _value: bytes = pl[_offset:].split(NULL_STR)[0]
                _value_length: int = int.from_bytes(_value, "little")
                _offset += len(_value) + len(NULL_STR)
                value = pl[_offset : _offset + _value_length]
                items.append(
                    connection_attribute_item(
                        key=key,
                        value=value,
                    )
                )

            connection_attributes = connection_attribute(
                size=connection_attribute_size,
                attrs=items,
            )

        self._handshake_response_packet = HandshakeResponsePacket(
            client_capabilities=client_capabilities,
            max_packet_size=max_packet_size,
            client_character_collation=client_character_collation,
            username=username.decode("utf-8"),
            authentication_data=authentication_data,
            authentication_response=authentication_response,
            default_database_name=default_database_name.decode("utf-8")
            if default_database_name is not None
            else None,
            authentication_plugin_name=authentication_plugin_name,
            connection_attributes=connection_attributes,
        )
        return packet

    @property
    def handshake_response_packet(self) -> HandshakeResponsePacket:
        if self._handshake_response_packet is None:
            raise RuntimeError

        return self._handshake_response_packet

    def update_phase(self, new_phase: MySQLConnectionLifecycle):
        self._phase = new_phase

    def update_direction(self, new_direction: MySQLProxyDirection):
        self._direction = new_direction

    async def connection_phase(
        self,
        timeout_per_one_request: int = 5,
    ) -> None:
        self.update_connection_phase(MySQLConnectionPhase.Init)
        self.update_phase(MySQLConnectionLifecycle.Connetion)
        logger.debug("### Initial Handshake Packet ###")
        try:
            await asyncio.wait_for(
                self.from_server_to_client(
                    fn=self.proxy_handler_initial_handshake_packet,
                ),
                timeout=timeout_per_one_request,
            )
            self.update_connection_phase(
                MySQLConnectionPhase.SendInitialHandShakePacket
            )
            logger.debug("### Handshake Response Packet ###")
            packet: MySQLPacket = await asyncio.wait_for(
                self.from_client_to_server(
                    fn=self.proxy_handler_handshake_response,
                ),
                timeout=timeout_per_one_request,
            )
            self.update_connection_phase(
                MySQLConnectionPhase.ReceivedHandShakePacketResponse
            )

            logger.debug(
                "### Client and server possibly exchange futher authentication method packets ###"
            )
            while True:
                packet = await asyncio.wait_for(
                    self.from_server_to_client(fn=None),
                    timeout=timeout_per_one_request,
                )
                if self.is_ok_packet(packet):
                    self.update_connection_phase(MySQLConnectionPhase.OK)
                    break
                elif self.is_error_packet(packet):
                    self.update_connection_phase(MySQLConnectionPhase.ERR)
                    raise MySQLForceShutdownConnection
                elif (
                    CapabilitiesFlags.CLIENT_PLUGIN_AUTH not in self.client_capabilities
                    or CapabilitiesFlags.CLIENT_PLUGIN_AUTH
                    not in self.server_capabilities
                ):
                    """
                    The client or server doesn't have PLUGIN_AUTH capability:
                        * Server sends 0xFE byte
                        * Client sends old_password
                    """
                    self.update_connection_phase(MySQLConnectionPhase.SEND_0xFE)
                    await asyncio.wait_for(
                        self.from_client_to_server(
                            fn=None,
                        ),
                        timeout=timeout_per_one_request,
                    )
                    self.update_connection_phase(MySQLConnectionPhase.OLD_PASSWORD)
                    continue
                else:
                    self.update_connection_phase(
                        MySQLConnectionPhase.AuthenticationSwitchRequest
                    )

                    while True:
                        pds: list[MySQLPacketWithDirection] = await self.only_deliver(
                            ctos_fn=None,
                            stoc_fn=None,
                        )
                        for pd in pds:
                            if pd.exception is not None:
                                raise pd.exception
                            if pd.packet is None or pd.direction is None:
                                raise ValueError

                            packet = pd.packet
                            if self.is_ok_packet(packet):
                                self.update_connection_phase(MySQLConnectionPhase.OK)
                                return
                            elif self.is_error_packet(packet):
                                self.update_connection_phase(MySQLConnectionPhase.ERR)
                                raise MySQLForceShutdownConnection

                        # No OK or ERR packet
                        continue
        except asyncio.TimeoutError:
            logger.debug("Timeout on connection phase")
        return

    @property
    def last_packet(self) -> MySQLPacket:
        if self._last_packet is None:
            raise RuntimeError
        return self._last_packet

    async def command_phase(
        self,
        command_timeout: int,
    ) -> None:
        self.update_phase(MySQLConnectionLifecycle.Command)
        logger.debug("========== Command Phase =============")
        self.update_direction(MySQLProxyDirection.CtoS)
        try:

            async def wrap_command_keydata(packet: MySQLPacket) -> MySQLPacket:
                await self.command_fetch_keydata(packet)
                return packet

            async def wrap_stoc(packet: MySQLPacket) -> MySQLPacket:
                await self.check_invalid_request_from_server(packet)
                return packet

            while True:
                pds: list[MySQLPacketWithDirection] = await asyncio.wait_for(
                    self.only_deliver(
                        ctos_fn=wrap_command_keydata,
                        stoc_fn=wrap_stoc,
                    ),
                    timeout=command_timeout,
                )
                for pd in pds:
                    if pd.exception is not None:
                        raise pd.exception

                    if pd.packet is None or pd.direction is None:
                        raise ValueError
        except asyncio.TimeoutError:
            logger.debug("Timeout on Connection phase")
            if self._direction is MySQLProxyDirection.CtoS:
                await send_error_packet_to_client(
                    client_writer=self._client_writer,
                    sequence_id=self.last_packet.sequence_id + 1,
                    err=MmyReadTimeout("Writing Timeout in Command phase"),
                )
        return

    async def _fetch_keydata(self) -> KeyData:
        logger.debug("+++++++++++++ KeyData phase +++++++++++++")
        if self._client_reader.at_eof():
            raise EOF(EOFWhere.client)

        try:
            _header: bytes = await asyncio.wait_for(
                self._client_reader.readexactly(KEY_DATA_HEADER_LENGTH),
                timeout=KEY_DATA_TIMEOUT,
            )
        except asyncio.IncompleteReadError:
            raise EOF(EOFWhere.client)

        if len(_header) != KEY_DATA_HEADER_LENGTH:
            raise ValueError(
                f"Header length must be {KEY_DATA_HEADER_LENGTH}bytes but received now {len(_header)}."
            )
        """
        KeyData header:
            Protocol ID: 4bytes
            Version: 1bytes         ->  total 9bytes
            Length: 4bytes
        """
        (protocol_id, version, length) = struct.unpack(
            "<4sBi", _header[:KEY_DATA_HEADER_LENGTH]
        )
        if self._client_reader.at_eof():
            raise EOF(EOFWhere.client)
        if protocol_id != PROTOCOL_ID:
            raise ProxyInvalidRequest

        data: bytes = await asyncio.wait_for(
            self._client_reader.read(length),
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

    async def initial_fetch_keydata(self) -> KeyData:
        kd: KeyData = await self._fetch_keydata()
        self._key_data = kd
        return self._key_data

    async def command_fetch_keydata(self, packet: MySQLPacket) -> None:
        assert self._phase is MySQLConnectionLifecycle.Command
        if packet.command_type is MySQLCommandType.COM_QUIT:
            return

        logger.debug("Receive command KeyData")
        _kd: KeyData = await self._fetch_keydata()
        try:
            _s: _Server = await select_mysql_host(
                mysql_hosts=self._mysql_hosts,
                key_data=_kd,
            )
        except MySQLHostBroken:
            await send_host_broken_packet(
                client_writer=self._client_writer,
            )

        if self.server != _s:
            # mmy protocol error
            await send_error_packet_to_client(
                client_writer=self._client_writer,
                sequence_id=packet.sequence_id + 1,
                err=MmyUnmatchServerError(
                    "MySQL server that is decided by KeyData value is different at the start of communication and now"
                ),
            )
            raise MmyUnmatchServerError

    async def check_invalid_request_from_server(self, packet: MySQLPacket) -> None:
        assert self._phase is MySQLConnectionLifecycle.Command
        if packet.first_payload is None:
            return

        if packet.first_payload == b"\xFB":  # This request is LOCAL INFILE
            # mmy protocol error
            await send_error_packet_to_client(
                client_writer=self._client_writer,
                sequence_id=packet.sequence_id,
                err=MmyLocalInfileUnsupportError("Unsupport LOAD INFILE Request"),
            )
        return

    @property
    def server(self) -> _Server:
        if self._server is None:
            raise RuntimeError

        return self._server

    async def select_server(self):
        server: Server = await select_mysql_host(
            mysql_hosts=self._mysql_hosts,
            key_data=self.key_data,
        )
        logger.debug(f"Create connection with {address_from_server(server)}")
        self._server = server
        self._server_reader, self._server_writer = await asyncio.open_connection(
            str(server.host), server.port
        )
        return server

    @property
    def server_capabilities(self) -> CapabilitiesFlagsSet:
        if self._server_capabilities is None:
            raise RuntimeError
        return self._server_capabilities

    @property
    def client_capabilities(self) -> CapabilitiesFlagsSet:
        if self._client_capabilities is None:
            raise RuntimeError
        return self._client_capabilities

    @property
    def server_reader(self) -> asyncio.StreamReader:
        if self._server_reader is None:
            raise RuntimeError
        return self._server_reader

    @property
    def server_writer(self) -> asyncio.StreamWriter:
        if self._server_writer is None:
            raise RuntimeError
        return self._server_writer

    @property
    def key_data(self) -> KeyData:
        if self._key_data is None:
            raise RuntimeError
        return self._key_data

    async def release(self):
        if self._server_writer is not None and not self.server_writer.is_closing:
            self.server_writer.close()
            await self.server_writer.wait_closed()
        if not self._client_writer.is_closing:
            self._client_writer.close()
            await self._client_writer.wait_closed()


async def _proxy_handler(
    mysql_hosts: MySQLHosts,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    connection_timeout: int,
    command_timeout: int,
):
    logger = logging.getLogger(MySQLProxyContext.__name__)
    try:
        """
        This function is outside the range of MySQL Client/Server Protocol.
        """
        ctx: MySQLProxyContext = MySQLProxyContext(
            client_reader=client_reader,
            client_writer=client_writer,
            mysql_hosts=mysql_hosts,
        )
        logger.debug(f"Proxy handler")

        try:
            await ctx.initial_fetch_keydata()
        except asyncio.TimeoutError:
            logger.debug("Timeout error on Initial KeyData")
            return
        except ProxyInvalidRequest:
            logger.debug("Invalid request")
            return
        try:
            await ctx.select_server()
        except MySQLHostBroken:
            await send_host_broken_packet(
                client_writer=client_writer,
            )
            return
        try:
            # Connection Phase
            await asyncio.wait_for(
                ctx.connection_phase(),
                timeout=connection_timeout,
            )

            await ctx.command_phase(
                command_timeout=command_timeout,
            )
        except asyncio.TimeoutError:
            logger.debug("Timeout error in KeyData")
            return

        logger.debug("End proxy handler")
    except EOF as e:
        logger.debug(e)
    except MmyProtocolError as e:
        logger.error(e)
    except MySQLForceShutdownConnection:
        logger.debug("Force shutdown connection")
    finally:
        await ctx.release()
        logger.debug("End of connection")


async def proxy_handler(
    mysql_hosts: MySQLHosts,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    connection_timeout: int,
    command_timeout: int,
):
    await _proxy_handler(
        mysql_hosts=mysql_hosts,
        client_reader=client_reader,
        client_writer=client_writer,
        connection_timeout=connection_timeout,
        command_timeout=command_timeout,
    )


async def create_server(
    mysql_hosts: MySQLHosts,
    host: str,
    port: int,
    connection_timeout: int,
    command_timeout: int,
) -> asyncio.Server:
    server = await asyncio.start_server(
        lambda r, w: proxy_handler(
            mysql_hosts=mysql_hosts,
            client_reader=r,
            client_writer=w,
            connection_timeout=connection_timeout,
            command_timeout=command_timeout,
        ),
        host,
        port,
    )
    return server


async def run_proxy_server(*args, **kwargs):
    server: socketserver.BaseServer = await create_server(*args, **kwargs)
    logger = logging.getLogger(__name__)
    logger.info("Start proxy")
    async with server:
        await server.serve_forever()


class ProxyServer:
    def __init__(
        self,
        mysql_hosts: MySQLHosts,
        host: str,
        port: int,
        connection_timeout: int,
        command_timeout: int,
    ):
        self._mysql_hosts = mysql_hosts
        self._host = host
        self._port = port
        self._connection_timeout = connection_timeout
        self._command_timeout = command_timeout
        self._servers: list[asyncio.Server] = list()

    async def serve(self):
        server: asyncio.Server = await asyncio.start_server(
            lambda r, w: proxy_handler(
                mysql_hosts=self._mysql_hosts,
                client_reader=r,
                client_writer=w,
                connection_timeout=self._connection_timeout,
                command_timeout=self._command_timeout,
            ),
            self._host,
            self._port,
        )
        await asyncio.sleep(0.1)
        self._servers.append(server)

    async def shutdown(self):
        for server in self._servers:
            server.close()

        for server in self._servers:
            await server.wait_closed()

        await asyncio.sleep(0.1)
