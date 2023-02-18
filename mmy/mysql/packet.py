import asyncio
import struct
from dataclasses import dataclass

from ..const import SYSTEM_NAME
from .errcode import MySQLErrorCode
from .hosts import MySQLHostBroken
from .proxy_err import MmyError


def create_error_packet(
    error_code: int,
    sql_state: str,
    error_message: str,
    sequence_id: int,
):
    error_message_bytes: bytes = error_message.encode("utf-8")
    payload: bytes = struct.pack(
        f"<BHs5s{len(error_message_bytes)}s",
        0xFF,
        error_code,
        "#".encode("utf-8"),
        sql_state.encode("utf-8"),
        error_message_bytes,
    )
    _payload_length: bytes = struct.pack("<i", len(payload))[:3]
    if sequence_id < 0 or sequence_id > 255:
        raise ValueError
    _sequence_id: bytes = struct.pack("<B", sequence_id)
    _header = _payload_length + _sequence_id
    return _header + payload


@dataclass
class ErrorPacket:
    error_code: MySQLErrorCode
    error_message: str


async def send_error_packet_to_client(
    client_writer: asyncio.StreamWriter,
    sequence_id: int,
    err: MmyError,
):
    try:
        _add_system_name: str = f"[{SYSTEM_NAME}({err.errno()})] " + str(err)
        _mysql_error = err.mysql_error()
        packet: bytes = create_error_packet(
            error_code=_mysql_error.value.error_code,
            error_message=_add_system_name,
            sql_state=_mysql_error.value.sql_state.value,
            sequence_id=sequence_id,
        )
        client_writer.write(packet)
        await client_writer.drain()
    finally:
        client_writer.close()
        raise err


@dataclass
class BrokenHostPacket:
    pass


async def send_host_broken_packet(
    client_writer: asyncio.StreamWriter,
):
    """
    Non MySQL client/server protocol. This is mmy protocol.
    Notify what destination of MySQL server is broken to client
    """
    try:
        """
        * Payload length: 0byte
        * Sequence ID: 255
        """
        _header: bytes = struct.pack(f"<BHB", 0, 0, 255)
        client_writer.write(_header)
        await client_writer.drain()
    finally:
        client_writer.close()
        raise MySQLHostBroken
