import asyncio
import struct
from dataclasses import dataclass

from ..const import SYSTEM_NAME
from .errcode import MySQLErrorCode


def create_error_packet(
    error_code: int,
    sql_state: str,
    error_message: str,
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
    _sequence_id: bytes = struct.pack("<B", 0)
    _header = _payload_length + _sequence_id
    return _header + payload


@dataclass
class ErrorPacket:
    error_code: MySQLErrorCode
    error_message: str


async def send_error_packet_to_client(
    client_writer: asyncio.StreamWriter,
    error_packet: ErrorPacket,
):
    try:
        _add_system_name: str = f"[{SYSTEM_NAME}] " + error_packet.error_message
        packet: bytes = create_error_packet(
            error_code=error_packet.error_code.value.error_code,
            error_message=_add_system_name,
            sql_state=error_packet.error_code.value.sql_state.value,
        )
        client_writer.write(packet)
        await client_writer.drain()
    finally:
        client_writer.close()
