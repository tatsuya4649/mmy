import ipaddress
from dataclasses import dataclass
from enum import Enum


class State(str, Enum):
    Run = "run"
    Move = "move"
    Broken = "broken"
    Unknown = "unknown"


@dataclass
class _Server:
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int


@dataclass
class Server(_Server):
    state: State


def address_from_server(_s: _Server) -> str:
    addr = "%s:%d" % (str(_s.host), _s.port)
    return addr
