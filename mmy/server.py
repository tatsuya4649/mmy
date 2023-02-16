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

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port

    def address_format(self) -> str:
        return address_from_server(self)


@dataclass
class Server(_Server):
    state: State


def address_from_server(_s: _Server) -> str:
    addr = "%s:%d" % (str(_s.host), _s.port)
    return addr


def state_rich_str(state: State) -> str:
    match state:
        case State.Run:
            return f"[bold turquoise2]{state.value}[/bold turquoise2]"
        case State.Move:
            return f"[bold chartreuse3]{state.value}[/bold chartreuse3]"
        case State.Broken:
            return f"[bold red]{state.value}[/bold red]"
        case State.Unknown:
            return f"[bold white]{state.value}[/bold white]"
