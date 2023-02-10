from dataclasses import dataclass
from enum import Enum


class State(str, Enum):
    Run = "run"
    Move = "move"
    Broken = "broken"
    Unknown = "unknown"


@dataclass
class _Server:
    host: str
    port: int


@dataclass
class Server(_Server):
    state: State
