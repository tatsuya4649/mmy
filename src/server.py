from dataclasses import dataclass
from enum import Enum


class State(Enum):
    Run = 0
    Move = 1
    Broken = 2
    Unknown = 3


@dataclass
class _Server:
    host: str
    port: int


@dataclass
class Server(_Server):
    state: State
