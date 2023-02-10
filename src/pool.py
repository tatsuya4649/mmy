import hashlib
from typing import NewType

from loguru import logger

from .server import Server, State

ServerID = NewType("ServerID", str)


class ServerPool:
    def __init__(
        self,
        _logger=None,
    ):
        self._servers: list[Server] = list()
        self._state_map: dict[State, list[Server]] = {
            State.Run: list(),
            State.Move: list(),
            State.Broken: list(),
            State.Unknown: list(),
        }
        self._id_map: dict[ServerID, Server] = dict()
        self.logger = _logger if _logger is not None else logger

    def add(self, server: Server) -> ServerID:
        _sid: ServerID = self.sid_from_address(server.host, server.port)
        if _sid in self._id_map:
            raise ValueError(
                "The server is already exists(%s:%d)" % (server.host, server.port)
            )

        self._state_map[server.state].append(server)
        self._servers.append(server)
        self._id_map[_sid] = server
        return _sid

    @classmethod
    def sid_from_address(cls, host: str, port: int) -> ServerID:
        hp: str = "%s_%d" % (host, port)
        _ssid: str = hashlib.md5(hp.encode("utf-8")).hexdigest()
        return ServerID(_ssid)

    def delete(self, sid: ServerID):
        _s: Server | None = self._id_map.get(sid)
        if _s is None:
            raise ValueError

        self._state_map[_s.state].remove(_s)
        del self._id_map[sid]
        self._servers = list(
            filter(
                lambda x: sid != self.sid_from_address(x.host, x.port), self._servers
            ),
        )


class MySQLServerPool(ServerPool):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
