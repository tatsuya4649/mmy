import pytest

from e1.src.pool import MySQLServerPool, Server, ServerID, ServerPool, State


def test_server_pool():
    sp = ServerPool()

    s = Server(
        host="localhost",
        port=10000,
        state=State.Run,
    )
    sid: ServerID = sp.add(s)

    def _count(c: int):
        assert len(sp._id_map) == c
        assert len(sp._servers) == c
        assert len(sp._state_map[State.Run]) == c

    _count(1)

    sp.delete(sid)

    _count(0)


def test_add_same_server():
    sp = ServerPool()

    s = Server(
        host="localhost",
        port=10000,
        state=State.Run,
    )
    sp.add(s)
    with pytest.raises(ValueError):
        sp.add(s)


def test_mysql():
    mp = MySQLServerPool()
