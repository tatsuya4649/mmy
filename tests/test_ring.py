import logging
import random
from hashlib import md5
from typing import Generator

import pytest
from mmy.ring import MDP, Md, MySQLRing
from mmy.server import Server, State

from ._mysql import mmy_info

logger = logging.getLogger(__name__)

SAMPLE_MD5_BYTES: int = 2
SAMPLE_MD5_ROWS_COUNT: int = 256**SAMPLE_MD5_BYTES


def sample_md5_bulk(bulk_count: int) -> Generator[list[str], None, None]:
    from hashlib import md5

    _sample_md5 = md5(b"a").hexdigest()
    _sample_md5_length = len(_sample_md5)

    a = "0" * _sample_md5_length
    i = int(a, 16)
    _bytes = SAMPLE_MD5_BYTES
    _padding_length = _sample_md5_length - _bytes * 2
    res = list()
    for i in range(0, SAMPLE_MD5_ROWS_COUNT - 1):
        i += 1
        b = "{i:0{width}x}".format(i=i, width=2 * _bytes)
        b += "0" * _padding_length

        res.append(b)
        if len(res) == bulk_count:
            yield res
            res.clear()
    else:
        yield res


class TestMySQLRing:
    LOG_STEP: int = 1000

    def generate_random_ip(self):
        fn = lambda: random.randint(0, 255)
        return f"{fn()}.{fn()}.{fn()}.{fn()}"

    def sample_nodes(self, count: int, state: State = State.Run) -> list[Server]:
        res: list[Server] = list()
        for _ in range(count):
            ip = self.generate_random_ip()
            res.append(
                Server(
                    host=ip,
                    port=random.randint(0, 65535),
                    state=state,
                )
            )
        return res

    @pytest.mark.asyncio
    async def test_add(
        self,
        mmy_info,
        mocker,
    ):
        INIT_COUNT: int = 1
        NEW_COUNT: int = 3
        VNODES: int = 80
        init_nodes: list[Server] = self.sample_nodes(count=INIT_COUNT)
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )

        assert len(ring) == INIT_COUNT
        for index, new_node in enumerate(self.sample_nodes(count=NEW_COUNT)):

            async def _move(mdp: MDP):
                """

                Destination of moved data must be new_node.
                    * move data is from "from's point" to "to's point"

                """
                assert mdp._to == new_node
                assert mdp._from.start_point == mdp.start
                assert mdp._from.end_point == mdp.end
                return

            mocker.patch(
                "mmy.ring.MySQLRing._move",
                side_effect=_move,
            )
            _m = await ring.add(node=new_node)
            for move in _m:
                await move

            assert len(ring.from_to_data) > 0

        assert len(ring) == INIT_COUNT + NEW_COUNT

    @pytest.mark.asyncio
    async def test_delete(
        self,
        mmy_info,
        mocker,
    ):
        INIT_COUNT: int = 100
        VNODES: int = 80
        init_nodes: list[Server] = self.sample_nodes(count=INIT_COUNT)
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )
        assert len(ring) == INIT_COUNT

        for index, deleted_node in enumerate(init_nodes):

            async def _move(mdp: MDP):
                """

                Destination of moved data must be new_node.
                    * move data is from "from's point" to "to's point"

                """
                logger.info(mdp)
                assert mdp._from.node == deleted_node
                assert mdp._from.start_point == mdp.start
                assert mdp._from.end_point == mdp.end
                return

            mocker.patch(
                "mmy.ring.MySQLRing._move",
                side_effect=_move,
            )
            _m = await ring.delete(node=deleted_node)
            for move in _m:
                await move
            assert ring.ring_node_count() == (INIT_COUNT - (index + 1)) * VNODES

        assert len(ring) == 0

    @pytest.mark.asyncio
    async def test_add_duplicate(
        self,
        mmy_info,
        mocker,
    ):
        INIT_COUNT: int = 1
        NEW_COUNT: int = 100
        VNODES: int = 80
        init_nodes: list[Server] = self.sample_nodes(count=INIT_COUNT)
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )
        assert len(ring) == INIT_COUNT

        async def _move(mdp: MDP):
            return

        mocker.patch(
            "mmy.ring.MySQLRing._move",
            side_effect=_move,
        )
        for new_node in self.sample_nodes(count=NEW_COUNT):

            _ms = await ring.add(node=new_node)
            for _m in _ms:
                await _m

        last_point: str | None = None
        for point in ring.ring_points():
            _point, _ = point
            if last_point is None:
                last_point = _point

            assert _point >= last_point

    @pytest.mark.asyncio
    async def test_not_owner_points(
        self,
        mmy_info,
    ):
        INIT_COUNT: int = 5
        VNODES: int = 80
        init_nodes: list[Server] = self.sample_nodes(count=INIT_COUNT)

        first_node = init_nodes[0]
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )
        _ps: list[Md] = ring.not_owner_points(first_node)
        for p in _ps:
            assert p.node != first_node
