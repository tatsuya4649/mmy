import logging
import random

import pytest
from tqdm import tqdm

from mmy.ring import MDP, MySQLRing, Node

from ._mysql import mmy_info

logger = logging.getLogger(__name__)


class TestMySQLRing:
    def generate_random_ip(self):
        fn = lambda: random.randint(0, 255)
        return f"{fn()}.{fn()}.{fn()}.{fn()}"

    def sample_nodes(self, count: int) -> list[Node]:
        res: list[Node] = list()
        for _ in range(count):
            ip = self.generate_random_ip()
            res.append(
                Node(
                    host=ip,
                    port=random.randint(0, 65535),
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
        NEW_COUNT: int = 100
        VNODES: int = 80
        init_nodes: list[Node] = self.sample_nodes(count=INIT_COUNT)
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )
        assert len(ring) == INIT_COUNT

        for index, new_node in enumerate(tqdm(self.sample_nodes(count=NEW_COUNT))):

            async def _move(mdp: MDP):
                """

                Destination of moved data must be new_node.
                    * move data is from "from's point" to "to's point"

                """
                logger.info(mdp)
                assert mdp._to.node == new_node
                assert mdp._from.point == mdp.start
                assert mdp._to.point == mdp.end
                return

            mocker.patch(
                "mmy.ring.MySQLRing._move",
                side_effect=_move,
            )
            _m = await ring.add(node=new_node)
            for move in _m:
                await move
            assert ring.ring_node_count() == (INIT_COUNT + index + 1) * VNODES
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
        init_nodes: list[Node] = self.sample_nodes(count=INIT_COUNT)
        ring = MySQLRing(
            mysql_info=mmy_info.mysql,
            init_nodes=init_nodes,
            default_vnodes=VNODES,
        )
        assert len(ring) == INIT_COUNT

        for index, new_node in enumerate(tqdm(init_nodes)):

            async def _move(mdp: MDP):
                """

                Destination of moved data must be new_node.
                    * move data is from "from's point" to "to's point"

                """
                logger.info(mdp)
                assert mdp._from.node == new_node
                assert mdp._from.point == mdp.start
                assert mdp._to.point == mdp.end
                return

            mocker.patch(
                "mmy.ring.MySQLRing._move",
                side_effect=_move,
            )
            _m = await ring.delete(node=new_node)
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
        init_nodes: list[Node] = self.sample_nodes(count=INIT_COUNT)
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

            _m = await ring.add(node=new_node)
            await _m

        last_point: str | None = None
        for point in ring.ring_points():
            _point, _ = point
            if last_point is None:
                last_point = _point
                continue

            assert _point >= last_point
