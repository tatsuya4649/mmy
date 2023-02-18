import asyncio

import pytest

from mmy.etcd import MmyEtcdInfo
from mmy.state import _state_main

from .test_etcd import docker_etcd_server


class TestState:
    @pytest.mark.asyncio
    async def test(self, docker_etcd_server):
        etcd_info = MmyEtcdInfo(
            hosts=docker_etcd_server,
        )
        await _state_main(
            etcd_info=etcd_info,
        )
