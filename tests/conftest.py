import asyncio

import pytest
from src.log import init_log

init_log()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop

    loop.close()
