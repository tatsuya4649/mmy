import asyncio

from src.etcd import MySQLEtcdClient, MySQLEtcdData
from src.server import Server, State

etcd = MySQLEtcdClient()


async def watch():
    await etcd.watch()


async def get():
    v = await etcd.get()
    return v


async def main():
    data: MySQLEtcdData = await get()
    from random import choice, randint

    _r = lambda: randint(0, 255)
    ns = Server(
        host=f"{_r()}.{_r()}.{_r()}.{_r()}",
        port=randint(100, 10000),
        state=choice(list(State)),
    )
    data.nodes.append(ns)

    await etcd.put(data)


asyncio.run(main())
