import asyncio
import ipaddress

from src.etcd import MySQLEtcdClient, MySQLEtcdData
from src.log import init_log
from src.server import State, _Server

etcd = MySQLEtcdClient()


async def watch():
    await etcd.watch()


async def get():
    v = await etcd.get()
    return v


async def main():
    from random import choice, randint

    _r = lambda: randint(0, 255)
    _s = list(State)
    _state = choice(_s)
    ns = _Server(
        host=ipaddress.ip_address(f"{_r()}.{_r()}.{_r()}.{_r()}"),
        port=randint(100, 10000),
    )
    await etcd.add_new_node(ns, State.Unknown)


#    await etcd.delete()


init_log()
asyncio.run(main())
