import asyncio
import hashlib
from dataclasses import dataclass
from typing import Literal

from dacite import Config, from_dict
from rich import print
from src.mysql import MySQLClient


async def main():
    cli = MySQLClient(
        host="127.0.0.1",
        port=10001,
        db="test",
        user="root",
        password="root",
    )
    import time

    _s = time.perf_counter()

    start = "1fe25b231d8c78985736109f297926ac"
    end = "7118f2a822e17501690438d064789f76"
    await cli.consistent_hashing_delete(
        table="user",
        start=start,
        end=end,
    )
    await cli.optimize_table(table="user")
    #    s = await cli.consistent_hashing_select(
    #        table="user",
    #        start=start,
    #        end=end,
    #    )
    #    async for frag in s:
    #        for i in frag:
    #            _b = f"{i['id']}".encode("utf-8")
    #            _hash = hashlib.md5(_b).hexdigest()
    #            assert _hash > start
    #            assert _hash < end

    _e = time.perf_counter()
    print(_e - _s)


if __name__ == "__main__":
    asyncio.run(main())
