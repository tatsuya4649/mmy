import asyncio

from src.log import init_log
from src.ring import MySQLRing, Node

if __name__ == "__main__":
    init_log()
    mr = MySQLRing()

    COUNT = 10

    async def main():
        for i in range(COUNT):
            n = Node(
                host="127.0.0.%d" % (i),
                port=i,
            )
            await mr.add(n)

        mr.logring()

    asyncio.run(main())
