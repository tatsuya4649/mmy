import asyncio

from src.mysql.proxy_connection import proxy_connect


async def main():
    c = proxy_connect(
        key="hello",
        user="root",
        password="root",
        db="test",
    )
    async with c as conn:
        await conn.ping()


asyncio.run(main())
