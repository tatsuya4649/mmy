import asyncio

from src.mysql.proxy_connection import connect


async def main():
    c = connect(
        key="hello",
        user="root",
        password="root",
        db="test",
    )
    async with c as conn:
        await conn.ping()


asyncio.run(main())
