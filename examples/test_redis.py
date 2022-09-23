import asyncio
import logging
from asyncdb import AsyncDB, AsyncPool

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

redis_url = "redis://127.0.0.1:6379/3"

async def pooler(loop):
    rd = AsyncPool("redis", dsn=redis_url, loop=loop)
    await rd.connect()
    print(f"Connected: {rd.is_connected()}")
    async with await rd.acquire() as conn:
        print(conn.is_connected())
        await conn.execute("set", "my-key", "UltraValue")
        await conn.execute("get", "my-key")
        value = await conn.execute("get", "my-key")
        print("raw value:", value)
        conn.execute("set", "my-key", "UltraKey")

    for lp in range(10000):
        print(f'Test number {lp}')
        async with await rd.acquire() as conn:
            await conn.ping()
            await conn.execute("set", "Test1", "UltraTest")
            await conn.delete("Test1")


async def test_redis(loop):
    db = AsyncDB('redis', dsn=redis_url, loop=loop)
    async with await db.connection() as conn:
        print(conn.is_connected())
        result, error = await db.test_connection()
        print(result, error)
        result, error = await db.test_connection('bigtest')
        # test ping:
        await conn.ping('Test Ping')
        await conn.execute("set", "Test1", "UltraTest")
        await conn.set("Test2", "No More Test")
        if await conn.exists("Test1", "Test2"):
            value = await conn.get("Test1")
            print(value)
        await conn.setex("Test3", "Expiration Data", 10)
        await conn.persist("Test3")
        value = await conn.get("Test3")
        print(value)
        user = {
            "Name": "Pradeep",
            "Company": "SCTL",
            "Address": "Mumbai",
            "Location": "RCP",
        }
        await conn.set_hash("user", user)
        if await conn.exists("user"):
            print(await conn.get_hash("user"))
            await conn.delete("user")
    print('Closed: ', db.is_closed())
    print('Ending ...')


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        loop.run_until_complete(pooler(loop))
        loop.run_until_complete(test_redis(loop))
    finally:
        loop.stop()
