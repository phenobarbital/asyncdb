import asyncio
import logging

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


from asyncdb import AsyncDB, AsyncPool
from asyncdb.exceptions import NoDataFound, ProviderError
from asyncdb.providers.redis import redis, redisPool

redis_url = "redis://localhost:6379/3"

rd = AsyncPool("redis", dsn=redis_url, loop=loop)
# rd = redisPool(dsn=redis_url, loop=loop)
loop.run_until_complete(rd.connect())

# rd = AsyncDB('redis', dsn=redis_url, loop=loop)
# loop.run_until_complete(rd.connection())


async def test_redis(conn):
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
    for lp in range(10000):
        print(f'Test number {lp}')
        async with await rd.acquire() as conn:
            await conn.ping()
            await conn.execute("set", "Test1", "UltraTest")
            await conn.delete("Test1")
    print('Ending ...')

try:
    print("Connected: {}".format(rd.is_connected()))
    with rd as conn:
        loop.run_until_complete(conn.execute("set", "my-key", "UltraValue"))
        value = loop.run_until_complete(conn.execute("get", "my-key"))
        print("raw value:", value)
    # adquire a new connection (with pool)
    r = loop.run_until_complete(rd.acquire())
    loop.run_until_complete(r.execute("set", "my-key", "UltraKey"))
    value = loop.run_until_complete(r.execute("get", "my-key"))
    print("new value:", value)
    loop.run_until_complete(test_redis(r))
finally:
    loop.run_until_complete(rd.close())
    loop.close()
