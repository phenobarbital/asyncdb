import asyncio
import logging

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

from asyncdb import AsyncDB, AsyncPool
from asyncdb.providers.memcache import memcache, memcachePool

params = {"host": "localhost", "port": 11211}

# mem = AsyncDB('memcache', loop=loop, params=params)
# mem = memcache(loop=loop, params=params)
mp = memcachePool(loop=loop, params=params)
loop.run_until_complete(mp.connect())


async def test_memcache(conn):
    await conn.set("Test2", "No More Test")
    value = await conn.get("Test2")
    print(value)
    await conn.set("Test3", "Expiration Data", 10)
    value = await conn.get("Test3")
    print(value)
    values = await conn.multiget("Test2", "Test3")
    print(values)
    await conn.delete("Test2")


try:
    m = loop.run_until_complete(mp.acquire())
    print("Connected: {}".format(m.is_connected()))
    loop.run_until_complete(test_memcache(m))
finally:
    loop.run_until_complete(mp.close())
    loop.close()
