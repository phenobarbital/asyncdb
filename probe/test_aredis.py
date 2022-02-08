import asyncio
import logging
import aredis
import inspect

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


from asyncdb import AsyncDB, AsyncPool
from asyncdb.exceptions import NoDataFound, ProviderError
from asyncdb.providers.redis import redis, redisPool

redis_url = "redis://localhost:6379/3"

# pool = aredis.ConnectionPool.from_url(
#     redis_url,
#     connection_class=aredis.Connection,
#     max_connections=300,
#     connect_timeout=10,
#     decode_responses=True,
#     #max_idle_time=10,
#     retry_on_timeout=True,
#     loop=loop
# )
#
# try:
#     client = aredis.StrictRedis(
#         connection_pool=pool
#     )
#     print(isinstance(client, aredis.StrictRedis))
#     #print(loop.run_until_complete(client.info()))
#     loop.run_until_complete(client.execute_command("set", "Test1", "UltraTest"))
#     conn = client.connection_pool._available_connections[0]
#     conn1 = pool._available_connections[0]
#     assert conn == conn1
# finally:
#     pool.disconnect()
#     loop.close()

rd = AsyncPool("asyncredis", dsn=redis_url, loop=loop)
loop.run_until_complete(rd.connect())
#
# # rd = AsyncDB('redis', dsn=redis_url, loop=loop)
# # loop.run_until_complete(rd.connection())
#
#
async def test_redis(pool):
    async with pool as db:
        print("Connected: {}".format(db.is_connected()))
        await db.test_connection()
        async with await pool.acquire() as conn:
            print(await conn.test_connection('TEST', 'TEST1'))
            await conn.execute("set", "Test1", "UltraTest")
            await conn.execute("set", "my-key", "UltraKey")
            value = await conn.execute("get", "my-key")
            print("new value:", value)
            await conn.set("Test2", "No More Test")
            if await conn.m_exists("Test1", "Test2"):
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
            if await conn.exists("user") is True:
                print('USER FOUND')
                print(await conn.get_hash("user"))
                await conn.delete("user")
        for lp in range(10000):
            print(f'Test number {lp}')
            async with await pool.acquire() as cnt:
                await cnt.ping()
                await cnt.execute("set", "Test1", "UltraTest")
                await cnt.delete("Test1")
        print('Ending ...')

try:
    loop.run_until_complete(test_redis(rd))
finally:
    loop.run_until_complete(rd.close())
    loop.close()
