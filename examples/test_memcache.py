import asyncio
import logging
from asyncdb.drivers.memcache import memcache


logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def test_memcache(params, loop):
    m = memcache(params=params, loop=loop)
    async with await m.connection() as conn:
        print(f"Connected: {conn.is_connected()}")
        result, error = await conn.test_connection()
        print('RESULT ', result, 'Error: ', error)
        await conn.set("Test2", "No More Sweet Music")
        value = await conn.get("Test2")
        print(value)
        #
        await conn.set("Test&4", "Data With Expiration", 10)
        value = await conn.get("Test&4")
        print(value)

        await conn.set_multi({"Test2": "Testing 2", "Test3": "Testing 3"})

        values = await conn.multiget("Test2", "Test3", "Test&4")
        print(values)

        await conn.delete("Test2")
        # delete all
        await conn.delete_multi("Test&4", "Test3")

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        params = {"host": "localhost", "port": 11211}
        loop.run_until_complete(
            test_memcache(params=params, loop=loop)
        )
    finally:
        loop.stop()
