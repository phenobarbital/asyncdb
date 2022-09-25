import asyncio
import logging
from asyncdb.drivers.hazel import hazel, HazelPortable

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Customer(HazelPortable):
    name: str
    age: int
    is_active: bool


async def test_connect(params, event_loop):
    hz = hazel(params=params, loop=event_loop)
    async with await hz.connection() as conn:
        print(f"Is Connected: {conn.is_connected()}")
        print('CONN ', conn)
        await conn.set("Test2", "No More Sweet Music")
        value = await conn.get("Test2")
        print(value)

        await conn.set_multi('Test3', {"Test2": "Testing 2"}, {"Test3": "Testing 3"})

        values = await conn.get_multi("Test3")
        print('VALUES: ', values)

        await conn.delete("Test2")
        # delete all
        await conn.delete_multi("Test2", "Test3")

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        params = {"host": "172.17.0.2", "port": 5701, "factories": [Customer]}
        loop.run_until_complete(test_connect(params, loop))
    finally:
        loop.stop()
