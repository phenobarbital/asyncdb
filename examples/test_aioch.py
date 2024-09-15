import asyncio
from asyncdb.drivers.aioch import aioch
from pprint import pprint


async def connect(db):
    async with await db.connection() as conn:
        print('HERE >>')
        pprint(await conn.test_connection())
        print('END >>')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    params = {
        "url": "http://localhost:8123/",
        "user": "default",
        "password": "u69ebsZQ",
        "database": "default"
    }
    driver = aioch(params=params, loop=loop)
    print('DRV > ', driver)
    loop.run_until_complete(connect(driver))
