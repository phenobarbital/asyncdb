import asyncio
from asyncdb import AsyncDB
from asyncdb.drivers.mongo import mongo

params = {
    "host": "127.0.0.1",
    "port": "27017",
    "username": 'troc_pgdata',
    "password": '12345678',
    "database": "navigator"
}

async def test_connect(params):
    db = AsyncDB('mongo', params=params)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)


async def check_connection():
    async with mongo(
        params=params
    ) as db_driver:
        is_connected = await db_driver.test_connection()
        if is_connected:
            print("Successfully connected to MongoDB.")
        else:
            print("Failed to connect to MongoDB.")


if __name__ == '__main__':
    asyncio.run(test_connect(params))
    asyncio.run(check_connection())
