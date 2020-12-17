from asyncdb import AsyncDB
import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

params = {
    "host": "127.0.0.1",
    "port": "27017",
    "username": 'troc_pgdata',
    "password": '12345678'
}

DRIVER='mongo'


async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('CONNECTED: ', db.is_connected() is True)
    result, error = await db.test_connection()
    print(result, error)
    print(type(result) == list)
    await db.close()


if __name__ == '__main__':
    try:
        loop.run_until_complete(test_connect(DRIVER, params, loop))
    except Exception as err:
        print(err)
    finally:
        loop.close()
