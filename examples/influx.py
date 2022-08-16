from asyncdb import AsyncDB, AsyncPool
import asyncio


loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

params = {
    "host": "127.0.0.1",
    "port": "8086",
    "user": "troc_pgdata",
    "password": "12345678",
    "org": "navigator",
    "bucket": "navigator",
    "token": "qroJLmcrjM-IsDhxz-nR_NIoUxpjAgDz9AuXJJlTnikCIr70CNa_IxXlO5BID4LVrpHHCjzzeSr_UZab5ON_9A=="
}

DRIVER='influx'

async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('IS CONNECTED> ', db.is_connected() is True)
    await db.create_database('testdb')
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
