import asyncio
from asyncdb import AsyncDB


DRIVER = 'influx'


async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('IS CONNECTED> ', db.is_connected() is True)
    await db.drop_database('testdb')
    await db.create_database('testdb')
    result, error = await db.test_connection()
    print(' == HEALTH == ')
    print(result, '/ Error: ', error)
    print(isinstance(result, dict))
    result, error = await db.query(
        'from(bucket:"navigator")|> range(start: -180m)|> filter(fn: (r) => r["_measurement"] == "task_execution") |> pivot(rowKey: ["_time"], columnKey: ["task"], valueColumn: "_value")',
        frmt='recordset'
    )
    # |> keep(columns: ["_time"])
    print('HERE ', result, error)
    await db.close()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        p = {
            "host": "127.0.0.1",
            "port": "8086",
            "user": "troc_pgdata",
            "password": "12345678",
            "org": "navigator",
            "bucket": "navigator",
            "token": "qroJLmcrjM-IsDhxz-nR_NIoUxpjAgDz9AuXJJlTnikCIr70CNa_IxXlO5BID4LVrpHHCjzzeSr_UZab5ON_9A=="
        }
        loop.run_until_complete(
            test_connect(DRIVER, params=p, event_loop=loop)
        )
    except Exception as err:
        print(err)
    finally:
        loop.close()
