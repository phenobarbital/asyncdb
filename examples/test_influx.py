import asyncio
from influxdb_client import Point
from datetime import datetime
from asyncdb import AsyncDB


DRIVER = 'influx'

async def test_connection(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('IS CONNECTED> ', db.is_connected() is True)
    result = await db.ping()
    print('PING > ', result)
    result, error = await db.test_connection()
    print(' == HEALTH == ')
    print(result, '/ Error: ', error)
    await db.close()

async def test_data(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('IS CONNECTED> ', db.is_connected() is True)
    await db.drop_database('testdb')
    await db.create_database('testdb')
    # data:
    # record as a list
    _point1 = Point("async_m").tag("location", "Prague").field("temperature", 25.3)
    _point2 = Point("async_m").tag("location", "New York").field("temperature", 24.3)
    successfully = await db.write(bucket="testdb", data=[_point1, _point2])
    print(f" > successfully: {successfully}")
    # Record as Line Protocol
    successfully = await db.write(bucket="testdb", data="h2o_feet,location=us-west level=125i 1")
    print(f" > successfully: {successfully}")
    # Record as Dictionary
    dictionary = {
        "measurement": "h2o_feet",
        "tags": {"location": "us-west"},
        "fields": {"level": 125},
        "time": 1
    }
    successfully = await db.write(bucket="testdb", data=dictionary)
    print(f" > successfully: {successfully}")
    # record as a Point:
    point = db.point('h2o_feet', tag=["location", "us-west"], field=["level", 120], time=1)
    await db.write(bucket="testdb", data=point)
    # query points:
    result, error = await db.query(
        'from(bucket:"testdb")|> range(start: -180m)|> filter(fn: (r) => r["_measurement"] == "async_m")'
    )
    print('HERE ', result, error)
    result, error = await db.query(
        'from(bucket:"navigator")|> range(start: -180m)|> filter(fn: (r) => r["_measurement"] == "task_execution") |> pivot(rowKey: ["_time"], columnKey: ["task"], valueColumn: "_value")',
        frmt='recordset'
    )
    # |> keep(columns: ["_time"])
    print('HERE ', result, error)
    result, error = await db.query(
        'from(bucket: "navigator") |> range(start: -12h)|> filter(fn: (r) => r["_measurement"] == "task_execution") |> filter(fn: (r) => r["task"] == "daily_postpaid") |> keep(columns: ["stats"])'
        ,frmt='recordset',
        json_output=["stats"]
    )
    print(' RESULT > ', result, error)
    # Delete them:
    args = {
        "start": db.to_isoformat(datetime.utcfromtimestamp(0)),
        "stop": db.to_isoformat(datetime.now())
    }
    print('ARGS > ', args)
    deleted = await db.delete(bucket="testdb", predicate="location = \"Prague\"", **args)
    print('Deleted ', deleted)
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
            test_connection(DRIVER, params=p, event_loop=loop)
        )
        loop.run_until_complete(
            test_data(DRIVER, params=p, event_loop=loop)
        )
    except Exception as err:
        print(err)
    finally:
        loop.close()
