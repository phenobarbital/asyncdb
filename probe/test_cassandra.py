from asyncdb import AsyncDB
import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

params = {
    "host": "127.0.0.1",
    "port": "9042",
    "username": 'cassandra',
    "password": 'cassandra'
}

DRIVER='cassandra'


async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('CONNECTED: ', db.is_connected() is True)
    result, error = await db.test_connection()
    print(result, error)
    print(type(result) == list)
    db.use('library')  # set database to work
    # # making a simple query:
    # result, error = await db.query('SELECT * from events')
    # print(error)
    # for row in result:
    #     print(row)
    # creation and insertion:
    await db.execute("CREATE TABLE tests (id int, name text, PRIMARY KEY(id))")
    ql = "INSERT INTO tests (id, name) VALUES(?, ?)"
    # prepare the statement:
    prepared, error = await db.prepare(ql)
    print(": prepare the statement: ", prepared)
    print(": Executing Insert of many entries: ")
    result, error = await db.execute(prepared, (2, "def"))
    result, error = await db.execute(prepared, (3, "ghi"))
    result, error = await db.execute(prepared, (4, "jkl"))
    examples = [(5, "mno"), (6, "pqr"), (7, "stu")]
    for example in examples:
        result, error = await db.execute(prepared, example)
        print(result, error)
    result, error = await db.query("SELECT * FROM tests")
    for row in result:
        print(row)
    await db.close()


if __name__ == '__main__':
    try:
        loop.run_until_complete(test_connect(DRIVER, params, loop))
    except Exception as err:
        print(err)
    finally:
        loop.close()
