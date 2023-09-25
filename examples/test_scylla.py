import asyncio
import random
import string
from asyncdb import AsyncDB

DRIVER='scylladb'
ARGS = {
    "host": "127.0.0.1",
    "port": "9042",
    "username": 'navigator',
    "password": 'navigator'
}

async def test_connection():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)

async def test_async():
    ARGS['driver'] = 'async'
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)

async def test_operations():
    db = AsyncDB(DRIVER, params=ARGS)
    await db.connection()
    print('CONNECTED: ', db.is_connected() is True)
    result, error = await db.test_connection()
    print(result, error)
    print(type(result) == list)
    await db.create_keyspace('navigator')
    await db.use('navigator')  # set database to work
    # creation and insertion:
    result, error = await db.execute(
        "CREATE TABLE IF NOT EXISTS tests (id int, name text, PRIMARY KEY(id))"
    )
    print('CREATE > ', result, error)
    ql = "INSERT INTO tests (id, name) VALUES(?, ?)"
    # prepare the statement:
    prepared = await db.prepare(ql)
    print(": prepared statement: ", prepared)
    print(": Executing Insert of many entries: ")
    result, error = await db.execute(prepared, (1, "abc"))
    result, error = await db.execute(prepared, (2, "def"))
    result, error = await db.execute(prepared, (3, "ghi"))
    result, error = await db.execute(prepared, (4, "jkl"))
    print('Execute a Many insert:: ')
    examples = [(i, ''.join(random.choices(string.ascii_lowercase, k=4))) for i in range(5, 10005)]
    result, error = await db.execute_many(prepared, examples)
    print(result, error)
    print('first select:')
    result, error = await db.query("SELECT * FROM tests")
    for row in result:
        print(row)
    # getting column info from Table:
    columns = await db.column_info(table='tests', schema='navigator')
    print('COLUMNS ', columns)
    # using row factories
    result, error = await db.query('SELECT * from tests LIMIT 10000', factory='pandas')
    print(result.result)
    result, error = await db.query('SELECT * from tests LIMIT 10000', factory='recordset')
    print(result.result)
    # output formats
    db.output_format('json')  # change output format to json
    result, error = await db.query('SELECT * from tests LIMIT 10000')
    print(result)

    db.output_format('pandas')  # change output format to pandas
    result, error = await db.query('SELECT * from tests LIMIT 10000')
    print(result)
    db.output_format('polars')  # change output format to iter generator
    result, error = await db.query('SELECT * from tests LIMIT 10000')
    print(result)
    # change output format to iter generator
    # db.output_format('datatable')
    # # TODO: error when a python list is on a column
    # result, error = await db.query('SELECT * from tests LIMIT 10000')
    # print(result)
    # print(type(result))
    db.output_format('record')   # change output format to iter generator
    result, error = await db.query('SELECT * from tests LIMIT 10000')
    print(type(result))
    # testing Recordset Object
    db.output_format('recordset')  # change output format to ResultSet
    result, error = await db.query('SELECT * from tests LIMIT 10000')
    for row in result:
        print(row)
    await db.execute('DROP TABLE tests')
    await db.close()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            test_connection()
        )
        loop.run_until_complete(
            test_async()
        )
        loop.run_until_complete(
            test_operations()
        )
    except Exception as err:
        print('Error: ', err)
    finally:
        loop.close()
