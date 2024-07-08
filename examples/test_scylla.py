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

ARGS = {
    "host": "10.10.10.93",
    "port": "9042",
    "username": '',
    "password": ''
}

async def test_connection():
    db = AsyncDB(
        DRIVER,
        params=ARGS,
        shard_awareness=True,
        whitelist=["10.10.10.93"]
    )
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)

async def test_async():
    ARGS['driver'] = 'async'
    db = AsyncDB(
        DRIVER,
        params=ARGS,
        shard_awareness=True,
        whitelist=["10.10.10.93"]
    )
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)

async def test_operations():
    db = AsyncDB(
        DRIVER,
        params=ARGS,
        shard_awareness=True,
        whitelist=["10.10.10.93"]
    )
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        result, error = await conn.test_connection()
        print(result, error)
        print(type(result) == list)
        await conn.create_keyspace('navigator')
        await conn.use('navigator')  # set database to work
        # creation and insertion:
        result, error = await db.execute(
            "CREATE TABLE IF NOT EXISTS tests (id int, name text, PRIMARY KEY(id))"
        )
        print('CREATE > ', result, error)
        ql = "INSERT INTO tests (id, name) VALUES(?, ?)"
        # prepare the statement:
        prepared = await conn.prepare(ql)
        print(": prepared statement: ", prepared)
        print(": Executing Insert of many entries: ")
        result, error = await conn.execute(prepared, (1, "abc"))
        result, error = await conn.execute(prepared, (2, "def"))
        result, error = await conn.execute(prepared, (3, "ghi"))
        result, error = await conn.execute(prepared, (4, "jkl"))
        print('Execute a Many insert:: ')
        examples = [(i, ''.join(random.choices(string.ascii_lowercase, k=4))) for i in range(5, 10005)]
        result, error = await conn.execute_many(prepared, examples)
        print(result, error)
        print('first select:')
        result, error = await conn.query("SELECT * FROM tests")
        for row in result:
            print(row)
        # getting column info from Table:
        columns = await db.column_info(table='tests', schema='navigator')
        print('COLUMNS ', columns)
        # using row factories
        result, error = await conn.query('SELECT * from tests LIMIT 10000', factory='pandas')
        print(result.result)
        result, error = await conn.query('SELECT * from tests LIMIT 10000', factory='recordset')
        print(result.result)
        # output formats
        conn.output_format('json')  # change output format to json
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        print(result)

        conn.output_format('pandas')  # change output format to pandas
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        print(result)
        conn.output_format('polars')  # change output format to iter generator
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        print(result)
        # change output format to iter generator
        conn.output_format('dt')
        # TODO: error when a python list is on a column
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        print(result)
        print(type(result))
        conn.output_format('record')   # change output format to iter generator
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        print(type(result))
        # testing Recordset Object
        conn.output_format('recordset')  # change output format to ResultSet
        result, error = await conn.query('SELECT * from tests LIMIT 10000')
        for row in result:
            print(row)
        await conn.execute('DROP TABLE tests')
        await conn.close()


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
