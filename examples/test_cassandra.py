import asyncio
from asyncdb import AsyncDB

DRIVER='cassandra'


async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    print('CONNECTED: ', db.is_connected() is True)
    result, error = await db.test_connection()
    print(result, error)
    print(type(result) == list)
    await db.use('library')  # set database to work
    # making a simple query:
    result, error = await db.query('SELECT * from events LIMIT 100')
    print(error)
    for row in result:
        print(row)
    # creation and insertion:
    await db.execute("CREATE TABLE tests (id int, name text, PRIMARY KEY(id))")
    ql = "INSERT INTO tests (id, name) VALUES(?, ?)"
    # prepare the statement:
    prepared, error = await db.prepare(ql)
    print(": prepare the statement: ", prepared, error)
    print(": Executing Insert of many entries: ")
    result, error = await db.execute(prepared, (2, "def"))
    result, error = await db.execute(prepared, (3, "ghi"))
    result, error = await db.execute(prepared, (4, "jkl"))
    examples = [(5, "mno"), (6, "pqr"), (7, "stu")]
    result, error = await db.execute_many(prepared, examples)
    print(result, error)
    result, error = await db.query("SELECT * FROM tests")
    for row in result:
        print(row)
    # getting column info from Table:
    columns = await db.column_info(table='tests', schema='library')
    print('COLUMNS ', columns)
    # using row factories
    result, error = await db.query('SELECT * from events LIMIT 10000', factory='pandas')
    print(result.result)
    result, error = await db.query('SELECT * from events LIMIT 10000', factory='recordset')
    print(result.result)
    # # output formats
    # db.output_format('json')  # change output format to json
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # print(result)

    # db.output_format('pandas')  # change output format to pandas
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # print(result)

    # db.output_format('polars')  # change output format to iter generator
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # print(result)
    # # change output format to iter generator
    # db.output_format('datatable')
    # # TODO: error when a python list is on a column
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # print(result)
    # print(type(result))
    # db.output_format('record')   # change output format to iter generator
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # print(type(result))
    # # testing Recordset Object
    # db.output_format('recordset')  # change output format to ResultSet
    # result, error = await db.query('SELECT * from events LIMIT 10000')
    # for row in result:
    #     print(row)
    await db.execute('DROP TABLE tests')
    await db.close()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        params = {
            "host": "127.0.0.1",
            "port": "9042",
            "username": 'cassandra',
            "password": 'cassandra'
        }
        loop.run_until_complete(
            test_connect(DRIVER, params, loop)
        )
    except Exception as err:
        print('Error: ', err)
    finally:
        loop.close()
