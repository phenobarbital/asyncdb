import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler

async def connect(db):
    async with await db.connection() as conn:
        pprint(await conn.test_connection())
        await conn.execute('create table tests(id integer, name text)')
        many = 'INSERT INTO tests VALUES(?, ?)'
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(': Executing Insert of many entries: ')
        await conn.executemany(many, examples)
        result, error = await conn.query('SELECT * FROM tests')
        for row in result:
            print(row)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(default_exception_handler)
    driver = AsyncDB('sqlite', params = { "database": ':memory:' }, loop=loop)
    asyncio.run(connect(driver))
