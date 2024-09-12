import asyncio
import datetime
from asyncdb import AsyncDB
from asyncdb.drivers.sa import sa


# create a pool with parameters
params = {
    "driver": "postgresql",
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator"
}
dsn = "postgresql+asyncpg://troc_pgdata:12345678@127.0.0.1:5432/navigator"

# asyncio version
async def test_connection(loop):
    start = datetime.datetime.now()
    db = AsyncDB('sa', dsn=dsn, loop=loop)
    async with await db.connection() as conn:
        # get the raw connector
        print('Connected: ', conn.is_connected())
        result, error = await conn.test_connection()
        print('TEST: ', result, error)
        conn.row_format('dict')
        result, error = await conn.test_connection()
        print('TEST: ', result, error)
        conn.row_format('record')
        result, error = await conn.test_connection()
        print('TEST: ', result, error)
        async with conn.get_connection() as cn:
            query = conn.text('SELECT * FROM pg_type')
            result = await cn.execute(query)
            types = result.fetchall()
        # print(types)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result, 'Error: ', error)
        # execute many
        sql = "SELECT $1, $2"
        await conn.executemany(sql, [(1, 2), (3, 4), (5, 6)])
        # basic cursors
        async with conn.cursor(
            "SELECT store_id, store_name FROM walmart.stores"
        ) as cursor:
            async for record in cursor:
                print(record)
        # basic metadata operations
        exec_time = (datetime.datetime.now() - start).total_seconds()
        print(f"Execution Time {exec_time:.3f}s\n")
        # a very very huge dataset:
        print('=== GETTING EPSON SALES === ')
        start = datetime.datetime.now()
        result, error = await conn.query(
            'SELECT * FROM epson.sales LIMIT 1000'
        )
        print(len(result))
        exec_time = (datetime.datetime.now() - start).total_seconds()
        print(f"Execution Time {exec_time:.3f}s\n")
        # Fetch One, Fetch many, FetchVal
        row, error = await conn.queryrow(
            'SELECT * FROM epson.sales LIMIT 1'
        )
        print(row)
        result = await conn.fetchmany(
            'SELECT * FROM epson.sales LIMIT 1000',
            size=2
        )
        print(len(result))

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test_connection(loop))
    finally:
        loop.close()
