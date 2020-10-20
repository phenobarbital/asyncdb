import asyncio
import datetime

from asyncdb import AsyncDB
from asyncdb.providers.postgres import postgres

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

# create a pool with parameters
params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

pg = postgres(params=params)
newloop = pg.get_loop()

# asyncio version
async def test_connection(db):
    start = datetime.datetime.now()
    async with await db.connection() as conn:
        # get the raw connector
        types = await conn.get_connection().fetch("SELECT * FROM pg_type")
        # print(types)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)
        # execute many
        sql = "SELECT $1, $2"
        await conn.executemany(sql, [(1, 2), (3, 4), (5, 6)])
        # simple query
        sql = "SELECT * FROM troc.query_util WHERE query_slug = '{}'".format(
            "walmart_stores"
        )
        print(await conn.columns(sql))
        # basic cursors
        async for record in await conn.cursor(
            "SELECT store_id, store_name FROM walmart.stores"
        ):
            print(record)
        # basic metadata operations
        exec_time = (datetime.datetime.now() - start).total_seconds()
        print(f"Execution Time {exec_time:.3f}s\n")


# non-async version
def connection(db):
    start = datetime.datetime.now()
    with db.connect() as conn:
        print(conn.get_connection())
        result, error = conn.test_connection()
        print(result)
        # execute a sentence
        result, error = conn.perform("SET TIMEZONE TO 'America/New_York'")
        print(result)
        # simple query
        sql = "SELECT * FROM troc.query_util WHERE query_slug = '{}'".format(
            "walmart_stores"
        )
        # result, error = loop.run_until_complete(conn.query(sql))
        # for r in result:
        #     print(r)
        # get non-async version of query and queryrow
        row, error = conn.fetchone(sql)
        print(row, error)
        if not error:
            print(row)
        result, error = conn.fetchall("SELECT store_id, store_name FROM walmart.stores")
        print(result, error)
        # if result:
        #     for r in result:
        #         print(r)
        exec_time = (datetime.datetime.now() - start).total_seconds()
        print(f"Execution Time {exec_time:.3f}s\n")


async def test_db(db):
    connection(pg)


newloop.run_until_complete(test_connection(pg))
loop.run_until_complete(test_db(pg))
pg.terminate()  # closing all remaining threads and loop
loop.close()
