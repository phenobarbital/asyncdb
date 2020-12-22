import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

from asyncdb import AsyncDB, AsyncPool
from asyncdb.providers.pg import pg, pgPool


params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

# pool = AsyncPool('pg', loop=loop, params=params)
pool = pgPool(loop=loop, params=params)
loop.run_until_complete(pool.connect())
print('Pool Connected: ', pool.is_connected())
db = loop.run_until_complete(pool.acquire())
print('Is Connected: ', db.is_connected())

sql = "SELECT * FROM troc.query_util WHERE query_slug = '{}'".format("walmart_stores")

async def connect(c):
    async with await c.connection() as conn:
        print('Connection: ', conn)
        result, error = await conn.test_connection()
        print(result, error)
        result, error = await conn.query(sql)
        if not error:
            print(result)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)


async def pooler(p):
    async with await p.acquire() as conn:
        print('Connection: ', conn)
        result, error = await conn.test_connection()
        # a huge dataset:
        result, error = await conn.query('SELECT * FROM trocplaces.stores')
        if not error:
            for row in result:
                print(row)
        result, error = await conn.query('SELECT * FROM troc.dashboards')
        if not error:
            for row in result:
                print(row)

if __name__ == "__main__":
    loop.run_until_complete(connect(db))
    print('Working on huge datasets')
    loop.run_until_complete(pooler(pool))
    loop.run_until_complete(pool.wait_close(gracefully=True, timeout=5))
    loop.stop()
