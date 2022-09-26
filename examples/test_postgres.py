import asyncio
import datetime
from asyncdb import AsyncDB
from asyncdb.drivers.postgres import postgres


# create a pool with parameters
params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}
asyncpg_url = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"

# def adb():
#     try:
#         db = AsyncDB('postgres', dsn=asyncpg_url)
#         if db:
#             db.connect()
#         return db
#     except Exception as err:
#         raise Exception(err)

# def sharing_token(token):
#     db = adb()
#     try:
#         token = db.table('troc.tokens').filter(key=token).fields(['id', 'created_at', 'key', 'expiration']).one()
#         return token
#     except Exception as e:
#         print(f'Unknown Token on Middleware: {e}')
#         return False
#     finally:
#         db.close()

# asyncio version
async def test_connection(loop):
    start = datetime.datetime.now()
    db = AsyncDB('postgres', dsn=asyncpg_url, loop=loop)
    async with await db.connection() as conn:
        # get the raw connector
        types = await conn.get_connection().fetch("SELECT * FROM pg_type")
        print(types)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result, 'Error: ', error)
        # execute many
        sql = "SELECT $1, $2"
        await conn.executemany(sql, [(1, 2), (3, 4), (5, 6)])
        # simple query
        sql = "SELECT * FROM troc.query_util WHERE query_slug = 'walmart_stores'"
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
def connection():
    start = datetime.datetime.now()
    db = postgres(params=params)
    with db.connect() as conn:
        # execute a sentence
        result, error = conn.perform("SET TIMEZONE TO 'America/New_York'")
        print(result)
        # simple query
        sql = "SELECT * FROM troc.query_util WHERE query_slug = 'walmart_stores'"
        # get non-async version of query and queryrow
        row, error = conn.fetchone(sql)
        print(row, error)
        if not error:
            print(row)
        result, error = conn.fetchall("SELECT store_id, store_name FROM walmart.stores")
        print(result, error)
        exec_time = (datetime.datetime.now() - start).total_seconds()
        print(f"Execution Time {exec_time:.3f}s\n")


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test_connection(loop))
        print('Now: not async')
        connection()
        # newloop.run_until_complete(test_connection(pg))
        # loop.run_until_complete(test_db(pg))
        # a = sharing_token('67C1BEE8DDC0BB873930D04FAF16B338F8CB09490571F8901E534937D4EFA8EE33230C435BDA93B7C7CEBA67858C4F70321A0D92201947F13278F495F92DDC0BE5FDFCF0684704C78A3E7BA5133ACADBE2E238F25D568AEC4170EB7A0BE819CE8F758B890855E5445EB22BE52439FA377D00C9E4225BC6DAEDD2DAC084446E7F697BF1CEC129DFB84FA129B7B8881C66EEFD91A0869DAE5D71FD5055FCFF75')
        # print(a.columns(), a.created_at)
        # test: first with db connected:
        # e = AsyncDB("pg", dsn=asyncpg_url, loop=loop)
        # loop.run_until_complete(connect(e))
        # loop.run_until_complete(prepared(e))
    finally:
        loop.close()
