import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

from asyncdb import AsyncDB, AsyncPool
from asyncdb.providers.pg import pg, pgPool

# from asyncdb.providers.sa import sa
# create a pool with parameters
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


db = loop.run_until_complete(pool.acquire())
loop.run_until_complete(pool.release(connection=db.get_connection()))
pool.terminate()

# running new multi-threaded async SA (using aiopg)
args = {
    "server_settings": {
        "application_name": "Testing"
    }
}
p = AsyncDB("pg", params=params, **args)
print(p)
loop.run_until_complete(p.connection())
print('HERE: ', p, not(p._connection.is_closed()), p.is_connected())
sql = "SELECT * FROM troc.query_util WHERE query_slug = '{}'".format("walmart_stores")

# result, error = loop.run_until_complete(p.query(sql))
# print(result)


# async def cursor(d):
#     async for record in await d.cursor(
#         "SELECT store_id, store_name FROM walmart.stores"
#     ):
#         print(record)
#
#
# # loop.run_until_complete(cursor(p))


async def connect(c):
    async with await c.connection() as conn:
        newloop = conn.get_loop()  # get the running loop
        print(conn, conn.is_connected())
        result, error = await conn.test_connection()
        print(result)
        result, error = await conn.query(sql)
        if not error:
            print(result)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)


loop.run_until_complete(connect(p))

# call closing provider
p.terminate()
#
# from sqlalchemy import (Column, Integer, MetaData, Table, Text, create_engine,
#                         select)
# from sqlalchemy.schema import CreateTable, DropTable
#
# ## In-memory sqlite database cannot be accessed from different threads
# sqlite = "sqlite:///test.db"
# sa = AsyncDB("sa", loop=loop, dsn=sqlite)
#
# metadata = MetaData()
# users = Table(
#     "users",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("name", Text),
# )
#
#
# async def insert_users(u):
#     # Insert some users
#     await u.execute(users.insert().values(name="Jeremy Goodwin"))
#     await u.execute(users.insert().values(name="Natalie Hurley"))
#     await u.execute(users.insert().values(name="Dan Rydell"))
#     await u.execute(users.insert().values(name="Casey McCall"))
#     await u.execute(users.insert().values(name="Dana Whitaker"))
#
#
# with sa.connection() as conn:
#     newloop = conn.get_loop()  # get the running loop
#     result, error = conn.test_connection()
#     # Create the table
#     result, error = newloop.run_until_complete(conn.execute(CreateTable(users)))
#     # Insert some users
#     newloop.run_until_complete(insert_users(conn))
#     result, error = newloop.run_until_complete(
#         conn.query(users.select(users.c.name.startswith("D")))
#     )
#     print(result)
#     # Drop the table
#     result, error = newloop.run_until_complete(conn.execute(DropTable(users)))
