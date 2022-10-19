import asyncio

from asyncdb import AsyncDB
from asyncdb.drivers.pg import pgPool

# from asyncdb.providers.sa import sa
# create a pool with parameters
params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
    # "ssl": True,
    # "check_hostname": True
}

async def pooler(loop):
    pool = pgPool(loop=loop, params=params)
    await pool.connect()
    print(f"Connected: {pool.is_connected()}")
    db = await pool.acquire()
    await pool.release(connection=db.get_connection())
    async with await pool.acquire() as conn:
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)
        # execute other, long-term query:
        result, error = await conn.execute("REFRESH MATERIALIZED VIEW flexroc.vw_form_information;")
        print(result, error)
    print('Is closed: ', {db.is_connected()})
    await pool.close()


async def test_pg(loop):
    args = {
        "server_settings": {
            "application_name": "Testing"
        }
    }
    db = AsyncDB("pg", params=params, **args)
    print(db)
    async with await db.connection() as conn:
        print('HERE: ', conn, not(conn._connection.is_closed()), conn.is_connected())
        result, error = await conn.test_connection()
        print('Test: ', result, 'Error: ', error)

        sql = "SELECT * FROM troc.query_util WHERE query_slug = 'walmart_stores'"
        result, error = await conn.query(sql)
        print(result, 'Error: ', error)
        async for record in await conn.cursor(
            "SELECT store_id, store_name FROM walmart.stores"
        ):
            print('Cursor Record: ', record)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)

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

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(pooler(loop))
        loop.run_until_complete(test_pg(loop))
    finally:
        loop.stop()
