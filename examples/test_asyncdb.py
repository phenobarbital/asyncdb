# -*- coding: utf-8 -*-
from asyncdb import AsyncDB, AsyncPool
from asyncdb.exceptions import NoDataFound, ProviderError, StatementError

"""
for this test needs to create:
    * a schema called test
       CREATE SCHEMA test;
    * a table called stores:
        CREATE TABLE test.stores
        (
          store_id integer NOT NULL,
          store_name character varying(60),
          CONSTRAINT test_stores_pkey PRIMARY KEY (store_id)
        )
        WITH (
          OIDS=FALSE
        );
        ALTER TABLE  test.stores
          OWNER TO "troc-pgdata";
"""

import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

asyncpg_url = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator"

pool = AsyncPool("pg", dsn=asyncpg_url, loop=loop)
loop.run_until_complete(pool.connect())
db = loop.run_until_complete(pool.acquire())
loop.run_until_complete(pool.release(connection=db.get_connection()))

print(db, type(db))

result = loop.run_until_complete(pool.execute("SELECT 1"))
print(result)

def adb():
    #loop = asyncio.get_event_loop()
    db = None
    if pool.is_connected():
        #db = asyncio.get_running_loop().run_until_complete(dbpool.acquire())
        db = loop.run_until_complete(pool.acquire())
    return db

def sharing_token(token):
    db = adb()
    try:
        token = db.table('troc.tokens').filter(key=token).fields(['id', 'created_at', 'key', 'expiration']).fetchrow()
        return token
    except Exception as e:
        print(f'Unknown Token on Middleware: {e}')
        return False
    finally:
        db.close()


async def connect(c):
    async with await c.connection() as conn:
        await conn.test_connection()
        prepared, error = await conn.prepare("SELECT * FROM walmart.stores")
        print(conn.get_columns())
        if error:
            print(error)
        # get a query
        stores, error = await conn.query(
            "SELECT store_id, store_name FROM walmart.stores"
        )
        st = [(k, v) for k,v in stores]
        print(st)
        # print(result)
        # execute a sentence
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        print(result)
        # executing many sentences
        # st = [(1, "Test 1"), (2, "Test 2"), (3, "Test 3")]
        # error = await conn.executemany(
        #     "INSERT INTO test.stores (store_id, store_name) VALUES ($1, $2)", *st
        # )
        # print("DELETING ROWS")
        # await conn.execute("TRUNCATE test.stores")
        # # working with a cursor
        async with await c.cursor("SELECT generate_series(0, 100) as serie") as cur:
            await cur.forward(10)
            print(await cur.fetchrow())
            print(await cur.fetch(5))
        # iterate a cursor:
        # async for record in await c.cursor(
        #     "SELECT store_id, store_name FROM walmart.stores"
        # ):
        #     print(record)
        # working with a transaction
        async with await c.transaction() as t:
            result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
            await t.commit()
        # table copy
        async with await c.transaction() as t:
            await t.copy_from_table(
                table="stores",
                schema="walmart",
                columns=["store_id", "store_name"],
                output="stores.csv",
            )
        # copy from file to table
        # TODO: repair error io.UnsupportedOperation: read
        async with await t.transaction() as t:
            await t.copy_to_table(table = 'stores', schema = 'test', columns = [ 'store_id', 'store_name'], source = '/home/jesuslara/proyectos/navigator-next/stores.csv')
            # copy from asyncpg records
            try:
                await c.copy_into_table(
                    table="stores",
                    schema="test",
                    columns=["store_id", "store_name"],
                    source=stores,
                )
            except (StatementError, ProviderError) as err:
                print(str(err))
                return False


async def prepared(p):
    async with await p.connection() as conn:
        prepared, error = await conn.prepare("""SELECT 2 ^ $1""")
        print(await prepared.fetchval(10))
        print(await prepared.fetchval(20))


if __name__ == '__main__':
    try:
        # # test: first with db connected:
        e = AsyncDB("pg", dsn=asyncpg_url, loop=loop)
        loop.run_until_complete(connect(e))
        # loop.run_until_complete(prepared(e))
    finally:
        loop.close()
