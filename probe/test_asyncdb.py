# -*- coding: utf-8 -*-
from asyncdb import AsyncDB, AsyncPool
from asyncdb.meta import asyncORM
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

asyncpg_url = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"

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
    return asyncORM(db=db)

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
        await c.copy_from_table(
            table="stores",
            schema="walmart",
            columns=["store_id", "store_name"],
            output="stores.csv",
        )
        # copy from file to table
        # TODO: repair error io.UnsupportedOperation: read
        # await c.copy_to_table(table = 'stores', schema = 'test', columns = [ 'store_id', 'store_name'], source = '/home/jesuslara/proyectos/navigator-next/stores.csv')
        # copy from asyncpg records
        # try:
        #     await c.copy_into_table(
        #         table="stores",
        #         schema="test",
        #         columns=["store_id", "store_name"],
        #         source=stores,
        #     )
        # except (StatementError, ProviderError) as err:
        #     print(str(err))
        #     return False


async def prepared(p):
    async with await p.connection() as conn:
        prepared, error = await conn.prepare("""SELECT 2 ^ $1""")
        print(await prepared.fetchval(10))
        print(await prepared.fetchval(20))


if __name__ == '__main__':
    try:
        a = sharing_token('67C1BEE8DDC0BB873930D04FAF16B338F8CB09490571F8901E534937D4EFA8EE33230C435BDA93B7C7CEBA67858C4F70321A0D92201947F13278F495F92DDC0BE5FDFCF0684704C78A3E7BA5133ACADBE2E238F25D568AEC4170EB7A0BE819CE8F758B890855E5445EB22BE52439FA377D00C9E4225BC6DAEDD2DAC084446E7F697BF1CEC129DFB84FA129B7B8881C66EEFD91A0869DAE5D71FD5055FCFF75')
        print(a.columns())
        # # test: first with db connected:
        e = AsyncDB("pg", dsn=asyncpg_url, loop=loop)
        loop.run_until_complete(connect(e))
        # loop.run_until_complete(prepared(e))
    finally:
        pool.terminate()
