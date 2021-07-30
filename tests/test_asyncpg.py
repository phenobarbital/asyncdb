import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path

@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

asyncpg_url = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"

PARAMS = {
    "host": '127.0.0.1',
    "port": '5432',
    "user": 'troc_pgdata',
    "password": '12345678',
    "database": 'navigator_dev'
}

@pytest.fixture
async def conn(event_loop):
    db = AsyncDB('pg', dsn=asyncpg_url, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

@pytest.fixture
async def pooler(event_loop):
    pool = AsyncPool('pg', dsn=asyncpg_url, loop=event_loop)
    await pool.connect()
    yield pool
    await pool.wait_close(gracefully=True, timeout=5)

pytestmark = pytest.mark.asyncio

async def test_pool_by_dsn(event_loop):
    """ test creation using DSN """
    pool = AsyncPool("pg", dsn=asyncpg_url, loop=event_loop)
    assert pool.application_name == 'Navigator'

async def test_pool_by_params(event_loop):
    pool = AsyncPool("pg", params=PARAMS, loop=event_loop)
    assert pool.get_dsn() == asyncpg_url

async def test_changing_app(event_loop):
    """ Change the Application Name on connect """
    args = {
        "server_settings": {
            "application_name": "Testing"
        }
    }
    p = AsyncPool("pg", params=PARAMS, loop=event_loop, **args)
    assert p.application_name == 'Testing'

async def test_pool_connect(event_loop):
    pool = AsyncPool("pg", params=PARAMS, loop=event_loop)
    pytest.assume(pool.application_name == 'Navigator')
    await pool.connect()
    pytest.assume(pool.is_connected() == True)
    db = await pool.acquire()
    pytest.assume(db.is_connected() == True)
    result = await pool.execute("SELECT 1")
    pytest.assume(result == 'SELECT 1')
    result, error = await db.test_connection()
    row = result[0]
    pytest.assume(row[0] == 1)
    await pool.release(connection=db)

async def test_connection(conn):
    await conn.connection()
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection()
    row = result[0]
    pytest.assume(row[0] == 1)
    prepared, error = await conn.prepare("SELECT store_id, store_name FROM walmart.stores")
    pytest.assume(conn.get_columns() == ["store_id", "store_name"])
    assert not error

async def test_huge_query(pooler):
    sql = 'SELECT * FROM trocplaces.stores'
    check_conn = None
    async with await pooler.acquire() as conn:
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        pytest.assume(not error)
        result, error = await conn.query(sql)
        pytest.assume(not error)
        pytest.assume(result is not None)
        check_conn = conn
    pytest.assume(check_conn is not None)


@pytest.mark.parametrize("passed, expected", [
    (20, 1048576),
    (5, 32),
    (1, 2),
    (0, 1),
    (10, 1024)
])
async def test_prepared(conn, passed, expected):
    async with await conn.connection() as conn:
        prepared, error = await conn.prepare("""SELECT 2 ^ $1""")
        result = await prepared.fetchval(passed)
        assert result == expected


@pytest.mark.parametrize("moveto, fetched, count, first, last", [
    (10, 5, 5, 11, 15),
])
async def test_cursor(conn, moveto, fetched, count, first, last):
    async with await conn.connection() as conn:
        async with await conn.cursor("SELECT generate_series(0, 100) as serie") as cur:
            await cur.forward(moveto)
            row = await cur.fetchrow()
            pytest.assume(row['serie'] == moveto)
            rows = await cur.fetch(fetched)
            pytest.assume(len(list(rows)) == count)
            pytest.assume(rows[0]['serie'] == first)
            pytest.assume(rows[-1]['serie'] == last)

test_table = """ CREATE TABLE IF NOT EXISTS test.stores
(
  store_id integer NOT NULL,
  store_name character varying(60),
  CONSTRAINT test_stores_pkey PRIMARY KEY (store_id)
)
WITH (
  OIDS=FALSE
); """

async def test_cicle(conn):
    """ Test all cicle since: table creation, insertion, update, truncate and drop """
    async with await conn.connection() as conn:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS test")
        result, error = await conn.execute(test_table)
        pytest.assume(not error)
        # create a store list:
        stores, error = await conn.query(
            "SELECT store_id, store_name FROM walmart.stores"
        )
        st = [(k, v) for k,v in stores]
        # check the prepared sentences:
        pytest.assume(len(st) == 1524)
        error = await conn.executemany(
            "INSERT INTO test.stores (store_id, store_name) VALUES ($1, $2)", st
        )
        pytest.assume(not error)
        # checking integrity:
        result, error = await conn.queryrow('SELECT count(*) as count FROM test.stores')
        pytest.assume(len(st) == result['count'])
        # testing the cursor iterator:
        #iterate a cursor:
        rows = []
        async for record in await conn.cursor(
            "SELECT store_id, store_name FROM test.stores"
        ):
            rows.append(record['store_id'])
        pytest.assume(len(rows) == 1524)
        # truncate the table
        result, error = await conn.execute("DELETE FROM test.stores")
        pytest.assume(not error)
        # check the copy from array
        result = await conn.copy_into_table(
            table="stores",
            schema="test",
            columns=["store_id", "store_name"],
            source=st,
        )
        pytest.assume(result == 'COPY 1524')
        ## copying into a file-like object:
        file = BytesIO()
        file.seek(0)
        result = await conn.copy_from_table(
            table="stores",
            schema="test",
            columns=["store_id", "store_name"],
            output=file,
        )
        pytest.assume(result and file is not None)
        result, error = await conn.execute('DROP TABLE test.stores')
        pytest.assume(result == 'DROP TABLE')
        # check if really dropped
        result, error = await conn.query('SELECT * FROM test.stores')
        pytest.assume(error)

async def test_copy_to_table(conn):
    """ test copy to table functionality """
    async with await conn.connection() as conn:
        result, error = await conn.execute(test_table)
        pytest.assume(not error)
        file = 'stores.csv'
        filepath = Path(__file__).resolve().parent
        filepath = filepath.joinpath(file)
        result = await conn.copy_to_table(
            table = 'stores',
            schema = 'test',
            columns = [ 'store_id', 'store_name'],
            source = filepath
        )
        pytest.assume(result == 'COPY 1470')
        result, error = await conn.execute('DROP TABLE test.stores')
        pytest.assume(result == 'DROP TABLE')
