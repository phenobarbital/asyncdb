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


DRIVER = 'postgres'
DSN = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"
params = {
    "host": '127.0.0.1',
    "port": '5432',
    "user": 'troc_pgdata',
    "password": '12345678',
    "database": 'navigator_dev'
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, dsn=DSN, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio


async def test_pool_by_dsn(event_loop):
    """ test creation using DSN """
    db = AsyncDB(DRIVER, dsn=DSN, loop=event_loop)
    assert db.is_connected() is False


async def test_pool_by_params(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    assert db.get_dsn() == DSN


async def test_pool_connect(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    await db.connection()
    pytest.assume(db.is_connected() == True)
    result, error = await db.execute("SELECT 1")
    print(result)
    pytest.assume(result == 'SELECT 1')
    result, error = await db.test_connection()
    print(result, error)
    row = result[0]
    pytest.assume(row[0] == 1)
    await db.close()
    assert (not db.get_connection())

async def test_connection(conn):
    await conn.connection()
    pytest.assume(conn.is_connected() == True)
    result, error = await conn.test_connection()
    row = result[0]
    pytest.assume(row[0] == 1)
    prepared, error = await conn.prepare("SELECT store_id, store_name FROM walmart.stores")
    pytest.assume(conn.get_columns() == ["store_id", "store_name"])
    assert not error

async def test_huge_query(event_loop):
    sql = 'SELECT * FROM trocplaces.stores'
    check_conn = None
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    async with await db.connection() as conn:
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
            row = await cur.getrow()
            pytest.assume(row['serie'] == moveto)
            rows = await cur.get(fetched)
            pytest.assume(len(list(rows)) == count)
            pytest.assume(rows[0]['serie'] == first)
            pytest.assume(rows[-1]['serie'] == last)
