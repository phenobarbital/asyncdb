import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
from io import BytesIO
from pathlib import Path
import pytest_asyncio
from datetime import datetime
import pandas
import polars as pl
from asyncdb.meta.record import Record
from asyncdb.meta.recordset import Recordset

DRIVER = 'pg'
DSN = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator"
PARAMS = {
    "host": '127.0.0.1',
    "port": '5432',
    "user": 'troc_pgdata',
    "password": '12345678',
    "database": 'navigator'
}


@pytest_asyncio.fixture()
async def conn(event_loop):
    db = AsyncDB(DRIVER, dsn=DSN, loop=event_loop)
    await db.connection()
    yield db
    await db.close()


@pytest_asyncio.fixture()
async def pooler(event_loop):
    args = {
        "timeout": 36000,
        "server_settings": {
            "application_name": "Navigator"
        }
    }
    pool = AsyncPool(DRIVER, dsn=DSN, loop=event_loop, **args)
    print(pool, type(pool))
    await pool.connect()
    yield pool
    await pool.wait_close(gracefully=True, timeout=10)

pytestmark = pytest.mark.asyncio


async def test_pool_by_dsn(event_loop):
    """ test creation using DSN """
    pool = AsyncPool(DRIVER, dsn=DSN, loop=event_loop)
    assert pool.application_name == 'NAV'
    assert not pool.is_connected()
    await pool.connect()
    assert pool.is_connected() is True
    await pool.wait_close(True, 5)
    assert pool.is_closed() is True


async def test_pool_by_params(event_loop):
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop)
    assert pool.get_dsn() == DSN
    assert pool.is_connected() is False
    await pool.connect()
    assert pool.is_connected() is True
    result, error = await pool.test_connection()
    assert not error
    assert result == 'SELECT 1'
    await pool.close()
    assert pool.is_closed() is True


async def test_changing_app(event_loop):
    """ Change the Application Name on connect """
    args = {
        "server_settings": {
            "application_name": "Testing"
        }
    }
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop, **args)
    assert pool.application_name == 'Testing'
    assert pool.is_closed() is True


async def test_context(pooler, event_loop):
    """ Using Pooler """
    assert pooler.application_name == 'Navigator'
    assert pooler.is_closed() is False
    async with pooler as conn:
        result = await conn.execute("SELECT 1")
        assert result == 'SELECT 1'


async def test_pool_connect(event_loop):
    args = {
        "server_settings": {
            "application_name": "Navigator"
        }
    }
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop, **args)
    assert pool.application_name == 'Navigator'
    await pool.connect()
    assert pool.is_connected() is True
    db = await pool.acquire()
    assert db.is_connected() is True
    result = await pool.execute("SELECT 1")
    assert result == 'SELECT 1'
    result, error = await pool.test_connection()
    assert not error
    assert result == 'SELECT 1'
    await pool.release(
        connection=db
    )
    async with await pool.acquire() as conn:
        assert conn.is_connected() is True
        result, error = await conn.test_connection()
        assert not error
        assert result[0][0] == 1
    await pool.wait_close()
    assert pool.is_closed() is True


async def test_connection(conn):
    assert conn.is_connected() is True
    result, error = await conn.test_connection()
    row = result[0]
    assert row[0] == 1
    prepared, error = await conn.prepare(
        "SELECT store_id, store_name FROM hisense.stores"
    )
    assert conn.get_columns() == ["store_id", "store_name"]
    assert not error


async def test_huge_query(event_loop):
    sql = 'SELECT * FROM placerai.stores LIMIT 1000'
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop)
    await pool.connect()
    assert pool.is_connected() is True
    async with await pool.acquire() as conn:
        result, error = await conn.execute("SET TIMEZONE TO 'America/New_York'")
        assert not error
        result, error = await conn.query(sql)
        assert not error
        assert result is not None
        assert len(result) == 1000
    await pool.wait_close()
    assert pool.is_closed() is True


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
            assert row['serie'] == moveto
            rows = await cur.fetch(fetched)
            assert len(list(rows)) == count
            assert rows[0]['serie'] == first
            assert rows[-1]['serie'] == last


test_table = """ CREATE TABLE IF NOT EXISTS test.stores
(
  store_id varchar NOT NULL,
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
        assert not error
        # create a store list:
        stores, error = await conn.query(
            "SELECT store_id, store_name FROM bestbuy.stores LIMIT 500"
        )
        st = [(k, v) for k, v in stores]
        # check the prepared sentences:
        assert len(st) == 500
        result, error = await conn.execute_many(
            "INSERT INTO test.stores (store_id, store_name) VALUES ($1, $2)", st
        )
        assert not error
        # checking integrity:
        result, error = await conn.queryrow('SELECT count(*) as count FROM test.stores')
        assert len(st) == result['count']
        # testing the cursor iterator:
        # iterate a cursor:
        rows = []
        async for record in await conn.cursor(
            "SELECT store_id, store_name FROM test.stores"
        ):
            rows.append(record['store_id'])
        assert len(rows) == 500
        # truncate the table
        result, error = await conn.execute("DELETE FROM test.stores")
        assert not error
        # check the copy from array
        result = await conn.copy_into_table(
            table="stores",
            schema="test",
            columns=["store_id", "store_name"],
            source=st,
        )
        assert result == 'COPY 500'
        ## copying into a file-like object:
        file = BytesIO()
        file.seek(0)
        result = await conn.copy_from_table(
            table="stores",
            schema="test",
            columns=["store_id", "store_name"],
            output=file,
        )
        assert result and file is not None
        assert result == 'COPY 500'
        # drop the table
        drop, error = await conn.execute('DROP TABLE test.stores')
        assert drop == 'DROP TABLE'
        # check if really dropped
        result, error = await conn.query('SELECT * FROM test.stores')
        assert error


async def test_copy_to_table(conn):
    """ test copy to table functionality """
    async with await conn.connection() as conn:
        result, error = await conn.execute(test_table)
        assert not error
        file = 'stores.csv'
        filepath = Path(__file__).resolve().parent
        filepath = filepath.joinpath(file)
        result = await conn.copy_to_table(
            table='stores',
            schema='test',
            columns=['store_id', 'store_name'],
            source=filepath
        )
        assert result == 'COPY 1470'
        result, error = await conn.execute('DROP TABLE test.stores')
        assert result == 'DROP TABLE'


async def test_huge_datasets(pooler):
    async with await pooler.acquire() as conn:
        # a huge dataset:
        start = datetime.now()
        rows = 0
        result, error = await conn.query('SELECT * FROM hisense.stores')
        assert not error
        rows += len(result)
        if not error:
            for row in result:
                assert row is not None
        result, error = await conn.query('SELECT * FROM navigator.dashboards')
        assert not error
        rows += len(result)
        if not error:
            for row in result:
                assert row is not None
        exec_time = (datetime.now() - start).total_seconds()
        print(f"Rows: {rows}, Execution Time {exec_time:.3f}s\n")
        assert exec_time > 0


async def test_formats(event_loop):
    db = AsyncDB('pg', params=PARAMS, loop=event_loop)
    async with await db.connection() as conn:
        assert db.is_connected() is True
        # first-format, native:
        conn.row_format('iterable')  # change output format to dict
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        assert type(result) == list
        conn.output_format('json')  # change output format to json
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        assert type(result) == str
        conn.output_format('pandas')  # change output format to pandas
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        print(result)
        assert type(result) == pandas.core.frame.DataFrame
        # change output format to iter generator
        conn.output_format('iterable')
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        print(result)
        # assert callable(result) # TODO: test method for generator exp
        conn.output_format('polars')  # change output format to iter generator
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        print(result)
        assert type(result) == pl.DataFrame
        # change output format to iter generator
        conn.output_format('csv')  # change output format to iter generator
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        assert type(result) == str
        # testing Record Object
        conn.output_format('record')   # change output format to iter generator
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        assert type(result) == list
        for row in result:
            assert type(row) == Record
        # testing Recordset Object
        conn.output_format('recordset')  # change output format to ResultSet
        result, error = await conn.query("SELECT * FROM bestbuy.stores")
        assert type(result) == Recordset
        # working with slices:
        obj = result[0:2]
        assert len(obj) == 2
        for row in result:
            assert type(row) == Record


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
