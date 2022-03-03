import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio
import pandas
import datatable as dt
from asyncdb.meta import Record, Recordset

DRIVER = 'sqlserver'
params = {
    "server": "localhost",
    "port": "1433",
    "database": 'AdventureWorks2019',
    "user": 'sa',
    "password": 'P4ssW0rd1.'
}
server_params = {
    "server": "localhost",
    "port": "3307",
    "database": 'PRD_MI',
    "user": 'navuser',
    "password": 'L2MeomsUgYpFBJ6t'
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect_by_params(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    assert await db.connection()
    assert db.is_connected() is True
    await db.close()


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(type(result) == dict)
    pytest.assume(result['one'] == 1)
    await db.close()


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_context_connection(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.use('AdventureWorks2019')
        result, error = await conn.query(
            'SELECT TOP (1000) * FROM "Person"."Person"'
        )
        pytest.assume(not error)
        pytest.assume(type(result) == list)
        pytest.assume(len(result) == 1000)
        for row in result:
            pytest.assume(type(row) == dict)
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_context_fetch_row(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.use('AdventureWorks2019')
        result, error = await conn.queryrow(
            "SELECT * FROM Person.Person WHERE FirstName = 'Ken' and LastName = 'Sánchez' ORDER BY BusinessEntityID"
        )
        pytest.assume(not error)
        pytest.assume(type(result) == dict)
        pytest.assume(result['MiddleName'] == 'J')
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_context_native_methods(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    sql = "SELECT * FROM Person.Person WHERE FirstName = 'Ken' ORDER BY BusinessEntityID"
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.use('AdventureWorks2019')
        result = await conn.fetch_all(
            sql
        )
        pytest.assume(type(result) == list)
        pytest.assume(len(result) == 6)
        result = await conn.fetch_one(
            sql
        )
        pytest.assume(type(result) == dict)
        pytest.assume(result['MiddleName'] == 'J')
        result = await conn.fetch(
            sql, size=2
        )
        pytest.assume(type(result) == list)
        pytest.assume(len(result) == 2)
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_procedure(driver, event_loop):
    db = AsyncDB(driver, params=server_params, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.use('PRD_MI')
        result, error = await conn.procedure(
            "VIBA_ENDPOINT_GET_LIST",
            orgid=93, user_id=1
        )
        pytest.assume(not error)
        pytest.assume(type(result) == list)
        pytest.assume(len(result) > 0)
    assert db.is_closed() is True


async def test_formats(conn):
    pytest.assume(conn.is_connected() is True)
    conn.use('AdventureWorks2019')
    sql = "SELECT * FROM Person.Person WHERE FirstName = 'Ken'"
    conn.output_format('native')  # change output format to native
    result, error = await conn.query(
        sql
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    pytest.assume(len(result) == 6)
    conn.output_format('record')  # change output format to list of records
    result, error = await conn.query(
        sql
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    for row in result:
        pytest.assume(type(row) == Record)
    conn.output_format('recordset')  # change output format to recordset
    result, error = await conn.query(
        sql
    )
    pytest.assume(not error)
    pytest.assume(type(result) == Recordset)
    for row in result:
        pytest.assume(type(row) == Record)
    # Bug with Datatable and UUID support.
    # conn.output_format('datatable')  # change output format to Datatable Frame
    # result, error = await conn.query(
    #     sql
    # )
    # pytest.assume(not error)
    # print(result)
    # pytest.assume(type(result) == dt.Frame)
    conn.output_format('pandas')  # change output format to Pandas Dataframe
    result, error = await conn.query(
        sql
    )
    pytest.assume(not error)
    print(result)
    pytest.assume(type(result) == pandas.core.frame.DataFrame)


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
