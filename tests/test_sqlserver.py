import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio


DRIVER = 'sqlserver'
params = {
    "server": "localhost",
    "port": "1433",
    "database": 'AdventureWorks2019',
    "user": 'sa',
    "password": 'P4ssW0rd1.'
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


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
