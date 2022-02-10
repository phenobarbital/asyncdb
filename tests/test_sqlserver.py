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
async def test_pool_by_params(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
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
    await db.close()


async def test_connection(conn):
    await conn.connection()
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection()
    pytest.assume(type(result) == dict)
    await conn.close()


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
