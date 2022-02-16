import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio

DRIVER = 'memcache'
PARAMS = {
    "host": "localhost",
    "port": 11211
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio


async def test_pool_by_params(event_loop):
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    await pool.close()


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_pool_by_params2(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    assert db.is_connected() is False


@pytest.mark.parametrize("driver", [
    (DRIVER), (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection('bigtest')
    pytest.assume(not error)
    assert result == 'bigtest'
    await db.close()


async def multiget(conn):
    pytest.assume(conn.is_connected() is True)
    result = await conn.set("Test2", "No More Test")
    pytest.assume(result)
    result = await conn.set("Test3", "Expiration Data", 10)
    pytest.assume(result)
    values = await conn.multiget("Test2", "Test3")
    pytest.assume(values == ["No More Test", "Expiration Data"])
    await conn.delete("Test2")
    await conn.delete("Test3")
    value = await conn.get("Test2")
    assert (not value)


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
