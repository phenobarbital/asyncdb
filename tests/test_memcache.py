import asyncio
import pytest
from asyncdb import AsyncDB, AsyncPool
from .conftest import (
    conn,
    pooler
)


@pytest.fixture()
def driver():
    return 'memcache'


@pytest.fixture()
def params():
    return {
        "host": "localhost",
        "port": 11211
    }


pytestmark = pytest.mark.asyncio


@pytest.mark.usefixtures("driver", "params")
async def test_pool_by_params(driver, params, event_loop):
    pool = AsyncPool(driver, params=params, loop=event_loop)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    await pool.close()


@pytest.mark.usefixtures("driver", "params")
async def test_pool_by_params2(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    async with await db.connection() as conn:
        assert db.is_connected() is True


@pytest.mark.usefixtures("driver", "params")
async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection('bigtest')
    pytest.assume(not error)
    assert result == '1'
    await db.close()


@pytest.mark.usefixtures("conn", "driver")
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
