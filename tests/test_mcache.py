import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio

DRIVER = 'mcache'
params = {
    "host": "localhost",
    "port": 11211
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    db.connection()
    yield db
    db.close()

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("driver", [
    (DRIVER), (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = db.test_connection('bigtest')
    pytest.assume(not error)
    assert result == 'bigtest'
    db.close()


async def multiget(conn):
    pytest.assume(conn.is_connected() is True)
    result = conn.set("Test2", "No More Test")
    pytest.assume(result)
    result = conn.set("Test3", "Expiration Data", 10)
    pytest.assume(result)
    values = conn.multiget("Test2", "Test3")
    pytest.assume(values == ["No More Test", "Expiration Data"])
    conn.delete("Test2")
    conn.delete("Test3")
    value = conn.get("Test2")
    assert (not value)


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
