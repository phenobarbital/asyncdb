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

DRIVER = 'redis'
params = {
    "host": "localhost",
    "port": "6379",
    "db": 3
}
DSN = "redis://localhost:6379/3"

@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio

async def test_pool_by_dsn(event_loop):
    """ test creation using DSN """
    pool = AsyncPool(DRIVER, dsn=DSN, loop=event_loop)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    db = await pool.acquire()
    result, error = await db.test_connection('helloworld')
    pytest.assume(not error)
    pytest.assume(result == 'helloworld')
    user = {
        "Name": "Pradeep",
        "Company": "SCTL",
        "Address": "Mumbai",
        "Location": "RCP",
    }
    await db.set_hash("user", user)
    pytest.assume(await db.exists("user") == 1)
    result = await db.get_hash("user")
    # print(result, await db.exists("user"))
    pytest.assume(result["Name"] == "Pradeep")
    await db.delete("user")
    pytest.assume(await db.exists("user") == 0)
    await pool.close()
    assert pool.is_closed() is True

async def test_pool_by_params(event_loop):
    pool = AsyncPool(DRIVER, params=params, loop=event_loop)
    assert pool.get_dsn() == redis_url

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_pool_by_params(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False

@pytest.mark.parametrize("driver", [
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    result, error = await db.test_connection('bigtest')
    pytest.assume(not error)
    pytest.assume(result == 'bigtest')
    await db.close()
    assert db.is_closed() is True

@pytest.mark.parametrize("driver", [
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER),
    (DRIVER)
])
async def test_context(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result, error = await db.test_connection()
        pytest.assume(not error)
        result, error = await db.test_connection('bigtest')
        pytest.assume(not error)
        pytest.assume(result == 'bigtest')
    assert db.is_closed() is True
