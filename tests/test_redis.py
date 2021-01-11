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
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    await db.close()


async def test_connection(conn):
    #await conn.connection()
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection('bigtest')
    pytest.assume(not error)
    assert result == 'bigtest'
