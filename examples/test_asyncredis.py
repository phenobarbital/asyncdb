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

DRIVER = 'asyncredis'
DSN = "redis://localhost:6379/3"
PARAMS = {
    "host": "localhost",
    "port": "6379",
    "db": 3
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
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
    result, error = await db.test_connection('helloworld', 'Hola Mundo')
    pytest.assume(not error)
    pytest.assume(result == 'Hola Mundo')
    await pool.close()
    assert pool.is_closed() is True

async def test_pool_by_params(event_loop):
    pool = AsyncPool(DRIVER, params=PARAMS, loop=event_loop)
    assert pool.get_dsn() == DSN

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_pool_by_params(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    pytest.assume(db.is_connected() is True)
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
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    result, error = await db.test_connection('TEST', 'bigtest')
    pytest.assume(not error)
    pytest.assume(result == 'bigtest')
    await db.close()
    assert db.is_closed() is True


async def test_connection(conn):
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection('TEST', 'bigtest')
    pytest.assume(not error)
    assert result == 'bigtest'


async def test_many(event_loop):
    pool = AsyncPool(DRIVER, dsn=DSN, loop=event_loop)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    for lp in range(10000):
        print(f'Test number {lp}')
        async with await pool.acquire() as cnt:
            await cnt.ping()
            await cnt.execute("set", "Test1", "UltraTest")
            result = await cnt.get('Test1')
            pytest.assume(result == "UltraTest")
            await cnt.delete("Test1")
    print('Ending ...')
