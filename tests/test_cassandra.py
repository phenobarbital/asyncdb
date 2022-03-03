import pytest
from asyncdb import AsyncDB
import asyncio

@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


DRIVER='cassandra'
VERSION = '4.0.1'
PARAMS = {
    "host": "127.0.0.1",
    "port": "9042",
    "username": 'cassandra',
    "password": 'cassandra',
    "database": 'library'
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_pool_by_params(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    assert db.is_connected() is False

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(type(result) == list)
    row = result[0]
    pytest.assume(row['release_version'] == VERSION)
    await db.close()
    pytest.assume(db.is_connected() is False)


async def test_connection(conn):
    #await conn.connection()
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection()
    pytest.assume(type(result) == list)
