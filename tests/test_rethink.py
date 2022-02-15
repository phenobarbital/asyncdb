import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest

DRIVER = 'rethink'
params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

params_auth = {
    "host": "localhost",
    "port": "28015",
    "db": "troc",
    "user": "test",
    "password": "supersecret"
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
    await db.connection()
    assert db.is_connected() is True
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_cursors(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    assert db.is_connected() is True
    await db.use('epson')
    async with db.cursor(
            tablename="epson_api_photo_categories") as cursor:
        async for row in cursor:
            pytest.assume(type(row) == dict)
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    await db.close()
    assert db.is_closed() is True


async def test_connection(conn):
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection()
    pytest.assume(not error)
    pytest.assume(type(result) == list)


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
