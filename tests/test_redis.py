import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
from .conftest import (
    conn,
    pooler
)


@pytest.fixture()
def driver():
    return 'redis'


@pytest.fixture()
def dsn():
    return "redis://127.0.0.1:6379/0"


@pytest.fixture()
def params():
    return {
        "host": "127.0.0.1",
        "port": "6379",
        "db": 0
    }


pytestmark = pytest.mark.asyncio


@pytest.mark.usefixtures("driver", "params")
async def test_pool_by_params(driver, params, event_loop):
    pool = AsyncPool(driver, params=params, loop=event_loop)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    await pool.close()


@pytest.mark.usefixtures("driver", "dsn")
async def test_pool_by_dsn(driver, dsn, event_loop):
    """ test creation using DSN """
    pool = AsyncPool(driver, dsn=dsn, loop=event_loop)
    print(pool)
    pytest.assume(pool.is_connected() is False)
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    db = await pool.acquire()
    result, error = await db.test_connection('helloworld')
    pytest.assume(not error)
    pytest.assume(result == '1')
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


@pytest.mark.usefixtures("driver", "params", "dsn")
async def test_driver_by_params(driver, params, dsn, event_loop):
    pool = AsyncPool(driver, params=params, loop=event_loop)
    assert pool.is_connected() is False
    assert pool.get_dsn() == dsn
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    await pool.close()
    assert pool.is_closed() is True


@pytest.mark.usefixtures("driver", "params", "dsn")
async def test_pool_by_params2(driver, params, dsn, event_loop):
    pool = AsyncPool(driver, params=params, loop=event_loop)
    assert pool.is_connected() is False
    assert pool.get_dsn() == dsn
    await pool.connect()
    pytest.assume(pool.is_connected() is True)
    await pool.close()
    assert pool.is_closed() is True


@pytest.mark.usefixtures("driver", "params")
async def test_conn_by_params(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    pytest.assume(db.is_connected() is True)
    await db.close()
    print(db.is_closed())
    assert db.is_closed() is True


@pytest.mark.usefixtures("driver", "params")
async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    print(driver, event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    result, error = await db.test_connection('bigtest')
    print(result, error)
    pytest.assume(not error)
    pytest.assume(result == '1')
    await db.close()
    print(db.is_closed())
    assert db.is_closed() is True


@pytest.mark.usefixtures("conn", "driver")
async def test_context(conn, event_loop):
    async with await conn.connection() as db:
        pytest.assume(db.is_connected() is True)
        result, error = await db.test_connection()
        pytest.assume(not error)
        result, error = await db.test_connection('TEST CONTEXT', 'TEST CONTEXT')
        pytest.assume(not error)
        pytest.assume(result == 'TEST CONTEXT')
    assert conn.is_closed() is True


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
