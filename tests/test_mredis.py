import pytest
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio


DRIVER = 'mredis'
params = {
    "host": "127.0.0.1",
    "port": "6379",
    "db": 0
}
DSN = "redis://127.0.0.1:6379/0"


@pytest_asyncio.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    db.connection()
    yield db
    db.close()

pytestmark = pytest.mark.asyncio

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect_by_dsn(driver, event_loop):
    """ test creation using DSN """
    db = AsyncDB(driver, dsn=DSN, loop=event_loop)
    print(db)
    pytest.assume(db.is_connected() is False)
    db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = db.test_connection('helloworld')
    pytest.assume(not error)
    pytest.assume(result == 'helloworld')
    user = {
        "Name": "Pradeep",
        "Company": "SCTL",
        "Address": "Mumbai",
        "Location": "RCP",
    }
    db.set_hash("user", user)
    pytest.assume(db.exists("user") == 1)
    result = db.get_hash("user")
    # print(result, await db.exists("user"))
    pytest.assume(result["Name"] == "Pradeep")
    db.delete("user")
    pytest.assume(db.exists("user") == 0)
    db.close()
    db.is_closed() is True

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_conn_by_params(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    db.connection()
    pytest.assume(db.is_connected() is True)
    db.close()
    print(db.is_closed())
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    print(driver, event_loop)
    with db as conn:
        pytest.assume(conn.is_connected() is True)
        result, error = db.test_connection()
        pytest.assume(not error)
        result, error = db.test_connection('bigtest')
        print(result, error)
        pytest.assume(not error)
        pytest.assume(result == 'bigtest')
    db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_context(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    with db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result, error = conn.test_connection()
        pytest.assume(not error)
        result, error = conn.test_connection('TEST CONTEXT')
        pytest.assume(not error)
        pytest.assume(result == 'TEST CONTEXT')
    db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_expiration(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    with db as conn:
        pytest.assume(conn.is_connected() is True)
        conn.setex("Test3", "Expiration Data", 5)
        pytest.assume(conn.exists('Test3') is True)
        value = conn.get("Test3")
        pytest.assume(value == "Expiration Data")
        await asyncio.sleep(5)
        pytest.assume(conn.exists('Test3') is False)
        conn.setex("Test4", "Expiration Data", 10)
        assert conn.persist("Test4")
        conn.delete("Test4")
        pytest.assume(conn.exists('Test4') is False)
    db.close()
    assert db.is_closed() is True

def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
