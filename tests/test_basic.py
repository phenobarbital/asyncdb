import asyncio
import pytest
from .conftest import (
    conn,
    pooler
)


@pytest.fixture()
def driver():
    return 'pg'


@pytest.fixture()
def dsn():
    return "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator"


@pytest.fixture()
def params():
    return {
        "host": '127.0.0.1',
        "port": '5432',
        "user": 'troc_pgdata',
        "password": '12345678',
        "database": 'navigator'
    }


pytestmark = pytest.mark.asyncio


@pytest.mark.usefixtures("pooler", "driver", "dsn")
async def test_pooler(pooler, event_loop):
    """ Using Pooler """
    assert pooler is not None
    assert pooler.application_name == 'Test'
    assert pooler.is_closed() is False
    async with pooler as conn:
        result = await conn.execute("SELECT 1")
        pytest.assume(result == 'SELECT 1')


@pytest.mark.usefixtures("conn", "driver", "dsn")
async def test_connection(conn, event_loop):
    assert conn is not None
    async with await conn.connection() as cn:
        result, error = await cn.execute("SELECT 1")
        assert not error
        pytest.assume(result == 'SELECT 1')
        pytest.assume(cn.is_connected() is True)
        result, error = await cn.test_connection()
        pytest.assume(not error)
        pytest.assume(dict(result[0]) == {'?column?': 1})


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
