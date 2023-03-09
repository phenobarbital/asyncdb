import asyncio
import pytest
from influxdb_client.domain.health_check import HealthCheck
from asyncdb import AsyncDB
from .conftest import (
    conn,
    pooler
)


@pytest.fixture()
def driver():
    return 'influx'


@pytest.fixture()
def params():
    return {
        "host": "127.0.0.1",
        "port": "8086",
        "database": 'navigator',
        "token": "qroJLmcrjM-IsDhxz-nR_NIoUxpjAgDz9AuXJJlTnikCIr70CNa_IxXlO5BID4LVrpHHCjzzeSr_UZab5ON_9A==",
        "user": "troc_pgdata",
        "password": "12345678",
        "org": "navigator"
    }


pytestmark = pytest.mark.asyncio


@pytest.mark.usefixtures("driver", "params")
async def test_pool_by_params(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False


@pytest.mark.usefixtures("driver", "params")
async def test_connect(driver, params, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    pytest.assume(isinstance(result, HealthCheck))
    await db.close()


@pytest.mark.usefixtures("connection", "driver")
async def test_connection(connection, event_loop):
    assert connection is not None
    async with await connection.connection() as db:
        pytest.assume(db.is_connected() is True)
        result, error = await db.test_connection()
        assert not error
        pytest.assume(isinstance(result, HealthCheck))


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
