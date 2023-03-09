from collections.abc import AsyncGenerator, Awaitable
import pytest_asyncio
from asyncdb import AsyncDB, AsyncPool


@pytest_asyncio.fixture()
async def pooler(event_loop, driver, dsn) -> AsyncGenerator[Awaitable, None]:
    args = {
        "timeout": 36000,
        "server_settings": {
            "application_name": "Test"
        }
    }
    pool = AsyncPool(driver, dsn=dsn, loop=event_loop, **args)
    print(pool, type(pool))
    await pool.connect()
    yield pool
    await pool.wait_close(gracefully=True, timeout=10)


@pytest_asyncio.fixture()
async def conn(event_loop, driver, dsn) -> AsyncGenerator[Awaitable, None]:
    db = AsyncDB(driver, dsn=dsn, loop=event_loop)
    yield db
    await db.close()


@pytest_asyncio.fixture()
async def connection(event_loop, driver, params) -> AsyncGenerator[Awaitable, None]:
    db = AsyncDB(driver, params=params, loop=event_loop)
    yield db
    await db.close()
