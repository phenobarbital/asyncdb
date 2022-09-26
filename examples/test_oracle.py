import asyncio
from asyncdb import AsyncDB
from asyncdb.drivers.oracle import oracle
from asyncdb.utils import cPrint


ora = {
    "host": "127.0.0.1",
    "port": 1521,
    "user": 'system',
    "password": 'oracle',
    "database": 'xe',
    "oracle_client": '/opt/oracle/instantclient_19_16'
}

async def test_connect(evt):
    d = oracle(params=ora, loop=evt)
    async with await d.connection() as conn:
        cPrint(f"Is Connected: {conn.is_connected()}")

async def test_db(evt):
    params = {
        "user": 'system',
        "password": 'oracle'
    }
    o = AsyncDB('oracle', dsn='127.0.0.1:1521/xe', params=params, oracle_client='/opt/oracle/instantclient_19_16', loop=evt)
    async with await o.connection() as conn:
        cPrint(f"Is Connected: {conn.is_connected()}")

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test_connect(loop))
        loop.run_until_complete(test_db(loop))
    finally:
        loop.stop()
