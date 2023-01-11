import asyncio
from datetime import datetime
from asyncdb.drivers.pg import pgPool

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}


async def test_pg():
    start = datetime.now()
    pool = pgPool(loop=loop, params=params)
    await pool.connect()
    print('Pool Connected: ', pool.is_connected())
    async with await pool.acquire() as conn:
        print('Connection: ', conn)
        result, error = await conn.test_connection()
        print(result, error)
        result, error = await conn.query('SELECT * FROM bad_table_data')
        print(result, error)
        exec_time = (datetime.now() - start).total_seconds()
        if not error:
            print(result)
        print(f"Execution Time {exec_time:.3f}s\n")
    await pool.wait_close(gracefully=True, timeout=5)

if __name__ == "__main__":
    loop.run_until_complete(test_pg())
    loop.stop()
