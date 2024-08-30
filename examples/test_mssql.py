import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler

params = {
    "host": "localhost",
    "port": "3307",
    "database": 'PRD_MI',
    "user": 'navuser',
    "password": 'L2MeomsUgYpFBJ6t'
}

DRIVER = 'sqlserver'


async def connect(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        pprint(conn.is_connected())
        await conn.use('PRD_MI')
        result, error = await conn.query('SELECT * FROM dbo.Forms')
        print('ERROR > ', error)
        print('RESULT > ', result[0])



if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(default_exception_handler)
        driver = AsyncDB(DRIVER, params=params, loop=loop)
        print(driver)
        loop.run_until_complete(connect(driver))
    finally:
        loop.stop()
