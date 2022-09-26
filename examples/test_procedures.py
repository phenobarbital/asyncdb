import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler

params = {
    "host": "localhost",
    "port": "1433",
    "database": 'AdventureWorks2019',
    "user": 'SA',
    "password": 'P4ssW0rd1.'
}

DRIVER = 'sqlserver'


async def connect(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        pprint(conn.is_connected())
        args = {"@OrgId": 73, "@UserId": 1, "@PageSize": 100, "@StartIndex": 1}
        result, error = await conn.exec(
            "GET_PRODUCTS_LIST", **args, paginated=True, page="TotalRowCount", idx="@StartIndex"
        )
        print(result, len(result), error)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(default_exception_handler)
        driver = AsyncDB(DRIVER, params=params, loop=loop)
        print(driver)
        loop.run_until_complete(connect(driver))
    finally:
        loop.stop()
