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


async def form_data(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        pprint(conn.is_connected())
        args = {"@OrgId": 69, "@UserId": 1, "@FormId": 2662, "@StartDateTime": "'2022-06-17 00:00:00'", "@EndDateTime": "'2022-06-17 23:59:59'"}
        result, error = await conn.exec(
            "FORM_DATA_VIEW_GET_LIST", **args, paginated=False, page="TotalRowCount", idx="@StartIndex"
        )
        print(result, error)
        # print(result, len(result), error)


async def activities(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        pprint(conn.is_connected())
        args = {
            "@OrgId": 74,
            "@UserId": 1,
            "@StartDateFrom": "'2022-06-17 00:00:00'",
            "@EndDateFrom": "'2022-06-17 23:59:59'",
            "@includeSubmittedActivities": 1,
            "@PageSize": 1000
        }
        result, error = await conn.exec(
            "dbo.ACTIVITY_ITEM_GET_LIST", **args, paginated=True, page="TotalRowCount", idx="@StartIndex"
        )
        print(error)
        if result:
            print(len(result))
        # print(result, len(result), error)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(default_exception_handler)
        driver = AsyncDB(DRIVER, params=params, loop=loop)
        print(driver)
        loop.run_until_complete(activities(driver))
    finally:
        loop.stop()
