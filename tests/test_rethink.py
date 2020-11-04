import asyncio
import logging

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


from asyncdb import AsyncDB, AsyncPool
from asyncdb.exceptions import NoDataFound, ProviderError
from asyncdb.providers.rethink import rethink


params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

rt = AsyncDB("rethink", params=params, loop=loop)

async def connect(db):
    async with await db.connection() as conn:
        print("Connected: {}".format(conn.is_connected))
        print(await conn.test_connection())
        conn.use('troc')
        db = await conn.list_databases()
        print(db)
        filter = {"company_id": 10, "state_code": "FL"}
        result, error = await conn.query('troc_populartimes', filter=filter)
        if not error:
            for row in result:
                print(row)
try:
    loop.run_until_complete(connect(rt))
finally:
    loop.run_until_complete(rt.close())
    # Find all running tasks:
    pending = asyncio.all_tasks()
    # Run loop until tasks done:
    loop.run_until_complete(asyncio.gather(*pending))
    print("Shutdown complete ...")
    #loop.stop()
