import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

from asyncdb import AsyncDB
from asyncdb.providers.rethink import rethink

# create a rethink connection with parameters
params = {
    'user': None,
    'password': None,
    'host': 'localhost',
    'port': '28015',
    'db': 'test'
}

rt = rethink(params=params)
#rt = AsyncDB('rethink', params=params)

async def connect(c):
    async with await c.connection() as conn:
        print("Connection established: %s" % c.connected)
        newloop = c.get_loop() # get the running loop
        conn.test_connection()
        await conn.db('test') # set the TEST DB
        # get a query
        shows, error = await conn.query(table = 'tv_shows')
        print(shows)

loop.run_until_complete(connect(rt))
rt.terminate()
