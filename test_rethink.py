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
    'db': 'troc'
}

#rt = rethink(params=params)
rt = AsyncDB('rethink', params=params)

async def connect(c):
    async with await c.connection() as conn:
        print("Connection established: %s" % c.connected)
        newloop = c.get_loop() # get the running loop
        conn.test_connection()
        await conn.dropdb('test')
        await conn.createdb('test')
        await conn.db('test') # set the TEST DB
        await conn.create_table('tv_shows')
        print ('- createindex:', await conn.createindex(table='tv_shows', field='title'))
        print ('- listdb:', await conn.listdb())
        print ('- list_databases:', await conn.list_databases())
        print ('- list_tables:', await conn.list_tables())
        print ('- insert:', await conn.insert('tv_shows', [
                                                {'id': 1, 'title': 'Show 1'},
                                                {'id': 2, 'title': 'Show 2'},
                                                {'id': 3, 'title': 'Show 3'},
                                                {'id': 4, 'title': 'Show 4'},
                                                {'id': 5, 'title': 'Show 5'},
                                                {'id': 6, 'title': 'Show 6'},
                                            ]))
        print ('- get:', await conn.get(table='tv_shows', id=2))
        print ('- get_all:', await conn.get_all(table='tv_shows', filter=1, index='id'))
        print ('- queryrow:', await conn.queryrow(table = 'tv_shows', filter={'title':'Show 1'}))
        print ('- query:', await conn.query(table = 'tv_shows'))
        print ('- replace:', await conn.replace('tv_shows', id=1, data={'id': 1, 'title': 'New Show 1'}))
        print ('- match:', await conn.match(table = 'tv_shows', field='title', regexp='New'))
        print ('- between:', await conn.between(table = 'tv_shows', min=2, max=4))

        await conn.sync('test')

loop.run_until_complete(connect(rt))
rt.terminate()
