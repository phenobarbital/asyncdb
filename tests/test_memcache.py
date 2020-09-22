import asyncio
import logging

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from asyncdb import AsyncDB, AsyncPool
from asyncdb.providers.mcache import mcache

params = {
    "host": 'localhost',
    "port": 11211
}
m = mcache(loop=loop, params=params)
m.connection()

def test_memcache(conn):
    conn.set('Test2', 'No More Sweet Music')
    value = conn.get('Test2')
    print(value)
    #
    conn.set('Test&4', 'Data With Expiration', 10)
    value = conn.get('Test&4')
    print(value)

    conn.set_multi({'Test2':'Testing 2', 'Test3': 'Testing 3'})

    values = conn.multiget('Test2', 'Test3', 'Test&4')
    print(values)

    conn.delete('Test2')
    # delete all
    conn.delete_multi('Test&4', 'Test3')

try:
    print("Connected: {}".format(m.is_connected()))
    test_memcache(m)
finally:
    m.close()
