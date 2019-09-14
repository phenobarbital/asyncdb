import asyncio

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

# from asyncdb import AsyncDB
# from asyncdb.providers.redis import redis
#
# from asyncdb.exceptions import ProviderError, NoDataFound

redis_url = 'redis://localhost:6379/3'
# r = AsyncDB('redis', dsn=redis_url, loop=loop)
# rd = redis(dns=redis_url, loop=loop)
