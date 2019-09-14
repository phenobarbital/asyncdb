#!/usr/bin/env python3

import asyncio

import aioredis

from asyncdb.providers import BaseProvider, registerProvider, exception_handler
from asyncdb.exceptions import *
from asyncdb.utils import *

class redis(BaseProvider):
    _provider = 'redis'
    _syntax = 'json'
    _pool = None
    _dsn = 'redis://{host}:{port}/{db}'
    _connection = None
    _connected = False
    _loop = None

    def __init__(self, dsn='', loop=None, params={}):
        self._params = params
        if not dsn:
            self._dsn = self.create_dsn(self._params)
        else:
            self._dsn = dsn
        try:
            self._DEBUG = bool(params['DEBUG'])
        except KeyError:
            self._DEBUG = False
        self._loop.set_exception_handler(exception_handler)
        self._loop.set_debug(self._DEBUG)
        print(self._dsn)

    """
    Context magic Methods
    """
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    """
    Properties
    """
    @property
    def pool(self):
        return self._pool

    def is_connected(self):
        return self._connected

    def loop(self):
        return self._loop

    '''
    __init async redis initialization
    '''
    # Create a redis pool
    @asyncio.coroutine
    async def connect(self):
        print("AsyncRedis: Connecting to {}".format(self._url))
        try:
            self._pool = await aioredis.create_redis_pool(self._url, minsize=5, maxsize = 10, loop = self._loop, encoding='utf-8')
        except Exception as err:
            print("Redis Error: {}".format(str(err)))
            return False
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()

    @asyncio.coroutine
    async def close(self):
        if self._pool:
            try:
                self._pool.close()
                await self._pool.wait_closed()
            finally:
                self._pool = None


    async def set(self, key, value):
        with await self._pool as connection:
            try:
                return await connection.set(key, value)
            except Exception as err:
                print(err)
                #await connection.execute('discard')
                raise Exception(err)



    async def get(self, key):
        with await self._pool as connection:
            try:
                return await connection.get(key)
            except Exception as e:
                raise Exception

    async def clear_redis(self):
        """
        Clear a cache
        """
        with await self._pool as connection:
            try:
                return await connection.flushall()
            except Exception as e:
                print('Error cleaning cache: %s' % e)
                raise Exception
                return False

    async def exists(self, key):
        try:
            with await self._pool as connection:
                return await connection.exists(key)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            print(err)
            raise err



    def delete(self, key):
        return self._pool.delete(key)


    async def setex(self, key, value, timeout):
        with await self._pool as connection:
            if not isinstance(timeout, int):
                time = 900
            else:
                time = timeout
            try:
                return await connection.setex(key, time, value)
            except Exception as err:
                print(err)
                #await connection.execute('discard')
                raise Exception(err)


    @asyncio.coroutine
    async def set_key(self, key, value):
        await self.set(key, value)


    @asyncio.coroutine
    async def get_key(self, key):
        return await self.get(key)

"""
Registering this Provider
"""
registerProvider(redis)
