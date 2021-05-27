#!/usr/bin/env python3
""" memcache Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities from aiomcache, this is a WIP
"""

import asyncio
import time
import aiomcache

from asyncdb.exceptions import *
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)
from asyncdb.utils import *


class memcachePool(BasePool):
    _dsn = None
    _max_queries = 10
    _pool = None
    _connection = None

    def __init__(self, loop=None, params={}):
        super(memcachePool, self).__init__(loop=loop, params=params)
        self._pool = None

    def create_dsn(self, params):
        return params

    def get_event_loop(self):
        return self._loop

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._loop.run_until_complete(self.release())

    async def connect(self):
        self._logger.debug("AsyncMcache: Connecting to {}".format(self._params))
        try:
            self._pool = aiomcache.Client(
                pool_size=self._max_queries, loop=self._loop, **self._params
            )
        except aiomcache.exceptions.ClientException as err:
            raise ProviderError("Unable to connect to Memcache: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()

    async def acquire(self):
        """
        Take a connection from the pool.
        """
        db = None
        self._connection = None
        try:
            # self._connection = await self._pool.acquire()
            self._connection = self._pool
        except aiomcache.exceptions.ClientException as err:
            raise ProviderError("Unable to connect to Memcache: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        if self._connection:
            db = memcache(pool=self)
        return db

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        else:
            conn = connection
        try:
            if conn:
                self._pool.release(conn)
        except Exception as err:
            raise ProviderError("Memcache Release Error: {}".format(str(err)))

    async def close(self):
        """
        Close Pool
        """
        try:
            if self._pool:
                await self._pool.close()
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Connection Close Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Closing Error: {}".format(str(err)))


class memcache(BaseProvider):
    _provider = "memcache"
    _syntax = "nosql"
    _pool = None
    _dsn = ""
    _connection = None
    _connected = False
    _loop = None
    _encoding = "utf-8"

    def __init__(self, loop=None, pool=None, params={}):
        super(memcache, self).__init__(loop=loop, params=params)
        if pool:
            self._pool = pool
            self._connection = pool.get_connection()
            self._connected = True
            self._initialized_on = time.time()

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    # Create a memcache Connection
    async def connection(self):
        """
        __init async Memcache initialization
        """
        self._logger.debug("AsyncMcache: Connecting to {}".format(self._params))
        try:
            self._connection = aiomcache.Client(loop=self._loop, **self._params)
        except (aiomcache.exceptions.ValidationException) as err:
            raise ProviderError("Invalid Connection Parameters: {}".format(str(err)))
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Connection Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Memcache Error: {}".format(str(err)))
            return False
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()

    def release(self):
        """
        Release a connection and return into pool
        """
        if self._pool:
            self._loop.run_until_complete(
                self._pool.release(connection=self._connection)
            )

    async def close(self):
        """
        Closing memcache Connection
        """
        if self._pool:
            await self._pool.release(connection=self._connection)
        else:
            try:
                await self._connection.close()
            except (aiomcache.exceptions.ClientException) as err:
                raise ProviderError("Close Error: {}".format(str(err)))
            except Exception as err:
                raise ProviderError("Unknown Memcache Error: {}".format(str(err)))
                return False

    async def flush(self):
        """
        Flush all elements inmediately
        """
        try:
            if self._connection:
                self._connection.flush_all()
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Memcache Error: {}".format(str(err)))
            return False

    async def execute(self, sentence=""):
        pass

    async def prepare(self, sentence=""):
        pass

    async def query(self, key="", *val):
        return await self.get(key, val)

    async def queryrow(self, key="", *args):
        return await self.get(key, val)

    async def set(self, key, value, timeout=None):
        try:
            if timeout:
                return await self._connection.set(
                    bytes(key, "utf-8"), bytes(value, "utf-8"), exptime=timeout
                )
            else:
                return await self._connection.set(
                    bytes(key, "utf-8"), bytes(value, "utf-8")
                )
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Set Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    async def get(self, key):
        try:
            result = await self._connection.get(bytes(key, "utf-8"))
            if result:
                return result.decode("utf-8")
            else:
                return None
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Get Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    async def delete(self, key):
        try:
            return await self._connection.delete(bytes(key, "utf-8"))
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Memcache Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Exists Unknown Error: {}".format(str(err)))

    async def multiget(self, *kwargs):
        try:
            ky = [bytes(key, "utf-8") for key in kwargs]
            print(ky)
            result = await self._connection.multi_get(*ky)
            print(result)
            return [k.decode("utf-8") for k in result]
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError("Get Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    async def test_connection(self, optional=1):
        result = None
        error = None
        try:
            await self.set("test_123", optional)
            result = await self.get("test_123")
        except Exception as err:
            error = err
        finally:
            await self.delete("test_123")
            return [result, error]


"""
Registering this Provider
"""
registerProvider(memcache)
