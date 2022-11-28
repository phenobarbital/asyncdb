#!/usr/bin/env python3
""" memcache Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities from aiomcache.
"""
import asyncio
import time
from typing import (
    Dict
)
import aiomcache
from aiomcache.exceptions import (
    ClientException
)
from asyncdb.exceptions import (
    ProviderError,
    DriverError
)
from .abstract import (
    BasePool,
    BaseDriver,
)


class memcachePool(BasePool):
    """
    Pool-based version of Memcached connector.
    """

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._dsn = None
        self._connection = None
        self._max_queries = 10
        super(memcachePool, self).__init__(
            dsn, loop, params, **kwargs
        )

    def create_dsn(self, params: dict):
        return params

    async def connect(self):
        self._logger.debug(
            f"Memcache: Connecting to {self._params}")
        try:
            self._pool = aiomcache.Client(
                pool_size=self._max_queries, **self._params
            )

        except ClientException as err:
            raise DriverError(
                f"Unable to connect to Memcache: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Error: {err}"
            ) from err
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def acquire(self):
        """
        Take a connection from the pool.
        TODO: create a Pool infraestructure.
        """
        db = None
        self._connection = None
        try:
            self._connection = self._pool
        except ClientException as err:
            raise ProviderError(
                f"Unable to connect to Memcache: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Error: {err}"
            ) from err
        if self._connection:
            db = memcache(
                pool=self,
                loop=self._loop,
                connection=self._connection
            )
        return db

    async def release(self, connection=None):  # pylint: disable=W0221
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
            raise ProviderError(
                f"Memcache Release Error: {err}"
            ) from err

    async def close(self): # pylint: disable=W0221
        """
        Close Pool
        """
        try:
            if self._pool:
                await self._pool.close()
        except (ClientException) as err:
            raise DriverError(
                f"Connection Close Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Closing Error: {err}"
            ) from err

    disconnect = close


class memcache(BaseDriver):
    _provider = "memcache"
    _syntax = "nosql"

    def __init__(
        self,
        dsn: str = None,
        loop=None,
        params: dict = None,
        **kwargs
    ) -> None:
        super(memcache, self).__init__(
            dsn=dsn,
            loop=loop,
            params=params,
            **kwargs
        )
        if "pool" in kwargs:
            self._pool = kwargs['pool']
            self._connection = kwargs['connection']
            self._connected = True
            self._initialized_on = time.time()

    def create_dsn(self, params: dict):
        return params

    # Create a memcache Connection
    async def connection(self):
        """
        __init async Memcache initialization
        """
        self._logger.debug(
            f"Memcache: Connecting to {self._params}"
        )
        try:
            self._connection = aiomcache.Client(**self._params)
        except (aiomcache.exceptions.ValidationException) as err:
            raise DriverError(
                f"Invalid Connection Parameters: {err}"
            ) from err
        except ClientException as err:
            raise DriverError(
                f"Unable to connect to Memcache: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Error: {err}"
            ) from err
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def close(self): # pylint: disable=W0221
        """
        Closing memcache Connection
        """
        if self._pool:
            await self._pool.release(connection=self._connection)
        else:
            try:
                await self._connection.close()
            except ClientException as err:
                raise DriverError(
                    f"Unable to connect to Memcache: {err}"
                ) from err
            except Exception as err:
                raise ProviderError(
                    f"Unknown Error: {err}"
                ) from err

    disconnect = close

    async def flush(self):
        """
        Flush all elements inmediately
        """
        try:
            if self._connection:
                self._connection.flush_all()
        except ClientException as err:
            raise DriverError(
                f"Unable to connect to Memcache: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Error: {err}"
            ) from err

    async def prepare(self, sentence=""):
        raise NotImplementedError

    async def execute(self, sentence=""): # pylint: disable=W0221
        raise NotImplementedError

    async def execute_many(self, sentence: str = ''): # pylint: disable=W0221
        raise NotImplementedError

    async def use(self, database: str) -> None: # pylint: disable=W0221
        raise NotImplementedError

    async def get(self, key):
        try:
            result = await self._connection.get(bytes(key, "utf-8"))
            if result:
                return result.decode("utf-8")
            else:
                return None
        except (aiomcache.exceptions.ClientException) as err:
            raise ProviderError(
                f"Get Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    async def query(self, sentence, **kwargs):
        return await self.get(sentence)

    async def queryrow(self, sentence, **kwargs): # pylint: disable=W0613
        result = await self.get(sentence)
        if isinstance(result, list):
            result = result[0]
        return result

    fetch_one = queryrow

    async def fetch_all(self, sentence, *args): # pylint: disable=W0221
        return await self.multiget(*args)

    async def get_multi(self, *kwargs):
        return await self.multiget(kwargs)

    async def multiget(self, *args):
        try:
            ky = [bytes(key, "utf-8") for key in args]
            print(ky)
            result = await self._connection.multi_get(*ky)
            print(result)
            return [k.decode("utf-8") for k in result]
        except ClientException as err:
            raise ProviderError(
                f"Get Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    async def set(self, key, value, timeout: int = None):
        try:
            args = {}
            if timeout:
                args = {
                    "exptime": timeout
                }
            return await self._connection.set(
                bytes(key, "utf-8"), bytes(value, "utf-8"), **args
            )
        except ClientException as err:
            raise ProviderError(
                f"Set Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    async def set_multi(self, mapping: dict, timeout=0):
        """Migrate to pylibmc with Threads.
        """
        try:
            for k, v in mapping.items():
                await self._connection.set(bytes(k, "utf-8"), bytes(v, "utf-8"), timeout)
        except ClientException as err:
            raise ProviderError(
                f"Set Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    async def delete(self, key):
        try:
            return await self._connection.delete(bytes(key, "utf-8"))
        except ClientException as err:
            raise ProviderError(
                f"Delete Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    async def delete_multi(self, *kwargs):
        try:
            for key in kwargs:
                result = await self._connection.delete(bytes(key, "utf-8"))
            return result
        except ClientException as err:
            raise ProviderError(
                f"DELETE Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"DELETE Unknown Error: {err}"
            ) from err

    async def test_connection(self, key: str = 'test_123', optional: str = '1'):  # pylint: disable=W0221
        result = None
        error = None
        try:
            await self.set(key, optional)
            result = await self.get(key)
        except Exception as err:  # pylint: disable=W0703
            error = err
        finally:
            await self.delete(key)
            return [result, error] # pylint: disable=W0150
