#!/usr/bin/env python3
""" Redis async Provider.
Notes on redis Provider
--------------------
This provider implements a few subset of funcionalities from aioredis, is a WIP
TODO:
 - use jsonpath to query json-objects
 - implements lists and hash datatypes
"""

import asyncio

import aioredis
import objectpath
import time

from asyncdb.exceptions import *
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)
from asyncdb.utils import *


class redisPool(BasePool):
    _dsn = "redis://{host}:{port}/{db}"
    _max_queries = 300
    _pool = None
    _connection = None
    _encoding = "utf-8"

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        super(redisPool, self).__init__(dsn=dsn, loop=loop, params=params)
        self._pool = None
        try:
            if params["encoding"]:
                self._encoding = params["encoding"]
        except KeyError:
            pass
        if 'max_queries' in kwargs:
            self._max_queries = kwargs["max_queries"]

    def get_event_loop(self):
        return self._loop

    def get_connection(self):
        return self._connection

    def is_closed(self):
        return self._pool.closed

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._loop.run_until_complete(self.release())

    # Create a redis connection pool
    async def connect(self, **kwargs):
        """
        __init async db initialization
        """
        self.logger.debug("Redis Pool: Connecting to {}".format(self._dsn))
        try:
            self._pool = await aioredis.create_pool(
                self._dsn,
                minsize=5,
                maxsize=self._max_queries,
                loop=self._loop,
                encoding=self._encoding,
                create_connection_timeout=300,
                **kwargs
            )
        except (aioredis.ProtocolError) as err:
            raise ConnectionTimeout(
                "Unable to connect to Redis: {}".format(str(err))
            )
        except (aioredis.RedisError) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err)
                )
            )
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
        # Take a connection from the pool.
        try:
            if self._pool.closed:
                await self._pool.connect()
            self._connection = await self._pool.acquire()
        except (aioredis.PoolClosedError) as err:
            raise ConnectionError(
                "Redis Pool is already closed: {}".format(str(err))
            )
        except (aioredis.ConnectionClosedError) as err:
            raise ConnectionError(
                "Redis Pool is closed o doesnt exists: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        if self._connection:
            db = redis(connection=self._connection, pool=self)
        return db

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        else:
            if isinstance(connection, redis):
                conn = connection.engine()
            else:
                conn = connection
        try:
            if conn:
                self._pool.release(conn)
        except Exception as err:
            raise ProviderError("Release Error: {}".format(str(err)))

    async def close(self, timeout=5):
        """
        Close Pool
        """
        try:

            if self._pool and self._pool.closed is False:
                self._pool.close()
                # also clear:
                await self._pool.clear()
                #await asyncio.sleep(1)
                close = asyncio.create_task(self._pool.wait_closed())
                try:
                    await asyncio.wait_for(
                        close,
                        timeout=timeout
                    )
                except asyncio.exceptions.TimeoutError as err:
                    pass
        except (
            aioredis.errors.PoolClosedError, aioredis.ConnectionClosedError
        ) as err:
            raise ProviderError("Connection close Error: {}".format(str(err)))
        except Exception as err:
            logging.exception("Pool Closing Error: {}".format(str(err)))
            return False

    async def execute(self, sentence, *args, **kwargs):
        """
        Execute a connection into the Pool
        """
        if self._pool:
            try:
                result = await self._pool.execute(sentence, *args, **kwargs)
                return result
            except TypeError as err:
                raise ProviderError("Execute Error: {}".format(str(err)))
            except aioredis.ProtocolError as err:
                raise ProviderError(
                    "Connection cannot be decoded or is broken, Error: {}".
                    format(str(err))
                )
            except (
                aioredis.errors.PoolClosedError,
                aioredis.ConnectionClosedError,
            ) as err:
                raise ProviderError(
                    "Connection close Error: {}".format(str(err))
                )
            except Exception as err:
                raise ProviderError("Redis Execute Error: {}".format(str(err)))


class redis(BaseProvider):
    _provider = "redis"
    _syntax = "json"
    _pool = None
    _dsn = "redis://{host}:{port}/{db}"
    _connection = None
    _connected = False
    _loop = None
    _encoding = "utf-8"

    def __init__(self, dsn="", connection=None, loop=None, pool=None, params={}):
        super(redis, self).__init__(dsn=dsn, loop=loop, params=params)
        if pool:
            self._pool = pool
            self._connection = aioredis.Redis(connection)
            self._connected = True
            self._initialized_on = time.time()
        try:
            if params["encoding"]:
                self._encoding = params["encoding"]
        except KeyError:
            pass

    """
    Context magic Methods
    """
    async def __aenter__(self) -> "BaseProvider":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.release()

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

    def loop(self):
        return self._loop

    @property
    def redis(self):
        return self._connection

    # Create a redis pool
    async def connection(self, **kwargs):
        """
        __init async redis initialization
        """
        self._logger.info("AsyncRedis: Connecting to {}".format(self._dsn))
        try:
            connection = await aioredis.create_connection(
                self._dsn,
                loop=self._loop,
                encoding=self._encoding,
                timeout=self._timeout,
                **kwargs
            )
            self._connection = aioredis.Redis(connection)
        except (aioredis.ProtocolError, aioredis.AuthError) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err)
                )
            )
        except (aioredis.RedisError, asyncio.TimeoutError) as err:
            raise ConnectionTimeout(
                "Unable to connect to Redis: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError("Unknown Redis Error: {}".format(str(err)))
            return False
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()

    async def release(self):
        """
        Release a connection and return into pool
        """
        if self._pool:
            await self._pool.release(
                connection=self._connection.connection
            )
        try:
            await self._connection.quit()
        except aioredis.errors.ConnectionClosedError as e:
            self._logger.error(f'Error releasing the Connection {e!s}')

    def is_closed(self):
        if not self._connection:
            return True
        else:
            return self._connection.closed

    async def ping(self, msg: str = ''):
        await self._connection.ping(msg)

    async def close(self):
        try:
            self._connection.close()
            await asyncio.shield(self._connection.wait_closed())
        finally:
            self._connection = None
            self._connected = False

    async def execute(self, sentence, *args):
        if self._connection:
            try:
                pass
                result = await self._connection.execute(sentence, *args)
                return result
            except (
                aioredis.errors.PoolClosedError,
                aioredis.errors.ConnectionClosedError,
            ) as err:
                raise ProviderError("Connection Error: {}".format(str(err)))

    async def prepare(self):
        pass

    async def test_connection(self, optional=1):
        result = None
        error = None
        try:
            await self.set('test_123', optional)
            result = await self.get('test_123')
        except Exception as err:
            error = err
        finally:
            await self.delete('test_123')
            return [result, error]


    async def query(self, key="", *val):
        return await self.get(key, val)

    async def queryrow(self, key="", *args):
        pass

    async def set(self, key, value):
        try:
            return await self._connection.set(key, value)
        except (aioredis.RedisError) as err:
            raise ProviderError("Redis Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Redis Unknown Error: {}".format(str(err)))

    async def get(self, key):
        try:
            return await self._connection.get(key)
        except (aioredis.RedisError) as err:
            raise ProviderError("Redis Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Redis Unknown Error: {}".format(str(err)))

    async def clear_redis(self):
        """
        Clear a cache
        """
        try:
            return await self._connection.flushall()
        except Exception as e:
            print("Error cleaning cache: %s" % e)
            raise Exception
            return False

    async def exists(self, key, *keys):
        if not self._connection:
            await self.connection()
        try:
            return self._connection.exists(key, *keys)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err))
            )

    async def delete(self, key, *keys):
        try:
            return await self._connection.delete(key, *keys)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err))
            )

    async def expire_at(self, key, timestamp):
        try:
            return await self._connection.expireat(key, timestamp)
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration timestamp: {}".format(str(timestamp))
            )
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err))
            )

    async def setex(self, key, value, timeout):
        """
        setex
           Set the value and expiration of a Key
           params:
            key: key Name
            value: value of the key
            timeout: expiration time in seconds
        """
        if not isinstance(timeout, int):
            time = 900
        else:
            time = timeout
        try:
            await self._connection.setex(key, time, value)
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration timestamp: {}".format(str(timestamp))
            )
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis SetEx Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis SetEx Unknown Error: {}".format(str(err))
            )

    def persist(self, key):
        """
        persist
            Remove the expiration of a key
        """
        try:
            return self._connection.persist(key)
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err))
            )

    async def set_key(self, key, value):
        await self.set(key, value)

    async def get_key(self, key):
        return await self.get(key)

    """
     Hash functions
    """

    async def hmset(self, key, *args, **kwargs):
        """
        set the value of a key in field (redis dict)
        """
        try:
            await self._connection.hmset_dict(key, *args, **kwargs)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err))
            )

    async def hgetall(self, key):
        """
        Get all the fields and values in a hash (redis dict)
        """
        try:
            return await self._connection.hgetall(key)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err))
            )

    async def set_hash(self, key, *args, **kwargs):
        await self.hmset(key, *args, **kwargs)

    async def get_hash(self, key):
        return await self.hgetall(key)

    async def hkeys(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return await self._connection.hkeys(key)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err))
            )

    async def hvals(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return await self._connection.hkeys(key)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err))
            )

    async def keys(self, key):
        return await self.hkeys(key)

    async def values(self, key):
        return await self.hvals(key)

    async def hset(self, key, field, value):
        """
        Set the string value of a hash field (redis dict)
        """
        try:
            await self._connection.hset(key, field, value)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err))
            )

    async def hget(self, key, field):
        """
        get the value of a hash field (redis dict)
        """
        try:
            return await self._connection.hset(key, field)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hget Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hget Unknown Error: {}".format(str(err))
            )

    async def hexists(self, key, field, value):
        """
        Determine if hash field exists on redis dict
        """
        try:
            await self._connection.hexists(key, field)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis hash exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis hash exists Unknown Error: {}".format(str(err))
            )

    async def hdel(self, key, field, *fields):
        """
        Delete one or more hash fields
        """
        try:
            await self._connection.hdel(key, field, *fields)
        except (aioredis.RedisError, aioredis.ProtocolError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err))
            )


"""
Registering this Provider
"""
registerProvider(redis)
