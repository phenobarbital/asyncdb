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
import uvloop
import aioredis
import objectpath
import time
from aioredis.connection import Connection, to_bool

from .base import (
    BasePool,
    BaseProvider,
)
from asyncdb.exceptions import *

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


class redisPool(BasePool):

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        self._dsn = "redis://{host}:{port}/{db}"
        super(redisPool, self).__init__(
            dsn=dsn,
            loop=loop,
            params=params,
            **kwargs
        )

    # Create a redis connection pool
    async def connect(self, **kwargs):
        """
        __init async db initialization
        """
        self._logger.debug("Redis Pool: Connecting to {}".format(self._dsn))
        try:
            self._pool = aioredis.ConnectionPool.from_url(
                self._dsn,
                encoding=self._encoding,
                decode_responses=True,
                max_connections=self._max_queries,
                **kwargs,
            )
            self._connection = aioredis.Redis(connection_pool=self._pool)
        except (aioredis.exceptions.ConnectionError) as err:
            raise ConnectionTimeout(
                "Unable to connect to Redis: {}".format(str(err)))
        except (aioredis.exceptions.RedisError) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err))
            )
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def acquire(self):
        """
        Take a connection from the pool.
        """
        # Take a connection from the pool.
        try:
            return redis(connection=self._connection, pool=self)
        except (aioredis.exceptions.ConnectionError) as err:
            raise ConnectionError(
                "Redis Pool is already closed: {}".format(str(err)))
        except (aioredis.exceptions.RedisError) as err:
            raise ConnectionError(
                "Redis Pool is closed o doesnt exists: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            return True
        try:
            print(type(connection.get_connection()))
            await self._pool.release(connection.get_connection())
        except Exception as err:
            raise ProviderError("Release Error: {}".format(str(err)))

    async def close(self, timeout=5):
        """
        Close Pool
        """
        try:
            if self._connection is not None:
                await self._connection.close()
            if self._pool:
                await self._pool.disconnect(inuse_connections=True)
                # await self._pool.disconnect()
            self._connected = False
            return True
        except (aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Connection close Error: {}".format(str(err)))
        except Exception as err:
            logging.exception("Pool Closing Error: {}".format(str(err)))
            return False

    disconnect = close

    async def execute(self, sentence, *args, **kwargs):
        """
        Execute a connection into the Pool
        """
        if self._pool:
            try:
                result = await self._connection.execute_command(
                    sentence, *args, **kwargs
                )
                return result
            except TypeError as err:
                raise ProviderError("Execute Error: {}".format(str(err)))
            except aioredis.exceptions.ConnectionError as err:
                raise ProviderError(
                    "Connection cannot be decoded or is broken, Error: {}".format(
                        str(err)
                    )
                )
            except (
                aioredis.exceptions.ConnectionError,
                aioredis.exceptions.RedisError
            ) as err:
                raise ProviderError(
                    "Connection close Error: {}".format(str(err)))
            except Exception as err:
                raise ProviderError("Redis Execute Error: {}".format(str(err)))


class redis(BaseProvider):
    _provider = "redis"
    _syntax = "json"

    def __init__(
            self, dsn="",
            loop=None,
            params={},
            **kwargs
    ):
        self._dsn = "redis://{host}:{port}/{db}"
        super(redis, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs)
        if "connection" in kwargs:
            self._connection = kwargs['connection']
            self._connected = True
        if "pool" in kwargs:
            self._pool = kwargs['pool']
            self._connected = True
        self._initialized_on = time.time()

    """
    Properties
    """
    @property
    def redis(self):
        return self._connection

    # Create a redis pool
    async def connection(self, **kwargs):
        """
        __init async redis initialization
        """
        self._logger.info("REDIS: Connecting to {}".format(self._dsn))
        try:
            self._connection = await aioredis.from_url(
                self._dsn,
                encoding=self._encoding,
                decode_responses=True,
                **kwargs,
            )
            # self._pool = aioredis.Redis(connection_pool=self._connection)
        except (aioredis.exceptions.ConnectionError, aioredis.exceptions.AuthenticationError) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err))
            )
        except (aioredis.RedisError, asyncio.TimeoutError) as err:
            raise ConnectionTimeout(
                "Unable to connect to Redis: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Redis Error: {}".format(str(err)))
            return False
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    def is_closed(self):
        if not self._connection:
            return True
        else:
            return not self._connected

    async def ping(self, msg: str = ""):
        await self._connection.ping()

    async def close(self, timeout: int = 10):
        try:
            # gracefully closing underlying connection
            await self._connection.close()
            try:
                # safely closing the inner connection pool
                await self._connection.connection_pool.disconnect()
            except Exception as err:
                print(err)
        except (RuntimeError, AttributeError) as err:
            pass
        except Exception as err:
            self._logger.exception(f'Redis Closing Error: {err}')
        finally:
            self._connected = False

    disconnect = close

    async def execute(self, sentence, *args):
        if self._connection:
            try:
                result = await self._connection.execute_command(sentence, *args)
                return result
            except (
                aioredis.RedisError,
                aioredis.exceptions.ConnectionError,
            ) as err:
                raise ProviderError("Connection Error: {}".format(str(err)))

    execute_many = execute

    async def prepare(self):
        pass

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

    async def query(self, key="", *val):
        return await self.get(key, val)

    async def queryrow(self, key="", *args):
        return await self.get(key, val)

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

    async def use(self, database: int):
        try:
            await self._connection.execute_command("SELECT", database)
        except (
            aioredis.RedisError,
            aioredis.exceptions.ConnectionError,
        ) as err:
            raise ProviderError(f"Redis: Can't change to DB: {err!s}")

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
            return await self._connection.exists(key, *keys)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err)))

    async def delete(self, key, *keys):
        try:
            return await self._connection.delete(key, *keys)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err)))

    async def expire_at(self, key, timestamp):
        try:
            return await self._connection.expireat(key, timestamp)
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration timestamp: {}".format(str(timestamp))
            )
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err)))

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
                "Redis: wrong Expiration timestamp: {}".format(str(timeout))
            )
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis SetEx Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis SetEx Unknown Error: {}".format(str(err)))

    def persist(self, key):
        """
        persist
            Remove the expiration of a key
        """
        try:
            return self._connection.persist(key)
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err)))

    async def set_key(self, key, value):
        await self.set(key, value)

    async def get_key(self, key):
        return await self.get(key)

    """
     Hash functions
    """

    async def hmset(self, name: str, info: dict):
        """
        set the value of a key in field (redis dict)
        """
        try:
            # await self._connection.hmset(name, mapping)
            await self._connection.hset(name, mapping=info)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

    async def hgetall(self, key):
        """
        Get all the fields and values in a hash (redis dict)
        """
        try:
            return await self._connection.hgetall(key)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

    async def set_hash(self, key, kwargs):
        await self.hmset(key, kwargs)

    async def get_hash(self, key):
        return await self.hgetall(key)

    fetch_all = get_hash

    async def hkeys(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return await self._connection.hkeys(key)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

    async def hvals(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return await self._connection.hkeys(key)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

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
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err)))

    async def hget(self, key, field):
        """
        get the value of a hash field (redis dict)
        """
        try:
            return await self._connection.hset(key, field)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hget Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hget Unknown Error: {}".format(str(err)))

    fetch_one = hget

    async def hexists(self, key, field, value):
        """
        Determine if hash field exists on redis dict
        """
        try:
            await self._connection.hexists(key, field)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis hash exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis hash exists Unknown Error: {}".format(str(err)))

    async def hdel(self, key, field, *fields):
        """
        Delete one or more hash fields
        """
        try:
            await self._connection.hdel(key, field, *fields)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err)))

    async def lrange(self, key, start: int = 0, stop: int = 100):
        """
        Delete one or more hash fields
        """
        try:
            await self._connection.lrange(key, start, stop)
        except (aioredis.RedisError, aioredis.exceptions.ConnectionError) as err:
            raise ProviderError("Redis lrange Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis lrange Unknown Error: {}".format(str(err)))
