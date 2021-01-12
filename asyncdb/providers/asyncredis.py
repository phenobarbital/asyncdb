#!/usr/bin/env python3
""" Redis async Provider.
Notes on redis Provider
--------------------
This provider implements a few subset of funcionalities from aredis, is a WIP
TODO:
 - use jsonpath to query json-objects
 - implements lists and hash datatypes
"""

import asyncio
import uvloop
import aredis
import objectpath
import time

from asyncdb.exceptions import *
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)
from asyncdb.utils import *

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class asyncredisPool(BasePool):
    _dsn = "redis://{host}:{port}/{db}"
    _max_queries = 10
    _pool = None
    _connection = None
    _encoding = "utf-8"

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        super(asyncredisPool, self).__init__(dsn=dsn, loop=loop, params=params)
        self._pool = None
        try:
            if params["encoding"]:
                self._encoding = params["encoding"]
        except KeyError:
            pass
        if 'max_queries' in kwargs:
            self._max_queries = kwargs["max_queries"]

    # Create a redis connection pool
    async def connect(self, **kwargs):
        """
        __init async db initialization
        """
        self.logger.debug(
            "Asyncio Redis Pool: Connecting to {}".format(self._dsn)
        )
        try:
            self._pool = aredis.ConnectionPool.from_url(
                self._dsn,
                connection_class=aredis.Connection,
                max_connections=self._max_queries,
                **kwargs
            )
        except (
            aredis.exceptions.ConnectionError,
            aredis.exceptions.RedisError
        ) as err:
            raise ProviderError(
                "Connection error to Redis: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError(
                "Unable to connect to Redis: {}".format(str(err))
            )
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
            if not self._pool:
                await self._pool.connect()
            self._connection = self._pool.get_connection()
        except (
            aredis.exceptions.ConnectionError,
            aredis.exceptions.RedisError
        ) as err:
            raise ConnectionError(
                "Redis Pool is closed o doesnt exists: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError("Redis Pool Acquire Error: {}".format(str(err)))
            return False
        if self._connection:
            db = asyncredis(
                    connection=self._connection,
                    pool=self
                )
        return db

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        else:
            if isinstance(connection, asyncredis):
                conn = connection.engine()
            if not conn:
                return True
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
            if self._connection:
                self._pool.release(self._connection)
            if self._pool:
                self._pool.disconnect()
        except (
            aredis.exceptions.ConnectionError,
            aredis.exceptions.RedisError
        ) as err:
            raise ConnectionError(
                "Redis Pool is closed o doesnt exists: {}".format(str(err))
            )
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
            except (
                aredis.exceptions.ConnectionError,
                aredis.exceptions.RedisError
            ) as err:
                raise ProviderError(
                    "Connection close Error: {}".format(str(err))
                )
            except Exception as err:
                raise ProviderError("Redis Execute Error: {}".format(str(err)))


class asyncredis(BaseProvider):
    _provider = "redis"
    _syntax = "json"
    _pool = None
    _dsn = "redis://{host}:{port}/{db}"
    _connection = None
    _connected = False
    _loop = None
    _encoding = "utf-8"

    def __init__(self,
                 dsn="",
                 connection=None,
                 pool=None,
                 loop=None,
                 params={}
                 ):
        super(asyncredis, self).__init__(dsn=dsn, loop=loop, params=params)
        if pool:
            self._pool = pool
            self._loop = self._pool.get_loop()
            self._connection = connection
        try:
            if params["encoding"]:
                self._encoding = params["encoding"]
        except KeyError:
            pass

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

    def loop(self):
        return self._loop

    @property
    def redis(self):
        return self._connection

    # Create a redis pool
    async def connection(self):
        """
        __init async redis initialization
        """
        self.logger.info("AsyncRedis: Connecting to {}".format(self._dsn))
        try:
            self._connection = aredis.StrictRedis.from_url(
                self._dsn,
                loop=self._loop,
                encoding=self._encoding,
                max_idle_time=0.2,
                idle_check_interval=0.1,
                connect_timeout=self._timeout
            )
            #await self._connection.connect()
        except (
            aredis.exceptions.ConnectionError,
            aredis.exceptions.RedisError
        ) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err)
                )
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
            await self._pool.release(connection=self._connection)

    async def close(self):
        if self._connection:
            try:
                for conn in self._connection.connection_pool._available_connections:
                    conn.disconnect()
                    conn = None
            except Exception as err:
                logging.exception('Error closing Redis Connection')
            finally:
                self._connection = None

    async def execute(self, sentence, *args):
        pass

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
        except (aredis.exceptions.RedisError) as err:
            raise ProviderError("Redis Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Redis Unknown Error: {}".format(str(err)))

    async def get(self, key):
        try:
            return await self._connection.get(key)
        except (aredis.exceptions.RedisError) as err:
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
            print(f"Error cleaning Cache: {e!s}")
            raise Exception(f"Error cleaning Cache: {e!s}")

    async def clear_db(self):
        """
        Clear a cache
        """
        try:
            return await self._connection.flushdb()
        except Exception as e:
            self._logger.error(f"Error cleaning DB: {e!s}")
            raise Exception(f"Error cleaning DB: {e!s}")

    async def exists(self, key, *keys):
        if not self._connection:
            await self.connection()
        try:
            return self._connection.exists(key, *keys)
        except (aredis.exceptions.RedisError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err))
            )

    async def delete(self, key, *keys):
        try:
            return await self._connection.delete(key, *keys)
        except (aredis.exceptions.RedisError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err))
            )

    async def expire(self, key, seconds=0):
        try:
            return await self._connection.expire(key, seconds)
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration Number: {}".format(str(seconds))
            )
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err))
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
        if not self._connection:
            await self.connection()
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
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
        except (aredis.exceptions.RedisError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err))
            )


"""
Registering this Provider
"""
registerProvider(asyncredis)
