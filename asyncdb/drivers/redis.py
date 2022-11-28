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
import time
from typing import Any, Union

import aioredis
import uvloop
from aioredis.exceptions import AuthenticationError, RedisError

from asyncdb.exceptions import ConnectionTimeout, DriverError, ProviderError

from .abstract import BaseDriver, BasePool

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


class redisPool(BasePool):

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
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
        self._logger.debug(
            f"Redis Pool: Connecting to {self._dsn}"
        )
        try:
            self._pool = aioredis.ConnectionPool.from_url(
                self._dsn,
                encoding=self._encoding,
                decode_responses=True,
                max_connections=self._max_queries,
                **kwargs,
            )
            self._connection = aioredis.Redis(connection_pool=self._pool)
        except (ConnectionError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except (RedisError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(f"Unknown Error: {err}") from err
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
        except (ConnectionError) as err:
            raise ConnectionError(
                f"Redis Pool is already closed: {err}"
            ) from err
        except (RedisError) as err:
            raise ConnectionError(
                f"Redis Pool is closed o doesnt exists: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Unknown Error: {err}"
            ) from err

    async def release(self, connection: "redis " = None): # pylint: disable=W0221
        """
        Release a connection from the pool
        """
        if not connection:
            return True
        try:
            print(type(connection.get_connection()))
            await self._pool.release(connection.get_connection())
        except Exception as err:
            raise ProviderError(
                f"Release Error: {err}"
            ) from err

    async def close(self): # pylint: disable=W0221
        """
        Close Pool
        """
        try:
            if self._connection is not None:
                await self._connection.close()
            if self._pool:
                await self._pool.disconnect(inuse_connections=True)
            self._connected = False
            return True
        except (ConnectionError) as err:
            raise ProviderError(
                f"Connection close Error: {err}"
            ) from err
        except Exception as err:
            self._logger.exception(
                f"Pool Closing Error: {err}"
            )
            raise DriverError(
                f"Connection close Error: {err}"
            ) from err

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
                raise ProviderError(
                    f"Execute Error: {err}"
                ) from err
            except aioredis.exceptions.ConnectionError as err:
                raise ProviderError(
                    f"Connection cannot be decoded or is broken, Error: {err}"
                ) from err
            except RedisError as err:
                raise ProviderError(
                    f"Connection close Error: {err}"
                ) from err
            except Exception as err:
                raise ProviderError(
                    f"Redis Execute Error: {err}"
                ) from err


class redis(BaseDriver):
    _provider = "redis"
    _syntax = "json"

    def __init__(self, dsn: str = None, loop=None, params: dict = None, **kwargs):
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

### Properties
    @property
    def redis(self):
        return self._connection

    # Create a redis pool
    async def connection(self, **kwargs):
        """
        __init async redis initialization
        """
        self._logger.info(
            f"REDIS: Connecting to {self._dsn}"
        )
        try:
            self._connection = await aioredis.from_url(
                self._dsn,
                encoding=self._encoding,
                decode_responses=True,
                **kwargs,
            )
        except AuthenticationError as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except ConnectionError as err:
            raise ProviderError(
                f"Connection Error: {err}"
            ) from err
        except (aioredis.RedisError, asyncio.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err
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

    async def ping(self, msg: str = None):
        if msg is not None:
            await self._connection.echo(msg)
        await self._connection.ping()

    async def close(self, timeout: int = 10):
        try:
            # gracefully closing underlying connection
            await self._connection.close()
            try:
                # safely closing the inner connection pool
                await self._connection.connection_pool.disconnect()
                self._connected = False
            except Exception as err:
                raise ProviderError(
                    f"Unknown Redis Error: {err}"
                ) from err
        except (RuntimeError, AttributeError):
            pass
        except Exception as err:
            self._logger.exception(f'Redis Closing Error: {err}')
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    disconnect = close

    async def execute(self, sentence, *args, **kwargs) -> Any:
        """execute.
        Raises:
            ProviderError: Error on execution.

        Returns:
            Any: _description_
        """
        if self._connection:
            try:
                result = await self._connection.execute_command(sentence, *args)
                return result
            except (
                RedisError,
            ) as err:
                raise ProviderError(
                    f"Connection Error: {err}"
                ) from err

    execute_many = execute

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError()  # pragma: no-cover

    async def test_connection(self, key: str = 'test_123', optional: int = 1):  # pylint: disable=W0221
        result = None
        error = None
        try:
            await self.set(key, optional)
            result = await self.get(key)
        except Exception as err:  # pylint: disable=W0703
            error = err
        finally:
            await self.delete(key)
            return [result, error]  # pylint: disable=W0150

    async def get(self, key):
        try:
            return await self._connection.get(key)
        except (aioredis.RedisError) as err:
            raise ProviderError(
                f"Redis Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Unknown Error: {err}"
            ) from err

    async def query(self, sentence: str, **kwargs):
        return await self.get(sentence)

    async def queryrow(self, sentence: str):
        result = await self.get(sentence)
        if isinstance(result, list):
            result = result[0]
        return result

    async def set(self, key, value, **kwargs):
        try:
            return await self._connection.set(key, value, **kwargs)
        except (aioredis.RedisError) as err:
            raise ProviderError(
                f"Redis Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Unknown Error: {err}"
            ) from err

    async def use(self, database: int):
        try:
            await self._connection.execute_command("SELECT", database)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Can't change to DB: {err!s}"
            ) from err

    async def clear_redis(self, host: bool = True):
        """
        Clear a cache.
        """
        try:
            if host is True:
                return await self._connection.flushall()
            else:
                return await self._connection.flushdb()
        except Exception as ex:
            raise ProviderError(
                f"Redis: Error cleaning DB: {ex!s}"
            ) from ex

    async def exists(self, key, *keys):
        if not self._connection:
            await self.connection()
        try:
            return await self._connection.exists(key, *keys)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Exists: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Exists Unknown Error: {err}"
            ) from err

    async def delete(self, key, *keys):
        try:
            return await self._connection.delete(key, *keys)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Delete: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Delete Unknown Error: {err}"
            ) from err

    async def expire_at(self, key, timestamp):
        try:
            return await self._connection.expireat(key, timestamp)
        except TypeError as ex:
            raise ProviderError(
                f"Redis: wrong Expiration timestamp: {timestamp}"
            ) from ex
        except Exception as err:
            raise ProviderError(
                f"Redis Expiration Unknown Error: {err}"
            ) from err

    async def setex(self, key, value, timeout):
        """
        setex.
           Set the value and expiration of a Key.
           params:
            key: key Name
            value: value of the key
            timeout: expiration time in seconds
        """
        if not isinstance(timeout, int):
            expiration = 900
        else:
            expiration = timeout
        try:
            await self._connection.setex(key, expiration, value)
        except TypeError as ex:
            raise ProviderError(
                f"Redis: wrong Expiration timestamp: {expiration}"
            ) from ex
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on SetEX: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis SetEX Unknown Error: {err}"
            ) from err

    def persist(self, key):
        """
        persist.
            Remove the expiration of a key.
        """
        try:
            return self._connection.persist(key)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Persist: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Persist Unknown Error: {err}"
            ) from err

    async def set_key(self, key, value):
        await self.set(key, value)

    async def get_key(self, key):
        return await self.get(key)


### Hash functions
    async def hmset(self, name: str, info: dict):
        """
        set the value of a key in field (redis dict).
        """
        try:
            # await self._connection.hmset(name, mapping)
            await self._connection.hmset(name, mapping=info)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on hmset: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis hmset Unknown Error: {err}"
            ) from err

    async def hgetall(self, key):
        """
        Get all the fields and values in a hash (redis dict).
        """
        try:
            return await self._connection.hgetall(key)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on hgetall: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis hgetall Unknown Error: {err}"
            ) from err

    async def set_hash(self, key, kwargs):
        await self.hmset(key, kwargs)

    async def get_hash(self, key):
        return await self.hgetall(key)

    fetch_all = get_hash

    async def hkeys(self, key):
        """
        Get the keys in a hash (redis dict).
        """
        try:
            return await self._connection.hkeys(key)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on hkeys: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis hkeys Unknown Error: {err}"
            ) from err

    async def hlen(self, key):
        """
        Return the number of elements in hash *key* (redis dict).
        """
        try:
            return await self._connection.hlen(key)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on hlen: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis hlen Unknown Error: {err}"
            ) from err

    async def hvals(self, key):
        """
        Return the list of values within hash (redis dict).
        """
        try:
            return await self._connection.hvals(key)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on hvals: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis hvals Unknown Error: {err}"
            ) from err

    async def keys(self, key):
        return await self.hkeys(key)

    async def values(self, key):
        return await self.hvals(key)

    async def hset(self, key, field, value, mapping: dict = None):
        """
        Set field to value within hash with name *key*.
        """
        try:
            await self._connection.hset(key, key=field, value=value, mapping=mapping)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Hset: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Hset Unknown Error: {err}"
            ) from err

    async def hget(self, key, field):
        """
        get the value of a hash field (redis dict)
        """
        try:
            return await self._connection.hget(key, field)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Hget: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Hget Unknown Error: {err}"
            ) from err

    fetch_one = hget

    async def hexists(self, key, field):
        """
        Determine if hash field exists on redis dict *key*.
        """
        try:
            await self._connection.hexists(key, field)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Hexists: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Hexists Unknown Error: {err}"
            ) from err

    async def hdel(self, key, field, *fields):
        """
        Delete one or more hash fields from *key*.
        """
        try:
            await self._connection.hdel(key, field, *fields)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on HDel: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis HDel Unknown Error: {err}"
            ) from err

    async def mset(self, mapping):
        """
        Sets key/values based on a mapping.
        """
        try:
            await self._connection.mset(mapping)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Mset: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Mset Unknown Error: {err}"
            ) from err

    async def move(self, key, database):
        """
        Moves a key to another database.
        """
        try:
            await self._connection.move(key, database)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Move: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Move Unknown Error: {err}"
            ) from err

    async def lrange(self, key, start: int = 0, stop: int = 100):
        """
        Return a slice of the list key between position start and end.
        """
        try:
            await self._connection.lrange(key, start, stop)
        except ConnectionError as err:
            raise DriverError(
                f"Error connecting to Redis {err}"
            ) from err
        except RedisError as err:
            raise ProviderError(
                f"Redis: Error on Lrange: {err!s}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Redis Lrange Unknown Error: {err}"
            ) from err
