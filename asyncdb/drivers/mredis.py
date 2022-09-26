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
import redis
from asyncdb.exceptions import (
    ProviderError,
    DriverError,
    ConnectionTimeout
)
from asyncdb.interfaces import (
    ConnectionDSNBackend
)
from .abstract import (
    InitDriver,
)


class mredis(InitDriver, ConnectionDSNBackend):
    _provider = "redis"
    _syntax = "json"
    _encoding = "utf-8"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._dsn = "redis://{host}:{port}/{db}"
        InitDriver.__init__(
            self,
            loop=loop,
            params=params,
            **kwargs
        )
        ConnectionDSNBackend.__init__(
            self,
            dsn=dsn,
            params=params
        )
        try:
            self._encoding = params["encoding"]
            del params["encoding"]
        except KeyError:
            pass

### Context magic Methods
    def __enter__(self):
        if not self._connection:
            self.connection()
        return self

    def __exit__(self, *args):
        self.release()

    @property
    def redis(self):
        return self._connection

    # Create a redis connection
    def connection(self, **kwargs): # pylint: disable=W0236
        """
        __init redis initialization.
        """
        try:
            args = {
                "socket_timeout": self._timeout,
                "encoding": self._encoding,
                "decode_responses": True,
            }
            if self._dsn:
                self._pool = redis.ConnectionPool.from_url(
                    url=self._dsn, **args)
            else:
                self._pool = redis.ConnectionPool(**self._params)
            args = {**args, **kwargs}
            self._logger.debug(
                f"Redis: Connecting to {self._params}"
            )
            self._connection = redis.Redis(connection_pool=self._pool, **args)
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            return self
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def release(self, connection=None):
        """
        Release a connection and return into pool
        """
        if not connection:
            connection = self._connection
        try:
            self._pool.release(connection=connection)
        except Exception as err: # pylint: disable=W0703
            self._logger.exception(err)
        self._connection = None

    def close(self): # pylint: disable=W0236,W0221
        if self._connection:
            try:
                self._connection.close()
            except Exception as err: # pylint: disable=W0703
                self._logger.exception(err)
        if self._pool:
            try:
                self._pool.disconnect(inuse_connections=True)
            except Exception as err: # pylint: disable=W0703
                self._logger.exception(err)
        self._pool = None
        self._connected = False

    def is_closed(self):
        return not self._connected

    disconnect = close

    def execute(self, sentence, *args): # pylint: disable=W0236,W0221
        try:
            result = self._connection.send_command(*args)
            return result
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    execute_many = execute

    def use(self, database): # pylint: disable=W0236,W0221
        raise NotImplementedError

    def prepare(self): # pylint: disable=W0236,W0221
        raise NotImplementedError

    def get(self, key):
        try:
            return self._connection.get(key)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def query(self, sentence): # pylint: disable=W0236,W0221
        return self.get(sentence)

    fetch_all = query

    def queryrow(self, sentence): # pylint: disable=W0236,W0221
        result = self.get(sentence)
        if isinstance(result, list):
            result = result[0]
        return result

    fetch_one = queryrow

    def set(self, key, value):
        try:
            return self._connection.set(key, value)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def clear_redis(self, host: bool = True):
        """
        Clear a cache.
        """
        try:
            if host is True:
                return self._connection.flushall()
            else:
                return self._connection.flushdb()
        except Exception as ex:
            raise ProviderError(
                f"Redis: Error cleaning DB: {ex!s}"
            ) from ex

    def exists(self, key, *keys):
        try:
            return bool(self._connection.exists(key, *keys))
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def delete(self, key, *keys):
        try:
            if keys:
                return self._connection.delete(*keys)
            else:
                return self._connection.delete(key, *keys)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def expire_at(self, key, timestamp):
        try:
            return self._connection.expireat(key, timestamp)
        except TypeError as ex:
            raise ProviderError(
                f"Redis: wrong Expiration timestamp {timestamp}: {ex}"
            ) from ex
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def setex(self, key, value, timeout):
        """
        setex
           Set the value and expiration of a Key
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
            self._connection.setex(key, expiration, value)
        except TypeError as ex:
            raise ProviderError(
                f"Redis: wrong Expiration timestamp {expiration}: {ex}"
            ) from ex
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def persist(self, key):
        """
        persist
            Remove the expiration of a key
        """
        try:
            return self._connection.persist(key)
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Redis Protocol Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def set_key(self, key, value):
        self.set(key, value)

    def get_key(self, key):
        return self.get(key)

### Hash functions
    def hmset(self, key, value):
        """
        set the value of a key in field (redis dict)
        """
        try:
            self._connection.hmset(key, value)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def hgetall(self, key):
        """
        Get all the fields and values in a hash (redis dict)
        """
        try:
            return self._connection.hgetall(key)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def set_hash(self, key, *args, **kwargs):
        self.hmset(key, *args, **kwargs)

    def get_hash(self, key):
        return self.hgetall(key)

    def hkeys(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return self._connection.hkeys(key)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def hvals(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return self._connection.hvals(key)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def keys(self, key):
        return self.hkeys(key)

    def values(self, key):
        return self.hvals(key)

    def hset(self, key, field, value):
        """
        Set the string value of a hash field (redis dict)
        """
        try:
            return self._connection.hset(key, field, value)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def hget(self, key, field):
        """
        get the value of a hash field (redis dict)
        """
        try:
            return self._connection.hget(key, field)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def hexists(self, key, field):
        """
        Determine if hash field exists on redis dict
        """
        try:
            return self._connection.hexists(key, field)
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def hdel(self, key, field, *fields):
        """
        Delete one or more hash fields
        """
        try:
            if fields:
                return self._connection.hdel(key, *fields)
            else:
                return self._connection.hdel(key, field)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError(
                f"Redis is Read Only: {err}"
            ) from err
        except (redis.exceptions.ResponseError) as err:
            raise ProviderError(
                f"Redis Response Error: {err}"
            ) from err
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                f"Unable to connect to Redis, connection Refused: {err}"
            ) from err
        except (redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                f"Unable to connect to Redis: {err}"
            ) from err
        except redis.exceptions.RedisError as err:
            raise DriverError(
                f"Unable to connect to Redis: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Redis Error: {err}"
            ) from err

    def test_connection(self, key: str = 'test_123', optional: int = 1): # pylint: disable=W0221,W0236
        result = None
        error = None
        try:
            self.set(key, optional)
            result = self.get(key)
        except Exception as err: # pylint: disable=W0703
            error = err
        finally:
            self.delete(key)
            return [result, error] # pylint: disable=W0150
