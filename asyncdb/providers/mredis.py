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

import objectpath
import redis
import time

from asyncdb.exceptions import *
from .base import (
    BasePool,
    InitProvider,
)
from .interfaces import (
    ConnectionDSNBackend
)


class mredis(InitProvider, ConnectionDSNBackend):
    _provider = "redis"
    _syntax = "json"
    _encoding = "utf-8"

    def __init__(self, dsn="", loop=None, pool=None, params={}, **kwargs):
        self._dsn = "redis://{host}:{port}/{db}"
        InitProvider.__init__(
            self,
            loop=loop,
            params=params,
            **kwargs
        )
        ConnectionDSNBackend.__init__(
            self,
            dsn=dsn,
            params=params,
            **kwargs
        )
        try:
            self._encoding = params["encoding"]
            del params["encoding"]
        except KeyError:
            pass

    """
    Context magic Methods
    """

    def __enter__(self):
        if not self._connection:
            self.connection()
        return self

    def __exit__(self, *args):
        self.release()

    """
    Properties
    """
    @property
    def redis(self):
        return self._connection

    # Create a redis connection
    def connection(self, **kwargs):
        """
        __init redis initialization
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
            self._logger.debug("Redis: Connecting to {}".format(self._params))
            self._connection = redis.Redis(connection_pool=self._pool, **args)
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except (redis.exceptions.ConnectionError) as err:
            raise ProviderError(
                "Unable to connect to Redis, connection Refused: {}".format(
                    str(err))
            )
        except (redis.exceptions.RedisError, redis.exceptions.TimeoutError) as err:
            raise ConnectionTimeout(
                "Unable to connect to Redis: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Redis Error: {}".format(str(err)))
            return False
        finally:
            return self

    def release(self, connection=None):
        """
        Release a connection and return into pool
        """
        if not connection:
            connection = self._connection
        try:
            self._pool.release(connection=connection)
        except Exception as err:
            self._logger.exception(err)
        self._connection = None

    def close(self):
        if self._connection:
            self._connection.close()
        if self._pool:
            self._pool.disconnect(inuse_connections=True)
            self._connected = False

    def is_closed(self):
        return not self._connected

    disconnect = close

    def execute(self, sentence, *args):
        pass
        if self._connection:
            try:
                result = self._connection.send_command(*args)
                return result
            except (
                redis.exceptions.RedisError,
                redis.exceptions.ConnectionError,
            ) as err:
                raise ProviderError("Connection Error: {}".format(str(err)))

    execute_many = execute

    def use(self, database):
        raise NotImplementedError

    def prepare(self):
        pass

    def query(self, key="", *val):
        return self.get(key, val)

    fetch_all = query

    def queryrow(self, key="", *args):
        return self.get(key, val)

    fetch_one = queryrow

    def set(self, key, value):
        try:
            return self._connection.set(key, value)
        except (redis.exceptions.RedisError) as err:
            raise ProviderError("Redis Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Redis Unknown Error: {}".format(str(err)))

    def get(self, key):
        try:
            return self._connection.get(key)
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Redis Unknown Error: {}".format(str(err)))

    def clear_redis(self):
        """
        Clear a cache
        """
        try:
            return self._connection.flushall()
        except Exception as e:
            print("Error cleaning cache: %s" % e)
            raise Exception
            return False

    def exists(self, key, *keys):
        try:
            return bool(self._connection.exists(key, *keys))
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err)))

    def delete(self, key, *keys):
        try:
            if keys:
                return self._connection.delete(*keys)
            else:
                return self._connection.delete(key, *keys)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Exists Unknown Error: {}".format(str(err)))

    def expire_at(self, key, timestamp):
        try:
            return self._connection.expireat(key, timestamp)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration timestamp: {}".format(str(timestamp))
            )
        except Exception as err:
            raise ProviderError(
                "Redis Expiration Unknown Error: {}".format(str(err)))

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
            time = 900
        else:
            time = timeout
        try:
            self._connection.setex(key, time, value)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except TypeError:
            raise ProviderError(
                "Redis: wrong Expiration timestamp: {}".format(str(timestamp))
            )
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
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

    def set_key(self, key, value):
        self.set(key, value)

    def get_key(self, key):
        return self.get(key)

    """
     Hash functions
    """

    def hmset(self, key, value, *args, **kwargs):
        """
        set the value of a key in field (redis dict)
        """
        try:
            self._connection.hmset(key, value)
        except (redis.exceptions.ReadOnlyError) as err:
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

    def hgetall(self, key):
        """
        Get all the fields and values in a hash (redis dict)
        """
        try:
            return self._connection.hgetall(key)
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Hmset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hmset Unknown Error: {}".format(str(err)))

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
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis hkeys Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis hkeys Unknown Error: {}".format(str(err)))

    def hvals(self, key):
        """
        Get the keys in a hash (redis dict)
        """
        try:
            return self._connection.hvals(key)
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis hvals Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis hvals Unknown Error: {}".format(str(err)))

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
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err)))

    def hget(self, key, field):
        """
        get the value of a hash field (redis dict)
        """
        try:
            return self._connection.hget(key, field)
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Hget Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hget Unknown Error: {}".format(str(err)))

    def hexists(self, key, field, value):
        """
        Determine if hash field exists on redis dict
        """
        try:
            return self._connection.hexists(key, field)
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis hash exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis hash exists Unknown Error: {}".format(str(err)))

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
            raise ProviderError("Redis is Read Only: {}".format(str(err)))
        except (redis.exceptions.RedisError, redis.exceptions.ResponseError) as err:
            raise ProviderError("Redis Hset Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Redis Hset Unknown Error: {}".format(str(err)))

    def test_connection(self, optional=1):
        result = None
        error = None
        try:
            self.set("test_123", optional)
            result = self.get("test_123")
        except Exception as err:
            error = err
        finally:
            self.delete("test_123")
            return [result, error]