#!/usr/bin/env python3
""" memcache no-async Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities from aiomcache, this is a WIP
"""

import asyncio
import pylibmc
import time

from asyncdb.exceptions import *

from .base import (
    BasePool,
    InitProvider,
)


class mcache(InitProvider):
    _provider = "memcache"
    _syntax = "nosql"
    _behaviors = {"tcp_nodelay": True, "ketama": True}

    def __init__(self, loop=None, params={}, **kwargs):
        super(mcache, self).__init__(loop=loop, params=params, **kwargs)
        self._server = ["{0}:{1}".format(params["host"], params["port"])]
        try:
            if kwargs["behaviors"]:
                self._behaviors = {
                    **self._behaviors, **kwargs["behaviors"]
                }
        except KeyError:
            pass

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    # Create a memcache Connection
    def connection(self):
        """
        __init Memcache initialization
        """
        self._logger.info("Memcache: Connecting to {}".format(self._server))
        try:
            self._connection = pylibmc.Client(
                self._server,
                binary=True,
                behaviors=self._behaviors
            )
        except (pylibmc.Error) as err:
            raise ProviderError(
                message="Connection Error: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError(
                message="Unknown Memcache Error: {}".format(str(err))
            )
            return False
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()

    def release(self):
        """
        Release all connections
        """
        self._connection.disconnect_all()

    def close(self):
        """
        Closing memcache Connection
        """
        try:
            self._connection.disconnect_all()
        except (pylibmc.Error) as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Unknown Memcache Closing Error: {}".format(str(err)))
            return False

    disconnect = close

    def flush(self):
        """
        Flush all elements inmediately
        """
        try:
            if self._connection:
                self._connection.flush_all()
        except (pylibmc.Error) as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Unknown Memcache Error: {}".format(str(err)))
            return False

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

    async def execute(self, sentence=""):
        pass

    async def execute_many(self, sentence=""):
        pass

    async def prepare(self, sentence=""):
        pass

    async def use(self, database=""):
        pass

    def query(self, key="", *val):
        return self.get_multi(key, val)

    fetch_all = query

    def queryrow(self, key="", *args):
        return self.get(key, val)

    fetch_one = queryrow

    def set(self, key, value, timeout=None):
        try:
            if timeout:
                return self._connection.set(
                    bytes(key, "utf-8"), bytes(value, "utf-8"), time=timeout
                )
            else:
                return self._connection.set(bytes(key, "utf-8"), bytes(value, "utf-8"))
        except (pylibmc.Error) as err:
            raise ProviderError("Set Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    def set_multi(self, map, timeout=0):
        try:
            self._connection.set_multi(map, timeout)
        except (pylibmc.Error) as err:
            raise ProviderError("Set Memcache Error: {}".format(str(err)))

    def get(self, key, default=None):
        try:
            result = self._connection.get(bytes(key, "utf-8"), default)
            if result:
                return result.decode("utf-8")
            else:
                return None
        except (pylibmc.Error) as err:
            raise ProviderError("Get Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    def get_multi(self, *kwargs):
        return self.multiget(kwargs)

    def delete(self, key):
        try:
            return self._connection.delete(bytes(key, "utf-8"))
        except (pylibmc.Error) as err:
            raise ProviderError("Memcache Exists Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError(
                "Memcache Exists Unknown Error: {}".format(str(err)))

    def delete_multi(self, *kwargs):
        try:
            ky = [bytes(key, "utf-8") for key in kwargs]
            result = self._connection.delete_multi(ky)
            return result
        except (pylibmc.Error) as err:
            raise ProviderError("Get Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))

    def multiget(self, *kwargs):
        try:
            ky = [bytes(key, "utf-8") for key in kwargs]
            result = self._connection.get_multi(ky)
            if result:
                return {key.decode("utf-8"): value for key, value in result.items()}
        except (pylibmc.Error) as err:
            raise ProviderError("Get Memcache Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Memcache Unknown Error: {}".format(str(err)))
