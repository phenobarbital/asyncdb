#!/usr/bin/env python3
""" memcache no-async Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities from aiomcache, this is a WIP
TODO: add Thread Pool Support.
"""
import asyncio
import time
from typing import Any
import pylibmc
from asyncdb.exceptions import (
    ProviderError,
    DriverError
)
from .abstract import (
    InitDriver,
)


class mcache(InitDriver):
    _provider = "memcache"
    _syntax = "nosql"
    _behaviors = {"tcp_nodelay": True, "ketama": True}

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        super(mcache, self).__init__(loop=loop, params=params, **kwargs)
        try:
            host = params["host"]
        except KeyError as ex:
            raise DriverError(
                "Memcache: Unable to find *host* in parameters."
            ) from ex
        try:
            port = params["port"]
        except KeyError:
            port = 11211
        self._server = [f"{host}:{port}"]
        try:
            if kwargs["behaviors"]:
                self._behaviors = {
                    **self._behaviors, **kwargs["behaviors"]
                }
        except KeyError:
            pass

### Context magic Methods
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    # Create a memcache Connection
    def connection(self): # pylint: disable=W0236
        """
        __init Memcache initialization.
        """
        self._logger.info(
            f"Memcache: Connecting to {self._server}"
        )
        try:
            self._connection = pylibmc.Client(
                self._server,
                binary=True,
                behaviors=self._behaviors
            )
        except (pylibmc.Error) as err:
            raise ProviderError(
                message=f"Connection Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                message=f"Unknown Memcache Error: {err}"
            ) from err
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    def release(self):
        """
        Release all connections
        """
        self._connection.disconnect_all()

    def close(self): # pylint: disable=W0221,W0236
        """
        Closing memcache Connection
        """
        try:
            self._connection.disconnect_all()
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"Close Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
               f"Unknown Memcache Closing Error: {err}"
            ) from err

    disconnect = close

    def flush(self):
        """
        Flush all elements inmediately
        """
        try:
            if self._connection:
                self._connection.flush_all()
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"Close Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Unknown Memcache Error: {err}"
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

    def execute(self, sentence: Any): # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def execute_many(self, sentence=""): # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def prepare(self, sentence=""):
        raise NotImplementedError

    async def use(self, database=""):
        raise NotImplementedError

    def query(self, key: str, *val): # pylint: disable=W0221,W0236
        return self.get_multi(key, val)

    fetch_all = query

    def queryrow(self, key: str, *args): # pylint: disable=W0221,W0236
        return self.get(key, *args)

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
            raise ProviderError(
                f"Set Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    def set_multi(self, mapping, timeout=0):
        try:
            self._connection.set_multi(mapping, timeout)
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"Set Memcache Error: {err}"
            ) from err

    def get(self, key, default=None):
        try:
            result = self._connection.get(bytes(key, "utf-8"), default)
            if result:
                return result.decode("utf-8")
            else:
                return None
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"Get Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Memcache Unknown Error: {err}"
            ) from err

    def get_multi(self, *kwargs):
        return self.multiget(kwargs)

    def delete(self, key):
        try:
            return self._connection.delete(bytes(key, "utf-8"))
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"DELETE Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"DELETE Unknown Error: {err}"
            ) from err

    def delete_multi(self, *kwargs):
        try:
            ky = [bytes(key, "utf-8") for key in kwargs]
            result = self._connection.delete_multi(ky)
            return result
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"DELETE Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"DELETE Unknown Error: {err}"
            ) from err

    def multiget(self, *kwargs):
        try:
            ky = [bytes(key, "utf-8") for key in kwargs]
            result = self._connection.get_multi(ky)
            if result:
                return {key.decode("utf-8"): value for key, value in result.items()}
        except (pylibmc.Error) as err:
            raise ProviderError(
                f"MULTI Memcache Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"MULTI Unknown Error: {err}"
            ) from err
