"""AsyncDB.

Asyncio-based database connectors for NAV.
"""
# -*- coding: utf-8 -*-
import asyncio
import uvloop
import importlib
import logging
import sys

from .meta import (
    asyncORM,
    asyncRecord,
)
__version__ = '1.7.1'

__all__ = ["asyncORM", "asyncRecord"]

# from .providers import *
from .exceptions import (
    NotSupported,
    ProviderError,
    asyncDBException,
)

from .providers import _PROVIDERS
from .utils.functions import module_exists

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Factory interface for Pool-based connectors
class AsyncPool:
    """
    AsyncPool.
       Base class for Asyncio-based DB Pools.
    """

    _provider = None
    _name = ""

    def __new__(cls, provider="dummy", **kwargs):
        cls._provider = None
        cls._name = provider
        classpath = "asyncdb.providers.{provider}".format(provider=cls._name)
        poolName = "{}Pool".format(cls._name)
        try:
            obj = module_exists(poolName, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise asyncDBException(
                    message="Cannot Load Pool provider {}".format(poolName)
                )
        except Exception as err:
            raise ProviderError(message=str(err), code=404)


# Factory Proxy Interfaces for Providers
class AsyncDB:
    _provider = None
    _name = ""

    def __new__(cls, provider="dummy", **kwargs):
        cls._provider = None
        cls._name = provider
        if provider in _PROVIDERS:
            obj = _PROVIDERS[provider]
            cls._provider = obj(**kwargs)
            return cls._provider
        classpath = "asyncdb.providers.{provider}".format(provider=cls._name)
        try:
            obj = module_exists(cls._name, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise asyncDBException(
                    message="Cannot Load provider {}".format(cls._name)
                )
        except Exception as err:
            raise ProviderError(message=str(err), code=404)
