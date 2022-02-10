"""AsyncDB.

Asyncio-based database connectors for NAV.
"""
# -*- coding: utf-8 -*-
import asyncio
import uvloop

from .meta import (
    asyncORM,
    asyncRecord,
)
from asyncdb.exceptions import (
    ProviderError,
    asyncDBException,
)
from asyncdb.providers import (
    _PROVIDERS,
    registerProvider
)
from asyncdb.utils.functions import module_exists

__version__ = '1.8.0'
__all__ = ["asyncORM", "asyncRecord", "registerProvider"]

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AsyncPool:
    """
    AsyncPool.
       Base class for Asyncio-based DB Pools.
       Factory interface for Pool-based connectors.
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


class AsyncDB:
    """AsyncDB.

    Factory Proxy Interfaces for Database Providers.
    """
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
