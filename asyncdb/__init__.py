# -*- coding: utf-8 -*-
"""AsyncDB.

Asyncio-based database connectors for NAV.
"""
import asyncio
import uvloop

from .meta import (
    asyncORM,
    asyncRecord,
)
from .exceptions import (
    ProviderError,
    asyncDBException,
)
from .utils import module_exists
from .version import (
    __title__, __description__, __version__, __author__, __author_email__
)
from .providers import (
    _PROVIDERS,
    registerProvider,
    InitProvider,
    BaseProvider
)

__all__ = ["InitProvider", "BaseProvider"]

# install uvloop and set as default loop for asyncio.
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


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
        except Exception:
            raise


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
        except Exception:
            raise
