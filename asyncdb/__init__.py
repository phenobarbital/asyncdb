# -*- coding: utf-8 -*-
import asyncio
import importlib
import logging
import sys

# Version of asyncdb  package
__version__ = "2.0.0"


from .meta import asyncORM, asyncRecord

__all__ = ["asyncORM", "asyncRecord"]

# from .providers import *
from .exceptions import NotSupported, ProviderError, asyncDBException


def module_exists(module_name, classpath):
    try:
        # try to using importlib
        module = importlib.import_module(classpath, package="providers")
        obj = getattr(module, module_name)
        return obj
    except ImportError:
        try:
            # try to using __import__
            obj = __import__(classpath, fromlist=[module_name])
            return obj
        except ImportError:
            logging.exception(f"No Driver for provider {module_name} was found")
            raise NotSupported(message=f"No Provider {module_name} Found", code=404)


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
        # logger.info('Load Pool Provider: {}'.format(self._name))
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
        logging.debug(f"Loading Provider {cls._name}")
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
