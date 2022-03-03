import logging
from typing import Callable
from .exceptions import (
    AsyncDBException,
)
from .utils import module_exists


class AsyncPool:
    """
    AsyncPool.
       Base class for Asyncio-based DB Pools.
       Factory interface for Pool-based connectors.
    """

    _provider: Callable = None
    _name: str = ""

    def __new__(cls, provider: str = "dummy", **kwargs) -> Callable:
        cls._name = provider
        classpath = "asyncdb.providers.{provider}".format(provider=cls._name)
        pool = "{}Pool".format(cls._name)
        try:
            obj = module_exists(pool, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise AsyncDBException(
                    message="Cannot Load Pool provider {}".format(pool)
                )
        except Exception as err:
            logging.exception(err)
            raise


class AsyncDB:
    """AsyncDB.

    Factory Proxy Interface for Database Providers.
    """
    _provider: Callable = None
    _name: str = ""

    def __new__(cls, provider: str = "dummy", **kwargs) -> Callable:
        cls._name = provider
        classpath = "asyncdb.providers.{provider}".format(provider=cls._name)
        try:
            obj = module_exists(cls._name, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise AsyncDBException(
                    message="Cannot Load provider {}".format(cls._name)
                )
        except Exception as err:
            logging.exception(err)
            raise
