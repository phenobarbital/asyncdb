import logging
from collections.abc import Callable
from .exceptions import (
    ProviderError
)
from .utils import module_exists
from .interfaces import PoolBackend, ConnectionBackend


class AsyncPool:
    """
    AsyncPool.
       Base class for Asyncio-based DB Pools.
       Factory interface for Pool-based connectors.
    """

    _provider: Callable = None
    _name: str = ""

    def __new__(cls, provider: str = "dummy", **kwargs) -> PoolBackend:
        cls._name = provider
        classpath = f"asyncdb.drivers.{provider}"
        pool = f"{provider}Pool"
        try:
            obj = module_exists(pool, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise ProviderError(
                    message=f"Cannot Load Backend Pool: {pool}"
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

    def __new__(cls, provider: str = "dummy", **kwargs) -> ConnectionBackend:
        cls._name = provider
        classpath = f"asyncdb.drivers.{provider}"
        try:
            obj = module_exists(cls._name, classpath)
            if obj:
                cls._provider = obj(**kwargs)
                return cls._provider
            else:
                raise ProviderError(
                    message=f"Cannot Load Backend {provider}"
                )
        except Exception as err:
            logging.exception(err)
            raise
