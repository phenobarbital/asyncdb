import logging

from .exceptions import ProviderError
from .interfaces import ConnectionBackend, PoolBackend
from .utils.modules import module_exists


class AsyncPool:
    """
    AsyncPool.
       Base class for Asyncio-based DB Pools.
       Factory interface for Pool-based connectors.
    """
    def __new__(cls, driver: str = "dummy", **kwargs) -> PoolBackend:
        classpath = f"asyncdb.drivers.{driver}"
        pool = f"{driver}Pool"
        try:
            mdl = module_exists(pool, classpath)
            obj = mdl(**kwargs)
            return obj
        except Exception as err:
            logging.exception(err)
            raise ProviderError(
                message=f"Cannot Load Backend Pool: {pool}"
            ) from err


class AsyncDB:
    """AsyncDB.

    Factory Proxy Interface for Database Providers.
    """
    def __new__(cls, driver: str = "dummy", **kwargs) -> ConnectionBackend:
        classpath = f"asyncdb.drivers.{driver}"
        try:
            mdl = module_exists(driver, classpath)
            obj = mdl(**kwargs)
            return obj
        except Exception as err:
            logging.exception(err)
            raise ProviderError(
                message=f"Cannot Load Backend {driver}"
            ) from err
