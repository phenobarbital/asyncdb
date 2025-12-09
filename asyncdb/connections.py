from typing import TypeVar, Type
import logging
from .exceptions import DriverError
from .interfaces.abstract import AbstractDriver
from .utils.modules import module_exists
from .utils import install_uvloop


T_aobj = TypeVar("T_aobj", bound="asyncdb")
install_uvloop()


class AsyncPool:
    """
    AsyncPool.
    Base class for Asyncio-based DB Pools.
    Factory interface for Pool-based connectors.
    """

    def __new__(cls: Type[T_aobj], driver: str = "dummy", **kwargs) -> AbstractDriver:
        classpath = f"asyncdb.drivers.{driver}"
        pool = f"{driver}Pool"
        try:
            mdl = module_exists(pool, classpath)
            return mdl(**kwargs)
        except Exception as err:
            logging.exception(err)
            raise DriverError(message=f"Cannot Load Backend Pool: {pool}") from err


class AsyncDB:
    """AsyncDB.

    Factory Proxy Interface for Database Providers.
    """

    def __new__(cls: Type[T_aobj], driver: str = "dummy", **kwargs) -> AbstractDriver:
        classpath = f"asyncdb.drivers.{driver}"
        try:
            mdl = module_exists(driver, classpath)
            return mdl(**kwargs)
        except Exception as err:
            logging.exception(err)
            raise DriverError(message=f"Cannot Load Backend {driver}") from err


def asyncdb(driver: str = "pg", *args, **kwargs) -> T_aobj:
    """asyncdb.

    Async Context Manager for Database Drivers.
    """
    credentials = kwargs.pop("credentials", None)
    if credentials:
        kwargs["params"] = credentials
    # Create the Driver Instance
    clspath = f"asyncdb.drivers.{driver}"
    mdl = module_exists(driver, clspath)
    factory = mdl(*args, **kwargs)
    # Get the connection
    return factory.connection_context()
