from typing import TypeVar, Type
import logging
from .exceptions import DriverError
from .interfaces.abstract import AbstractDriver
from .utils.modules import module_exists
from .utils import install_uvloop


T_aobj = TypeVar("T_aobj", bound="Asyncdb")
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
            obj = mdl(**kwargs)
            return obj
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
            obj = mdl(**kwargs)
            return obj
        except Exception as err:
            logging.exception(err)
            raise DriverError(message=f"Cannot Load Backend {driver}") from err


class Asyncdb:
    """
    Asyncdb.

    Getting a Database Driver Connection.
    """

    async def __new__(cls: Type[T_aobj], driver: str, *args, **kwargs) -> T_aobj:
        clspath = f"asyncdb.drivers.{driver}"
        mdl = module_exists(driver, clspath)
        obj = mdl(**kwargs)
        await obj.__ainit__(*args, **kwargs)
        return obj

    async def __call__(self):
        print(f"Running job {self.name} asynchronously.")

    async def __ainit__(self, *args, **kwargs) -> None:
        # Add any additional async initialization logic here
        pass
