from typing import Any, Optional, Union
from collections.abc import Callable, Awaitable
from abc import ABC
import asyncio
from datetime import datetime
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .abstract import AbstractDriver, DriverContextManager, EventLoopManager
from ..exceptions import DriverError
from ..utils.types import SafeDict


class ConnectionBackend(AbstractDriver, DriverContextManager, EventLoopManager):
    """
    Basic Interface with basic methods for open and close database connections.
    """

    _provider: str = "connection"
    _syntax: str = ""  # Used by QueryParser for parsing queries
    _init_func: Optional[Callable] = None  # a function called when connection is made

    def __init__(self, params: dict[Any] = Any, **kwargs) -> None:
        if "credentials" in kwargs:
            params = kwargs.get("credentials", {})
        AbstractDriver.__init__(self, **kwargs)
        DriverContextManager.__init__(
            self,
            **kwargs,
        )
        EventLoopManager.__init__(self, **kwargs)
        self._cursor = None
        self._generated: datetime = None
        self._starttime: datetime = None
        self._executor = None
        self._max_queries = kwargs.get("max_queries", 300)
        try:
            self.params = params.copy()
        except (TypeError, AttributeError, ValueError):
            self.params = {}
        # Debug:
        try:
            self._debug = bool(params.get("DEBUG", False))
        except (TypeError, KeyError, AttributeError):
            self._debug = kwargs.get("debug", False)
        # Executor:
        self._executor = None

    # Properties
    @classmethod
    def type(cls):
        return cls._provider

    def start_timing(self):
        self._starttime = datetime.now()
        return self._starttime

    def generated_at(self, started: Union[datetime, None] = None):
        if not started:
            started = datetime.now()
        try:
            self._generated = started - self._starttime
        except TypeError:
            return None
        return self._generated

    def last_duration(self):
        return self._generated

    @classmethod
    def driver(cls):
        return cls.__name__

    @classmethod
    def dialect(cls):
        return cls._syntax

    def get_executor(self, executor="thread", max_workers: int = 2) -> Any:
        if executor == "thread":
            return ThreadPoolExecutor(max_workers=max_workers)
        elif executor == "process":
            return ProcessPoolExecutor(max_workers=max_workers)
        elif self._executor is not None:
            return self._executor
        else:
            return None

    async def _thread_func(self, fn: Union[Callable, Awaitable], *args, executor: Any = None, **kwargs):
        """_execute.

        Returns a future to be executed into a Thread Pool.
        """
        loop = asyncio.get_event_loop()
        func = partial(fn, *args, **kwargs)
        if not executor:
            executor = self._executor
        try:
            fut = loop.run_in_executor(executor, func)
            return await fut
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise


class ConnectionDSNBackend(ABC):
    """
    Interface for Databases with DSN Support.
    """

    def __init__(self, dsn: str = None, params: Optional[dict] = None) -> None:
        if dsn:
            self._dsn = dsn
        else:
            self._dsn = self.create_dsn(params)
        try:
            self._params = params.copy()
        except (TypeError, AttributeError, ValueError):
            self._params = {}

    def create_dsn(self, params: dict):
        try:
            return self._dsn.format_map(SafeDict(**params)) if params else None
        except TypeError as err:
            self._logger.error(err)
            raise DriverError(f"Error creating DSN connection: {err}") from err

    def get_dsn(self):
        return self._dsn
