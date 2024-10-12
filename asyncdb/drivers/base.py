# -*- coding: utf-8 -*-
import sys
import asyncio
from typing import Union, Optional, Any
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import Iterable
import traceback
from ..exceptions import EmptyStatement
from ..interfaces import PoolBackend, ConnectionDSNBackend, ConnectionBackend, DatabaseBackend
from .outputs import OutputFactory


class BasePool(PoolBackend, ConnectionDSNBackend, ABC):
    """BasePool.

    Abstract Class to create Pool-based database connectors with DSN support.
    """

    def __init__(
        self,
        dsn: Union[str, None] = None,
        loop: asyncio.AbstractEventLoop = None,
        params: Optional[Union[dict, None]] = None,
        **kwargs,
    ):
        ConnectionDSNBackend.__init__(self, dsn=dsn, params=params)
        PoolBackend.__init__(self, loop=loop, params=params, **kwargs)


class InitDriver(ConnectionBackend, DatabaseBackend, ABC):
    """
    InitDriver
        Abstract Class for Simple Connections.
    ----
    """

    _provider: str = "init"
    _syntax: str = "init"

    def __init__(self, loop: Union[asyncio.AbstractEventLoop, None] = None, params: Union[dict, None] = None, **kwargs):
        if params is None:
            params = {}
        self._max_connections = 4
        self._parameters = ()
        # noinspection PyTypeChecker
        self._serializer: OutputFactory = None
        self._row_format = "native"
        ConnectionBackend.__init__(self, loop=loop, params=params, **kwargs)
        DatabaseBackend.__init__(self)
        self._initialized_on = None
        # always starts output format to native:
        self.output_format("native")
        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def row_format(self, frmt: str = "native"):
        """
        Formats:
        - row_format: run before query
        - output: runs in the return (serialization) of data
        """
        self._row_format = frmt

    async def output(self, result, error):
        # return result in default format
        self._result = result
        return [result, error]

    def output_format(self, frmt: str = "native", *args, **kwargs):  # pylint: disable=W1113
        self._serializer = OutputFactory(self, frmt=frmt, *args, **kwargs)

    async def valid_operation(self, sentence: Any):
        """
        Returns if is a valid operation.
        TODO: add some validations.
        """
        if not sentence:
            raise EmptyStatement(f"{__name__!s} Error: cannot use an empty sentence")
        if not self._connection:
            await self.connection()


class BaseDriver(InitDriver, ConnectionDSNBackend, ABC):
    """
    BaseDriver
        Abstract Class for Database Connections.
    ----
    """

    _provider: str = "base"
    _syntax: str = "base"  # can use QueryParser for parsing SQL queries

    def __init__(
        self,
        dsn: Union[str, None] = None,
        loop: asyncio.AbstractEventLoop = None,
        pool: Optional[BasePool] = None,
        params: dict = None,
        **kwargs,
    ):
        InitDriver.__init__(self, loop=loop, params=params, **kwargs)
        ConnectionDSNBackend.__init__(self, dsn=dsn, params=params)
        # always starts output format to native:
        self.output_format("native")
        self._pool = None
        if pool:
            self._pool = pool
            self._loop = self._pool.get_loop()


class BaseDBDriver(BaseDriver):
    """
    Interface for more DB-oriented connections.
    """

    @abstractmethod
    def tables(self, schema: str = "") -> Iterable[Any]:
        """tables.
        Getting a list of tables in schema.
        """

    @abstractmethod
    def table(self, tablename: str = "") -> Iterable[Any]:
        """table.
        Getting table structure in schema.
        """

    @abstractmethod
    async def column_info(self, tablename: str, schema: str = "") -> Iterable[Any]:
        """
        Getting Column info from an existing Table in Driver.
        """
