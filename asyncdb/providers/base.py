# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Callable,
    Optional,
    Any,
    Iterable,
    List
)
from asyncdb.interfaces import (
    PoolBackend,
    ConnectionDSNBackend,
    ConnectionBackend,
    DatabaseBackend,
    CursorBackend
)
from asyncdb.exceptions import ProviderError, EmptyStatement


class BasePool(PoolBackend, ConnectionDSNBackend):
    """BasePool.

    Abstract Class to create Pool-based database connectors.
    """
    init_func: Optional[Callable] = None

    def __init__(self, dsn: str = "", loop=None, params={}, **kwargs):
        super(BasePool, self).__init__(
            dsn, loop, params, **kwargs
        )
        self._params = params.copy()

    """
    __init async db initialization
    """
    # Create a database connection pool
    @abstractmethod
    async def connect(self):
        pass

    """
    Take a connection from the pool.
    """

    @abstractmethod
    async def acquire(self):
        pass

    """
    Close Pool
    """

    @abstractmethod
    async def close(self, **kwargs):
        pass

    """
    Release a connection from the pool
    """

    @abstractmethod
    async def release(self, connection, timeout=10):
        pass


class BaseProvider(ConnectionBackend, ConnectionDSNBackend, DatabaseBackend):
    """
    BaseProvider
        Abstract Class for DB Connection
    ----
    """
    _provider: str = "base"
    _syntax: str = "base"  # can use QueryParser for parsing SQL queries
    init_func: Optional[Callable] = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        self._pool = None
        self._max_connections = 4
        self._generated = None
        self._starttime = None
        self._parameters = ()
        ConnectionDSNBackend.__init__(self, dsn, loop, params, **kwargs)
        ConnectionBackend.__init__(self, loop, params, **kwargs)
        DatabaseBackend.__init__(self, params, **kwargs)
        self._initialized_on = None

    def start_timing(self):
        self._starttime = datetime.now()

    def generated_at(self):
        self._generated = datetime.now() - self._starttime
        return self._generated


class BaseDBProvider(BaseProvider):
    """
    Interface for more DB-oriented connections.
    """
    @abstractmethod
    def tables(self, schema: str = "") -> Iterable[Any]:
        pass

    @abstractmethod
    def table(self, tablename: str = "") -> Iterable[Any]:
        pass

    @abstractmethod
    async def column_info(
            self,
            tablename: str,
            schema: str = ''
    ) -> Iterable[Any]:
        """
        Getting Column info from an existing Table in Provider.
        """
        pass


class SQLProvider(BaseDBProvider):
    """SQLProvider.

    Driver for SQL-based providers.
    """
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop=None, params={}, **kwargs):
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        super(SQLProvider, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs
        )

    async def close(self, timeout: int = 5):
        """
        Closing Method for any SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError(
                f"{__name__!s}: Closing Error: {err!s}"
            )
        finally:
            self._connection = None
            self._connected = False
            return True

    # alias for connection
    disconnect = close

    async def valid_operation(self, sentence: Any):
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: cannot use an empty SQL sentence"
            )
        if not self._connection:
            await self.connection()


class BaseCursor(CursorBackend):
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """
    _provider: BaseProvider


class DDLBackend(ABC):
    """
    DDL Backend for Creation of SQL Objects.
    """

    @abstractmethod
    async def create(
        self,
        object: str = 'table',
        name: str = '',
        fields: Optional[List] = None
    ) -> Optional[Any]:
        """
        Create is a generic method for Database Objects Creation.
        """
        pass
