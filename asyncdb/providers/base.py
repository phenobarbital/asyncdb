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
    Dict
)
from .interfaces import (
    PoolBackend,
    ConnectionDSNBackend,
    ConnectionBackend,
    DatabaseBackend,
    CursorBackend
)
from .outputs import OutputFactory
from asyncdb.exceptions import EmptyStatement
from asyncdb.models import Model


class BasePool(PoolBackend, ConnectionDSNBackend):
    """BasePool.

    Abstract Class to create Pool-based database connectors.
    """
    init_func: Optional[Callable] = None

    def __init__(self, dsn: str = "", loop=None, params: dict = None, **kwargs):
        ConnectionDSNBackend.__init__(
            self,
            dsn=dsn,
            params=params,
            **kwargs
        )
        PoolBackend.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)

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


class InitProvider(ConnectionBackend, DatabaseBackend, ABC):
    """
    InitProvider
        Abstract Class for Connections
    ----
    """
    _provider: str = "init"
    _syntax: str = "init"  # can use QueryParser for parsing SQL queries
    init_func: Optional[Callable] = None

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs):
        if params is None:
            params = {}
        self._pool = None
        self._max_connections = 4
        self._generated = None
        self._starttime = None
        self._parameters = ()
        # noinspection PyTypeChecker
        self._serializer: OutputFactory = None
        self._row_format = 'native'
        self._connected: bool = False
        self._connection = None
        ConnectionBackend.__init__(self, loop=loop, params=params, **kwargs)
        DatabaseBackend.__init__(self, params=params, **kwargs)
        self._initialized_on = None
        # always starts output format to native:
        self.output_format('native')

    def start_timing(self):
        self._starttime = datetime.now()

    def generated_at(self):
        self._generated = datetime.now() - self._starttime
        return self._generated

    def last_duration(self):
        return self._generated

    def set_connection(self, connection):
        self._connection = connection

    """
    Formats:
     - row_format: run before query
     - output: runs in the return (serialization) of data
    """

    def row_format(self, frmt: str = 'native'):
        self._row_format = frmt

    async def output(self, result, error):
        # return result in default format
        self._result = result
        return [result, error]

    def output_format(self, frmt: str = 'native', *args, **kwargs):
        self._serializer = OutputFactory(self, frmt, *args, **kwargs)

    async def valid_operation(self, sentence: Any):
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: cannot use an empty sentence"
            )
        if not self._connection:
            await self.connection()


class BaseProvider(InitProvider, ConnectionDSNBackend, ABC):
    """
    BaseProvider
        Abstract Class for DB Connection
    ----
    """
    _provider: str = "base"
    _syntax: str = "base"  # can use QueryParser for parsing SQL queries
    init_func: Optional[Callable] = None

    def __init__(self, dsn="", loop=None, params: dict = None, **kwargs):
        InitProvider.__init__(
            self,
            loop=loop,
            params=params,
            **kwargs
        )
        ConnectionDSNBackend.__init__(
            self,
            dsn=dsn,
            params=params,
            **kwargs
        )
        # always starts output format to native:
        self.output_format('native')


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


class BaseCursor(CursorBackend):
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """
    _provider: BaseProvider


class ModelBackend(ABC):
    """
    Interface for Backends with Dataclass-based Models Support.
    """

    """
    Class-based Methods.
    """
    @abstractmethod
    async def mdl_create(self, model: "Model", rows: list):
        """
        Create all records based on a dataset and return result.
        """
        pass

    @abstractmethod
    async def mdl_delete(self, model: "Model", conditions: dict, **kwargs):
        """
        Deleting some records using Model.
        """
        pass

    @abstractmethod
    async def mdl_update(self, model: "Model", conditions: dict, **kwargs):
        """
        Updating records using Model.
        """
        pass

    @abstractmethod
    async def mdl_filter(self, model: "Model", **kwargs):
        """
        Filter a Model based on some criteria.
        """
        pass

    @abstractmethod
    async def mdl_all(self, model: "Model", **kwargs):
        """
        Get all records on a Model.
        """
        pass

    @abstractmethod
    async def mdl_get(self, model: "Model", **kwargs):
        """
        Get one single record from Model.
        """
        pass

    @abstractmethod
    async def model_select(self, model: "Model", fields: Dict = None, **kwargs):
        """
        Get queries with model.
        """
        pass

    @abstractmethod
    async def model_all(self, model: "Model", fields: Dict = None):
        """
        Get queries with model.
        """
        pass

    @abstractmethod
    async def model_get(self, model: "Model", fields: Dict = None, **kwargs):
        """
        Get one row from model.
        """
        pass

    @abstractmethod
    async def model_delete(self, model: "Model", fields: Dict = None, **kwargs):
        """
        delete a row from model.
        """
        pass

    @abstractmethod
    async def model_save(self, model: "Model", fields: Dict = None, **kwargs):
        """
        save a row from model.
        """
        pass

    @abstractmethod
    async def model_insert(self, model: "Model", fields: Dict = None, **kwargs):
        """
        insert a row from model.
        """
        pass
