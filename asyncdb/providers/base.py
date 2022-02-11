# -*- coding: utf-8 -*-
import logging
import asyncio
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Callable,
    Optional,
    Any
)
from asyncdb.exceptions import (
    default_exception_handler,
    ProviderError,
    EmptyStatement
)


class BasePool(ABC):
    """BasePool.

    Abstract Class to create Pool-based database connectors.
    """
    init_func: Optional[Callable] = None

    def __init__(self, dsn: str = "", loop=None, params={}, **kwargs):
        self._pool = None
        self._max_queries = 300
        self._connection = None
        self._connected = False
        if loop:
            self._loop = loop
            asyncio.set_event_loop(self._loop)
        else:
            self._loop = asyncio.get_event_loop()
            asyncio.set_event_loop(self._loop)
        if self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        # exception handler
        self._loop.set_exception_handler(
            default_exception_handler
        )
        self._params = params.copy()
        if dsn:
            self._dsn = dsn
        else:
            self._dsn = self.create_dsn(self._params)
        try:
            self._DEBUG = bool(params["DEBUG"])
        except KeyError:
            try:
                self._DEBUG = kwargs["debug"]
            except KeyError:
                self._DEBUG = False
        try:
            self._timeout = kwargs["timeout"]
        except KeyError:
            self._timeout = 600
        # set the logger:
        self._logger = logging.getLogger(__name__)

    def create_dsn(self, params):
        try:
            return self._dsn.format(**params)
        except Exception as err:
            self._logger.exception(err)
            return None

    def get_dsn(self):
        return self._dsn

    @property
    def logger(self):
        return self._logger

    """
    Context magic Methods
    """

    async def __aenter__(self) -> "BasePool":
        if not self._pool:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.close(timeout=5)

    """
    Properties
    """

    def pool(self):
        return self._pool

    def get_loop(self):
        return self._loop

    def is_connected(self):
        return self._connected

    def get_connection(self):
        return self._connection

    def engine(self):
        return self._connection

    def is_closed(self):
        self._logger.debug("Connection closed: %s" % self._pool.closed)
        return self._pool.closed

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


"""
Base
    Abstract Class for Provider
----
  TODO
    * making BaseProvider more generic and create a BaseDBProvider
    * create a BaseHTTPProvider for RESTful services (rest api, redash, etc)
"""
"""
BaseProvider
    Abstract Class for DB Connection
----
  params:
      result: asyncdb resultset
  TODO: change to BaseDBProvider
"""


class BaseProvider(ABC):
    _provider: str = "base"
    _syntax: str = "sql"  # can use QueryParser for parsing SQL queries
    _test_query: str = ''
    init_func: Optional[Callable] = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        self._params = {}
        self._pool = None
        self._dict = []
        self._connection = None
        self._connected = False
        self._util = None
        self._refresh = False
        self._result = []
        self._columns = []
        self._parameters = ()
        self._cursor = None
        self._sta = None
        self._attributes = None
        self._generated = None
        self._max_connections = 4
        self._initialized_on = None
        self._result = None
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        if self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.set_exception_handler(default_exception_handler)
        # get params
        if params:
            self._params = params
        if not dsn:
            self._dsn = self.create_dsn(self._params)
        else:
            self._dsn = dsn
        # if not self._dsn and not params:
        #     raise RuntimeError('Absent Credentials.')
        try:
            self._DEBUG = bool(params["DEBUG"])
        except KeyError:
            try:
                self._DEBUG = kwargs["debug"]
            except KeyError:
                self._DEBUG = False
        try:
            self._timeout = kwargs["timeout"]
        except KeyError:
            self._timeout = 600
        try:
            self._logger = logging.getLogger(__name__)
        except Exception as err:
            self._logger.exception(err)
            raise

    def create_dsn(self, params):
        try:
            return self._dsn.format(**params)
        except Exception as err:
            self._logger.exception(err)
            return None

    def get_dsn(self):
        return self._dsn

    def generated_at(self):
        return self._generated

    @property
    def logger(self):
        return self._logger

    """
    Async Context magic Methods
    """

    async def __aenter__(self):
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    @classmethod
    def type(self):
        # return self.__name__.lower()
        return self._provider

    def get_connection(self):
        return self._connection

    def engine(self):
        return self._connection

    def is_connected(self):
        return self._connected

    def is_closed(self):
        return not self._connected

    @classmethod
    def driver(self):
        return self.__name__

    @classmethod
    def dialect(self):
        return self._syntax

    """
    Properties
    """

    @property
    def columns(self):
        return self._columns

    def prepared_attributes(self):
        return self._attributes

    @property
    def connected(self):
        return self._connected

    def get_result(self):
        return self._result

    def set_connection(self, conn):
        if conn:
            self._connection = conn
            self._connected = True
        else:
            self._connection = None
            self._connected = False

    def get_loop(self):
        return self._loop

    def get_event_loop(self):
        return self._loop

    """
    Get Columns
    """

    def get_columns(self):
        return self._columns

    """
    Test Connnection
    """

    async def test_connection(self):
        if self._test_query is None:
            raise NotImplementedError()
        try:
            print(self._test_query)
            return await self.query(self._test_query)
        except Exception as err:
            raise ProviderError(message=str(err), code=0)

    """
    Terminate a connection
    """

    def terminate(self):
        try:
            self._loop.run_until_complete(self.close())
        except Exception as err:
            print("Connection Error: {}".format(str(err)))
            return False
        finally:
            self._connection = None
            self._connected = False
            return True

    """
    Get a connection from the pool
    """

    @abstractmethod
    async def connection(self):
        pass

    async def prepare(self, sentence: Any = None):
        """
        Prepare an statement
        """
        pass

    @abstractmethod
    async def execute(self, sentence: Any = None):
        """
        Execute a sentence
        """
        pass

    @abstractmethod
    async def column_info(self, tablename: str):
        """
        Getting Column info from an existing Table in Provider.
        """
        pass

    @abstractmethod
    async def query(self, sentence=""):
        """
        Making a Query and return result
        """
        pass

    @abstractmethod
    async def queryrow(self, sentence=""):
        pass

    """
    Close Connection
    """

    @abstractmethod
    async def close(self):
        pass

    def run_query(self, sql):
        if len(sql) == 0:
            raise EmptyStatement("Sentence is a empty string")
        return self._loop.run_until_complete(self.query(sql))

    def row(self, sql):
        if len(sql) == 0:
            raise EmptyStatement("Sentence is a empty string")
        return self._loop.run_until_complete(self.fetchrow(sql))

    """
     DDL Methods
    """

    def tables(self, schema=""):
        pass

    def table(self, table_name=""):
        pass
