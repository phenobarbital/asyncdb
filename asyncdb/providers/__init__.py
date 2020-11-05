# -*- coding: utf-8 -*-
import asyncio
import importlib
import os.path
import sys
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Callable,
    Optional,
)

from asyncdb.exceptions import *
from asyncdb.exceptions import (
    _handle_done_tasks,
    default_exception_handler,
)
from asyncdb.utils.functions import module_exists

_PROVIDERS = {}

# logging system
import logging
loglevel = logging.INFO
logger_config = dict(
    version = 1,
    formatters = {
        "console": {
            'format': '%(message)s'
        },
        "file": {
            "format": "%(asctime)s: [%(levelname)s]: %(pathname)s: %(lineno)d: \n%(message)s\n"
        },
        'default': {
            'format': '[%(levelname)s] %(asctime)s %(name)s: %(message)s'}
        },
    handlers = {
        "console": {
                "formatter": "console",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                'level': loglevel
        },
        'StreamHandler': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'level': loglevel
        }
        },
    root = {
        'handlers': ['StreamHandler','console'],
        'level': loglevel,
        },
)

class BasePool(ABC):
    _dsn = ""
    _loop = None
    _pool = None
    _timeout = 600
    _max_queries = 300
    _connected = False
    _connection = None
    _params = None
    _DEBUG = False
    _logger = None
    init_func: Optional[Callable] = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
            asyncio.set_event_loop(self._loop)
        self._loop.set_exception_handler(default_exception_handler)
        self._params = params
        if not dsn:
            self._dsn = self.create_dsn(self._params)
        else:
            self._dsn = dsn
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
            pass
        self._logger = logging.getLogger(__name__)

    def create_dsn(self, params):
        return self._dsn.format(**params)

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

    def is_connected(self):
        return self._connected

    def get_connection(self):
        return self._connection

    def engine(self):
        return self._connection

    def is_closed(self):
        # logger.debug("Connection closed: %s" % self._pool._closed)
        return self._pool._closed

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
BaseDB
    Abstract Class for DB Connection
----
  params:
      result: asyncpg resultset
  TODO: change to BaseDBProvider
"""


class BaseProvider(ABC):
    _provider = "base"
    _syntax = "sql"  # can use QueryParser for parsing SQL queries
    _dsn = ""
    _connection = None
    _connected = False
    _util = None
    _refresh = False
    _result = []
    _columns = []
    _parameters = ()
    _cursor = None
    _dict = []
    _loop = None
    _pool = None
    _params = {}
    _sta = ""
    _test_query = None
    _timeout = 600
    _max_connections = 4
    _generated = None
    _DEBUG = False
    _logger = None
    init_func: Optional[Callable] = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        self._params = {}
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
            asyncio.set_event_loop(self._loop)
        self._loop.set_exception_handler(default_exception_handler)
        # get params
        if params:
            self._params = params
        if not dsn:
            self._dsn = self.create_dsn(self._params)
        else:
            self._dsn = dsn
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
            pass
        self._logger = logging.getLogger(__name__)

    def create_dsn(self, params):
        if params:
            return self._dsn.format(**params)

    def generated_at(self):
        return self._generated

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

    """
    Prepare an statement
    """

    @abstractmethod
    async def prepare(self):
        pass

    """
    Prepare an statement
    """

    @abstractmethod
    async def prepare(self):
        pass

    """
    Execute a sentence
    """

    @abstractmethod
    async def execute(self, sentence=""):
        pass

    """
    Making a Query and return result
    """

    @abstractmethod
    async def query(self, sentence=""):
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


def registerProvider(provider):
    global _PROVIDERS
    name = provider.driver()
    classpath = f'asyncdb.providers.{name}'
    try:
        cls = module_exists(name, classpath)
        _PROVIDERS[name] = cls
    except ImportError as err:
        raise ImportError(err)
