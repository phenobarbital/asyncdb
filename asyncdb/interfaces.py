"""
Basic Interfaces for every kind of Database Connector.
"""
import asyncio
import logging
import importlib
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    List,
    Dict,
    Any,
    Optional,
    Iterable,
    Union
)
from asyncdb.exceptions import (
    default_exception_handler,
    ProviderError,
    EmptyStatement
)
from collections.abc import Sequence


class PoolBackend(ABC):
    """
    Basic Interface for Pool-based Connectors.
    """
    _provider: str = "base"
    _syntax: str = ''  # Used by QueryParser for parsing queries

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: Dict[Any, Any] = {},
            **kwargs
    ) -> None:
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
        try:
            self._params = params.copy()
        except TypeError:
            self._params = {}
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
        try:
            self._logger = logging.getLogger(name=__name__)
        except Exception as err:
            logging.exception(err)
            raise

    @abstractmethod
    async def connect(self) -> "PoolBackend":
        pass

    @abstractmethod
    async def disconnect(self, timeout: int = 5) -> None:
        pass

    close = disconnect

    @abstractmethod
    async def acquire(self) -> None:
        pass

    @abstractmethod
    async def release(
        self,
        connection: "ConnectionBackend" = None,
        timeout: int = 10
    ) -> None:
        pass

    """ Magic Methods """
    async def __aenter__(self) -> "PoolBackend":
        if not self._pool:
            await self.connect()
        self._connection = await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.release(
            connection=self._connection,
            timeout=5
        )

    @property
    def log(self):
        return self._logger

    def pool(self):
        return self._pool

    def get_loop(self):
        return self._loop

    event_loop = get_loop

    def is_connected(self):
        return self._connected

    def get_connection(self):
        return self._pool

    engine = get_connection

    def is_closed(self):
        logging.debug(f"Connection closed on: {self._pool}")
        return not self._connected

    @classmethod
    def driver(self):
        return self.__name__

    @classmethod
    def dialect(self):
        return self._syntax


class ConnectionBackend(ABC):
    """
    Basic Interface with basic methods for connect and disconnect.
    """
    _provider: str = "base"
    _syntax: str = ''  # Used by QueryParser for parsing queries

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop = None,
            params: Dict[Any, Any] = {},
            **kwargs
    ) -> None:
        self._connection = None
        self._connected = False
        self._cursor = None
        try:
            self.params = params.copy()
        except TypeError:
            pass
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        if self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        # exception handler
        self._loop.set_exception_handler(
            default_exception_handler
        )
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
            self._logger = logging.getLogger(name=__name__)
        except Exception as err:
            logging.exception(err)
            raise

    @abstractmethod
    async def connection(self):
        pass

    @abstractmethod
    async def close(self, timeout: int = 10):
        pass

    # Properties
    @classmethod
    def type(self):
        return self._provider

    @property
    def log(self):
        return self._logger

    def pool(self):
        return self._pool

    def get_loop(self):
        return self._loop

    event_loop = get_loop

    def is_connected(self):
        return self._connected

    def is_closed(self):
        logging.debug(f"Connection closed on: {self._connection}")
        return not self._connected

    def get_connection(self):
        return self._connection

    engine = get_connection

    def generated_at(self):
        return self._generated

    @classmethod
    def driver(self):
        return self.__name__

    @classmethod
    def dialect(self):
        return self._syntax

    """
    Async Context magic Methods
    """
    async def __aenter__(self):
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class ConnectionDSNBackend(ABC):
    """
    Interface for Databases with DSN Support.
    """

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: Dict[Any, Any] = {},
            **kwargs
    ) -> None:
        if dsn:
            self._dsn = dsn
        else:
            self._dsn = self.create_dsn(params)

    def create_dsn(self, params):
        try:
            return self._dsn.format(**params)
        except Exception as err:
            self._logger.exception(err)
            return None

    def get_dsn(self):
        return self._dsn


class TransactionBackend(ABC):
    """
    Interface for Drivers Support transactions.
    """

    def __init__(self):
        self._connection = None
        self._transaction = None

    @abstractmethod
    async def transaction(self, options: Dict[Any, Any]):
        """
        Getting a Transaction Object.
        """
        pass

    async def transaction_start(
        self, options: Dict[Any, Any]
    ) -> None:
        """
        Starts a Transaction.
        """
        self._transaction = self.transaction(options)

    @abstractmethod
    async def commit(self) -> None:
        pass

    @abstractmethod
    async def rollback(self) -> None:
        pass


class DatabaseBackend(ABC):
    """
    Interface for Basic Methods on Databases (query, fetch, execute).
    """
    _test_query: Optional[Any] = None

    def __init__(
            self,
            *args,
            **kwargs
    ) -> None:
        self._columns: List[Any] = []
        self._attributes = None
        self._result: List[Any] = []
        self._prepared: Any = None

    """
    Properties
    """
    @property
    def columns(self):
        return self._columns

    def get_columns(self):
        return self._columns

    @property
    def result(self):
        return self._result

    def get_result(self):
        return self._result

    async def test_connection(self, **kwargs):
        """Test Connnection.
        Making a connection Test using the basic Query Method.
        """
        if self._test_query is None:
            raise NotImplementedError()
        try:
            return await self.query(self._test_query, **kwargs)
        except Exception as err:
            raise ProviderError(
                message=str(err), code=0
            )

    @abstractmethod
    async def use(self, database: str) -> None:
        """
        Change the current Database.
        """
        pass

    @abstractmethod
    async def execute(self, sentence: Any = None, *args) -> Optional[Any]:
        """
        Execute a sentence
        """
        pass

    @abstractmethod
    async def execute_many(
            self,
            sentence: Union[str, List],
            *args
    ) -> Optional[Any]:
        """
        Execute many sentences at once.
        """
        pass

    executemany = execute_many

    @abstractmethod
    async def query(self, sentence=""):
        """
        Making a Query and return result
        """
        pass

    @abstractmethod
    async def queryrow(self, sentence=""):
        pass

    @abstractmethod
    async def prepare(self, sentence: Any = None):
        """
        Prepare an statement.
        """
        pass

    def prepared_statement(self):
        return self._prepared

    def prepared_attributes(self):
        return self._attributes

    @abstractmethod
    async def fetch_all(self, sentence: str, **kwargs) -> List[Sequence]:
        pass

    @abstractmethod
    async def fetch_one(
            self,
            sentence: str,
            number: int = None
    ) -> Optional[Dict]:
        """
        Fetch only one record, optional getting an offset using "number".
        """
        pass

    async def fetch_val(
        self, sentence: str, column: Any = None, number: int = None
    ) -> Any:
        """
        Fetch the value of a Column in a record.
        """
        row = await self.fetch_one(sentence, number)
        return None if row is None else row[column]


class CursorBackend(ABC):
    """
    Interface for Database Cursors.
    """

    def __init__(
        self,
        provider: Any,
        sentence: str,
        result: Optional[List] = None,
        parameters: Iterable[Any] = None,
    ):
        # self._cursor = None
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.engine()

    """
    Magic Context Methods for Cursors.
    """
    async def __aenter__(self) -> "CursorBackend":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(
            self._sentence,
            self._params
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return await self._provider.close()
        except Exception as err:
            logging.exception(err)

    def __aiter__(self) -> "CursorBackend":
        """The cursor is also an async iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchrow()` to provide an async iterable."""
        row = await self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    @abstractmethod
    async def execute(sentence: Any, params: Optional[Dict] = None) -> Any:
        pass

    """
    Cursor Methods.
    """

    async def fetch_one(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetch_many(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetch(size)

    async def fetch_all(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class DBCursorBackend(ABC):
    """
    Interface for Backends with Cursor Support.
    """
    _provider: str = "base"

    def __init__(
            self,
            *args,
            **kwargs
    ) -> None:
        self._columns: List[Any] = []
        self._attributes = None
        self._result: List[Any] = []
        self._prepared: Any = None
        self._cursor: Optional[Any] = None
        try:
            # dynamic loading of Cursor Class
            cls = f"asyncdb.providers.{self._provider}"
            cursor = f"{self._provider}Cursor"
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except ImportError as err:
            logging.exception(f"Error Loading Cursor Class: {err}")
            self.__cursor__ = None

    async def cursor(
                    self,
                    sentence: str,
                    params: Iterable[Any] = None,
                    **kwargs
    ) -> Optional["DBCursorBackend"]:
        """ Returns an iterable Cursor Object """
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: Cannot use an empty sentence"
            )
        if params is None:
            params = []
        try:
            return self.__cursor__(
                provider=self,
                sentence=sentence,
                parameters=params
            )
        except Exception as err:
            logging.exception(err)
            return None

    @abstractmethod
    async def fetch(
            self,
            sentence: str,
            number: int = None,
            **kwargs
    ) -> List[Sequence]:
        pass

    @abstractmethod
    async def fetch_one(
            self,
            sentence: str,
            **kwargs
    ) -> List[Sequence]:
        pass

    @abstractmethod
    async def fetch_all(self, sentence: str, **kwargs) -> List[Sequence]:
        pass

    @abstractmethod
    async def fetchrow(self, sentence: str, *args, **kwargs) -> List[Sequence]:
        pass

    """
    Cursor Iterator Context
    """

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration
