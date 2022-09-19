"""
Basic Interfaces for every kind of Database Connector.
"""
import asyncio
import logging
import importlib
from collections.abc import Sequence, Iterable
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Optional,
    Union
)
from .meta import Record, Recordset
from .exceptions import (
    default_exception_handler,
    ProviderError,
    EmptyStatement
)


class PoolBackend(ABC):
    """
    Basic Interface for Pool-based Connectors.
    """
    _provider: str = "base"
    _syntax: str = ''  # Used by QueryParser for parsing queries

    def __init__(
            self,
            dsn: str = None,
            loop: asyncio.AbstractEventLoop = None,
            params: dict[Any] = None,
            **kwargs
    ) -> None:
        self._pool = None
        try:
            self._encoding = kwargs["encoding"]
        except KeyError:
            self._encoding = "utf-8"
        if "max_queries" in kwargs:
            self._max_queries = kwargs["max_queries"]
        else:
            self._max_queries = 300
        self._connection = None
        self._connected = False
        self._dsn = dsn
        if loop:
            self._loop = loop
            asyncio.set_event_loop(self._loop)
        else:
            self._loop = asyncio.get_event_loop()
            asyncio.set_event_loop(self._loop)
        if self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
            asyncio.set_event_loop(self._loop)
        # exception handler
        self._loop.set_exception_handler(
            default_exception_handler
        )
        try:
            self._debug = bool(params["DEBUG"])
        except (TypeError, KeyError):
            try:
                self._debug = kwargs["debug"]
            except KeyError:
                self._debug = False
        try:
            self._timeout = kwargs["timeout"]
        except KeyError:
            self._timeout = 600
        # set the logger:
        try:
            self._logger = logging.getLogger(name=__name__)
        except Exception as err:
            self._logger = None
            logging.exception(err)
            raise

    @abstractmethod
    async def connect(self) -> "PoolBackend":
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def disconnect(self, timeout: int = 5) -> None:
        raise NotImplementedError()  # pragma: no cover

    close = disconnect

    @abstractmethod
    async def acquire(self) -> "ConnectionBackend":
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def release(
        self,
        connection: "ConnectionBackend" = None,
        timeout: int = 10
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

### Magic Methods
    async def __aenter__(self) -> "PoolBackend":
        if not self._pool:
            await self.connect()
        await self.acquire()
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
        if not self._connected:
            logging.debug(f"Connection closed on: {self._pool}")
            return True
        return False

    @classmethod
    def driver(cls):
        return cls.__name__

    @classmethod
    def dialect(cls):
        return cls._syntax


class ConnectionBackend(ABC):
    """
    Basic Interface with basic methods for connect and disconnect.
    """
    _provider: str = "base"
    _syntax: str = ''  # Used by QueryParser for parsing queries

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop = None,
            params: dict[Any] = Any,
            **kwargs
    ) -> None:
        self._connection = None
        self._connected = False
        self._cursor = None
        self._generated = None
        self._pool = None
        try:
            self._encoding = kwargs["encoding"]
        except KeyError:
            self._encoding = "utf-8"
        if "max_queries" in kwargs:
            self._max_queries = kwargs["max_queries"]
        else:
            self._max_queries = 300
        try:
            self.params = params.copy()
        except (TypeError, AttributeError, ValueError):
            self.params = {}
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        if self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
        asyncio.set_event_loop(self._loop)
        # exception handler
        self._loop.set_exception_handler(
            default_exception_handler
        )
        try:
            self._debug = bool(params["DEBUG"])
        except KeyError:
            try:
                self._debug = kwargs["debug"]
            except KeyError:
                self._debug = False
        try:
            self._timeout = kwargs["timeout"]
        except KeyError:
            self._timeout = 600
        try:
            self._logger = logging.getLogger(name=__name__)
        except Exception as err:
            self._logger = None
            logging.exception(err)
            raise

    @abstractmethod
    async def connection(self) -> Any:
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def close(self, timeout: int = 10):
        raise NotImplementedError()  # pragma: no cover

    def is_closed(self):
        if not self._connected:
            logging.debug(f"Connection closed on: {self._pool}")
            return True
        return False

    # Properties
    @classmethod
    def type(cls):
        return cls._provider

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
        return self._connection

    engine = get_connection

    @property
    def raw_connection(self) -> Any:
        return self._connection

    def generated_at(self):
        return self._generated

    @classmethod
    def driver(cls):
        return cls.__name__

    @classmethod
    def dialect(cls):
        return cls._syntax

### Async Context magic Methods
    async def __aenter__(self) -> "ConnectionBackend":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        try:
            await self.close()
        except Exception as err:
            self._logger.exception(f'Closing Error: {err}')
            raise


class ConnectionDSNBackend(ABC):
    """
    Interface for Databases with DSN Support.
    """
    _logger: Any = None

    def __init__(
            self,
            dsn: str = None,
            params: dict[Any] = None
    ) -> None:
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
            if params:
                return self._dsn.format(**params)
            else:
                return None
        except TypeError as err:
            self._logger.exception(err)
            raise ProviderError(
                f"Error creating DSN connection: {err}"
            ) from err

    def get_dsn(self):
        return self._dsn


class TransactionBackend(ABC):
    """
    Interface for Drivers with Transaction Support.
    """

    def __init__(self):
        self._connection = None
        self._transaction = None

    @abstractmethod
    async def transaction(self, options: dict[Any]) -> Any:
        """
        Getting a Transaction Object.
        """
        raise NotImplementedError()  # pragma: no cover

    async def transaction_start(
        self, options: dict
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
            self
    ) -> None:
        self._columns: list = []
        self._attributes = None
        self._result: list = []
        self._prepared: Any = None

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
                message=str(err)
            ) from err

    @abstractmethod
    async def use(self, database: str) -> None:
        """
        Change the current Database.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        """
        Execute a sentence
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def execute_many(
            self,
            sentence: list,
            *args
    ) -> Optional[Any]:
        """
        Execute many sentences at once.
        """

    @abstractmethod
    async def query(self, sentence: Union[str, list], **kwargs) -> Optional[Recordset]:
        """queryrow.

        Making a Query and returns a resultset.
        Args:
            sentence (Union[str, list]): sentence(s) to be executed.
            kwargs: Optional attributes to query.

        Returns:
            Optional[Record]: Returns a Resultset
        """

    @abstractmethod
    async def queryrow(self, sentence: Union[str, list]) -> Optional[Record]:
        """queryrow.

        Returns a single row of a query sentence.
        Args:
            sentence (Union[str, list]): sentence to be executed.

        Returns:
            Optional[Record]: Return one single row of a query.
        """

    @abstractmethod
    async def prepare(self, sentence: Union[str, list]) -> Any:
        """
        Making Prepared statement.
        """

    def prepared_statement(self):
        return self._prepared

    def prepared_attributes(self):
        return self._attributes

    @abstractmethod
    async def fetch_all(self, sentence: str, **kwargs) -> list[Sequence]:
        pass

    @abstractmethod
    async def fetch_one(
            self,
            sentence: str,
            number: int = None
    ) -> Optional[dict]:
        """
        Fetch only one record, optional getting an offset using "number".
        """

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
        result: Optional[Any] = None,
        parameters: Iterable[Any] = None,
        **kwargs
    ):
        # self._cursor = None
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.engine()
        self._kwargs = kwargs

### Magic Context Methods for Cursors.
    async def __aenter__(self) -> "CursorBackend":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(
            self._sentence,
            self._params
        )
        return self

    def __enter__(self) -> "CursorBackend":
        self._cursor = self._connection.cursor()
        self._cursor.execute(
            self._sentence,
            self._params
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return await self._provider.close()
        except ProviderError as err:
            logging.exception(err)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return self._provider.close()
        except ProviderError as err:
            logging.exception(err)
            raise

    def __aiter__(self) -> "CursorBackend":
        """The cursor is also an async iterator."""
        return self

    def __iter__(self) -> "CursorBackend":
        """The cursor iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchrow()` to provide an async iterable.

            raise: StopAsyncIteration when done.
        """
        row = await self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    def __next__(self):
        """Use `cursor.fetchrow()` to provide an iterable.

            raise: StopAsyncIteration when done.
        """
        row = self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

### Cursor Methods.
    async def fetch_one(self) -> Optional[Sequence]:
        return await self._cursor.fetchone()

    async def fetch_many(self, size: int = None) -> Iterable[Sequence]:
        return await self._cursor.fetch(size)

    async def fetch_all(self) -> Iterable[Sequence]:
        return await self._cursor.fetchall()


class DBCursorBackend(ABC):
    """
    Interface for Backends with Cursor Support.
    """
    _provider: str = "base"

    def __init__(
        self
    ) -> None:
        self._columns: list = []
        self._attributes = None
        self._result: Iterable[Any] = None
        self._prepared: Any = None
        self._cursor: Optional[Any] = None
        try:
            # dynamic loading of Cursor Class
            cls = f"asyncdb.providers.{self._provider}"
            cursor = f"{self._provider}Cursor"
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except (ImportError) as err:
            logging.exception(f"Error Loading Cursor Class: {err}")
            self.__cursor__ = None

    def cursor(
        self,
        sentence: Union[str, any],
        params: Iterable[Any] = None,
        **kwargs
    ) -> Optional["DBCursorBackend"]:
        """ Returns an iterable Cursor Object """
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: Cannot use an empty Sentence."
            )
        if params is None:
            params = []
        try:
            return self.__cursor__(
                provider=self,
                sentence=sentence,
                parameters=params,
                **kwargs
            )
        except (TypeError, AttributeError, ValueError):
            raise
        except Exception as err:
            logging.exception(err)
            raise

### Cursor Iterator Context
    def __aiter__(self):
        return self

    async def __anext__(self) -> Optional[Record]:
        """_summary_

        Raises:
            StopAsyncIteration: raised when end is reached.

        Returns:
            _type_: Single record for iteration.
        """
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration
