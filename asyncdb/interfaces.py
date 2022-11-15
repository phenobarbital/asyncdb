"""
Basic Interfaces for every kind of Database Connector.
"""
import asyncio
import logging
import inspect
import types
from importlib import import_module
from collections.abc import Sequence, Iterable, Callable
from datetime import datetime
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    List,
    Optional,
    Union
)
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datamodel.exceptions import ValidationError
from .meta import Record, Recordset
from .exceptions import (
    default_exception_handler,
    ProviderError,
    EmptyStatement
)
from .models import Model, Field, is_missing, is_dataclass
from .utils.types import Entity, SafeDict

class PoolBackend(ABC):
    """
    Basic Interface for Pool-based Connectors.
    """
    _provider: str = "base"
    _syntax: str = ''  # Used by QueryParser for parsing queries
    _init_func: Optional[Callable] = None

    def __init__(
            self,
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
    _init_func: Optional[Callable] = None # a function called when connection is made

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop = None,
            params: dict[Any] = Any,
            **kwargs
    ) -> None:
        self._connection: Callable = None
        self._connected: bool = False
        self._cursor = None
        self._generated: datetime = None
        self._starttime: datetime = None
        self._pool = None
        self._executor = None
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
        # if self._loop.is_closed():
        #     self._loop = asyncio.get_running_loop()
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

    def set_connection(self, connection):
        self._connection = connection

    @abstractmethod
    async def close(self, timeout: int = 10) -> None:
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

    def start_timing(self):
        self._starttime = datetime.now()
        return self._starttime

    def generated_at(self, started: datetime = None):
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

### Async Context magic Methods
    async def __aenter__(self) -> "ConnectionBackend":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        try:
            await asyncio.wait_for(self.close(), timeout=20)
        except Exception as err:
            self._logger.exception(f'Closing Error: {err}')
            raise

    def get_executor(self, executor = 'thread', max_workers: int = 2) -> Any:
        if executor == 'thread':
            return ThreadPoolExecutor(max_workers=max_workers)
        elif executor == 'process':
            return ProcessPoolExecutor(max_workers=max_workers)
        else:
            return None

    async def _thread_func(self, fn, *args, executor: Any = None, **kwargs):
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
    _logger: Any = None

    def __init__(
            self,
            dsn: str = None,
            params: Optional[dict] = None
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
                return self._dsn.format_map(SafeDict(**params))
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
            **kwargs
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
        row = await self.fetch_many(sentence, number)
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
            cls = f"asyncdb.drivers.{self._provider}"
            cursor = f"{self._provider}Cursor"
            module = import_module(cls, package="drivers")
            self.__cursor__ = getattr(module, cursor)
        except ModuleNotFoundError as e:
            logging.exception(f"Error Loading Cursor Class: {e}")
            self.__cursor__ = None
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
        except (TypeError, AttributeError, ValueError) as e:
            raise TypeError(
                f"{__name__}: No support for Cursors."
            ) from e
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

class ModelBackend(ABC):
    """
    Interface for Backends with Dataclass-based Models Support.
    """

# ## Class-based Methods.
    async def _create_(self, _model: Model, rows: list):
        """
        Create all records based on a dataset and return result.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        results = []
        for row in rows:
            try:
                record = _model(**row)
            except (ValueError, ValidationError) as e:
                raise ValueError(
                    f"Invalid Row for Model {_model}: {e}"
                ) from e
            if record:
                try:
                    result = await record.insert()
                    results.append(result)
                except Exception as e:
                    raise ProviderError(
                        f"Error on Creation {table}: {e}"
                    ) from e
        return results

    @abstractmethod
    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """

    @abstractmethod
    async def _updating_(self, _model: Model, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
        """

    @abstractmethod
    async def _fetch_(self, _model: Model, *args, **kwargs):
        """
        Returns one row from Model.
        """

    @abstractmethod
    async def _filter_(self, _model: Model, *args, **kwargs):
        """
        Filter a Model using Fields.
        """

    @abstractmethod
    async def _select_(self, _model: Model, *args, **kwargs):
        """
        Get a query from Model.
        """

    @abstractmethod
    async def _all_(self, _model: Model, *args):
        """
        Get queries with model.
        """

    @abstractmethod
    async def _get_(self, _model: Model, *args, **kwargs):
        """
        Get one row from model.
        """

    @abstractmethod
    async def _delete_(self, _model: Model, **kwargs):
        """
        delete a row from model.
        """

    @abstractmethod
    async def _update_(self, _model: Model, **kwargs):
        """
        Updating a row in a Model.
        """

    @abstractmethod
    async def _save_(self, _model: Model, **kwargs):
        """
        Save a row in a Model, using Insert-or-Update methodology.
        """

    @abstractmethod
    async def _insert_(self, _model: Model, **kwargs):
        """
        insert a row from model.
        """

## Aux Methods:
    def _get_value(self, field: Field, value: Any) -> Any:
        datatype = field.type
        new_val = None
        if is_dataclass(datatype) and value is not None:
            if is_missing(value):
                new_val = None
            else:
                new_val = value
        if inspect.isclass(datatype) and value is None:
            if isinstance(datatype, (types.BuiltinFunctionType, types.FunctionType)):
                try:
                    new_val = datatype()
                except (TypeError, ValueError, AttributeError):
                    self._logger.error(f'Error Calling {datatype} in Field {field}')
                    new_val = None
        elif callable(datatype) and value is None:
            new_val = None
        else:
            new_val = value
        return new_val

    def _get_attribute(self, field: Field, value: Any, attr: str = 'primary_key') -> Any:
        if hasattr(field, attr):
            datatype = field.type
            if field.primary_key is True:
                value = Entity.toSQL(value, datatype)
                return value
        return None

    def _where(self, fields: dict[Field], **where):
        """
        TODO: add conditions for BETWEEN, NOT NULL, NULL, etc
           Re-think functionality for parsing where conditions.
        """
        result = ""
        if not fields:
            fields = {}
        if not where:
            return result
        elif isinstance(where, dict):
            _cond = []
            for key, value in where.items():
                f = fields[key]
                datatype = f.type
                if value is None or value == "null" or value == "NULL":
                    _cond.append(f"{key} is NULL")
                elif value == "!null" or value == "!NULL":
                    _cond.append(f"{key} is NOT NULL")
                elif isinstance(value, bool):
                    val = str(value)
                    _cond.append(f"{key} is {value}")
                elif isinstance(value, list):
                    _cond.append(
                        f"{key} = ANY(ARRAY[{value!r}])"
                    )
                elif isinstance(datatype, (list, List)):
                    val = ", ".join(
                        map(str, [Entity.escapeLiteral(v, type(v)) for v in value])
                    )
                    _cond.append(
                        f"ARRAY[{val}]<@ {key}::character varying[]")
                elif Entity.is_array(datatype):
                    val = ", ".join(
                        map(str, [Entity.escapeLiteral(v, type(v)) for v in value])
                    )
                    _cond.append(f"{key} IN ({val})")
                else:
                    # is an scalar value
                    val = Entity.escapeLiteral(value, datatype)
                    _cond.append(f"{key}={val}")
            _and = " AND ".join(_cond)
            result = f"\nWHERE {_and}"
            return result
        else:
            return result
