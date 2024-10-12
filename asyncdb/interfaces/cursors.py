from typing import Any, Iterable, Optional, Sequence, Union
from abc import ABC
import logging
from importlib import import_module
from ..meta.record import Record
from ..exceptions import DriverError, EmptyStatement


class CursorBackend(ABC):
    """
    Interface for Database Cursors.
    """

    def __init__(
        self, provider: Any, sentence: str, result: Optional[Any] = None, parameters: Iterable[Any] = None, **kwargs
    ):
        self._cursor = None
        self._provider = provider
        self._connection = provider.engine()
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"DB.{self.__class__.__name__}")

    ### Magic Context Methods for Cursors.
    async def __aenter__(self) -> "CursorBackend":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(self._sentence, self._params)
        return self

    def __enter__(self) -> "CursorBackend":
        self._cursor = self._connection.cursor()
        self._cursor.execute(self._sentence, self._params)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return await self._provider.close()
        except RuntimeError as e:
            self._logger.error(str(e))
        except DriverError as err:
            self._logger.exception(err)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return self._provider.close()
        except DriverError as err:
            self._logger.exception(err)
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
        if row := await self._cursor.fetchone() is not None:
            return row
        else:
            raise StopAsyncIteration

    def __next__(self):
        """Use `cursor.fetchrow()` to provide an iterable.

        raise: StopAsyncIteration when done.
        """
        if row := self._cursor.fetchone() is not None:
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

    def __init__(self) -> None:
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
        except ImportError as err:
            logging.exception(f"Error Loading Cursor Class: {err}")
            self.__cursor__ = None

    def cursor(
        self, sentence: Union[str, any], params: Union[Iterable[Any], None] = None, **kwargs
    ) -> Optional["DBCursorBackend"]:
        """Returns an iterable Cursor Object"""
        if not sentence:
            raise EmptyStatement(f"{__name__!s} Error: Cannot use an empty Sentence.")
        if params is None:
            params = []
        try:
            return self.__cursor__(provider=self, sentence=sentence, parameters=params, **kwargs)
        except (TypeError, AttributeError, ValueError) as e:
            raise TypeError(f"{__name__}: No support for Cursors.") from e
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
        if data := await self._cursor.fetchrow() is not None:
            return data
        else:
            raise StopAsyncIteration
