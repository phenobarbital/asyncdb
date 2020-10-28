import importlib
from asyncdb.providers import BaseProvider

from typing import (
    Any,
    List,
    Dict,
    Generator,
    Iterable,
    Optional,
)

class baseCursor:
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """
    _cursor = None
    _connection = None
    _provider: BaseProvider = None
    _result: Any = None
    _sentence: str = ''

    def __init__(
        self,
        provider:BaseProvider,
        sentence:str,
        result: Optional[List] = None,
        parameters: Iterable[Any] = None
    ):
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.get_connection()

    async def __aenter__(self) -> "baseCursor":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(
            self._sentence, self._params
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self._provider.close()

    def __aiter__(self) -> "baseCursor":
        """The cursor is also an async iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchone()` to provide an async iterable."""
        row = await self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetchone(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetchmany(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetchmany(size)

    async def fetchall(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class SQLProvider(BaseProvider):
    """
    SQLProvider.

    Driver methods for SQL-based providers
    """
    _syntax = "sql"
    _test_query = "SELECT 1"
    _prepared = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    __cursor__ = None


    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        try:
            # dynamic loading of Cursor Class
            cls = f'asyncdb.providers.{self._provider}'
            cursor = f'{self._provider}Cursor'
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except ImportError as err:
            print('Error Loading Cursor Class: ', err)
            pass
        super(SQLProvider, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    """
    Context magic Methods
    """
    async def __aenter__(self) -> "sqlite":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self.close()

    async def close(self, timeout=5):
        """
        Closing Method for SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError("{}: Closing Error: {}".format(__name__, str(err)))
        finally:
            self._connection = None
            self._connected = False
            return True

    async def connect(self, **kwargs):
        """
        Get a proxy connection, alias of connection
        """
        self._connection = None
        self._connected = False
        return await self.connection(self, **kwargs)

    async def release(self):
        """
        Release a Connection
        """
        await self.close()


    async def valid_operation(self, sentence: str):
        self._result = None
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if not self._connection:
            await self.connection()


    def cursor(self, sentence: str, parameters: Iterable[Any] = None) -> Iterable:
        """ Returns a iterable Cursor Object """
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if parameters is None:
            parameters = []
        try:
            return self.__cursor__(self, sentence=sentence, parameters=parameters)
        except Exception as err:
            print(err)
            return False
