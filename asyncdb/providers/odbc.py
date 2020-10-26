#!/usr/bin/env python3
import asyncio
import os
import time
from typing import (
    Any,
    List,
    Dict,
    Generator,
    Iterable,
    Optional,
)
import aioodbc
from aioodbc.cursor import Cursor
import pyodbc

from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)


class odbcCursor:
    _cursor = None
    _connection = None
    _provider: BaseProvider = None
    _result: Any = None
    _sentence: str = ''

    def __init__(
        self,
        provider = None,
        result: Optional[List] = None,
        sentence: str = '',
        parameters: Iterable[Any] = None
    ):
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.get_connection()

    async def __aenter__(self) -> "odbcCursor":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(
            self._sentence, self._params
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self._provider.close()

    def __aiter__(self) -> "odbcCursor":
        """The cursor is also an async iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchone()` to provide an async iterable."""
        row = await self._cursor.fetchone()
        #print(row)
        #print(type(row)) is pyodbc.Row
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


class odbc(BaseProvider):
    _provider = "odbc"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _dsn = "Driver={driver};Database={database}"
    _prepared = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        if 'host' in params:
            self._dsn = "DRIVER={driver};Database={database};server={host};uid={user};pwd={password}"
        super(odbc, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    """
    Context magic Methods
    """
    async def __aenter__(self) -> "sqlite":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self.close()

    async def prepare(self):
        pass

    async def close(self, timeout=5):
        """
        Closing Method for ODBC
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError("ODBC: Closing Error: {}".format(str(err)))
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

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await aioodbc.connect(
                dsn=self._dsn
            )
            if self._connection:
                if callable(self.init_func):
                    try:
                        await self.init_func(self._connection)
                    except Exception as err:
                        print("ODBC: Error on Init Connection: {}".format(err))
                self._connected = True
                self._initialized_on = time.time()
        except pyodbc.Error as err:
            print('ERR ', err)
            logging.exception(err)
            raise ProviderError("ODBC Internal Error: {}".format(str(err)))
        except Exception as err:
            print('ERR ', err)
            logging.exception(err)
            raise ProviderError("ODBC Unknown Error: {}".format(str(err)))
        finally:
            return self

    async def release(self):
        """
        Release a Connection
        """
        await self.close()

    async def query(self, sentence: str = Any):
        """
        Getting a Query from Database
        """
        #TODO: getting aiosql structures or sql-like function structures or query functions
        error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            # getting cursor:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchall()
            if not self._result:
                return [None, NoDataFound]
        except pyodbc.Error as err:
            error = "ODBC: Query Error: {}".format(err)
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return [self._result, error]

    async def fetchall(self, sentence: str):
        """
        aliases for query, without error support
        """
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            # getting cursor:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchall()
            if not self._result:
                raise NoDataFound
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return self._result

    async def fetchmany(self, sentence: str, size: int = None):
        """
        aliases for query, without error support
        """
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound
        except pyodbc.ProgrammingError as err:
            error = "ODBC Query Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return self._result

    async def queryrow(self, sentence: str = Any):
        """
        Getting a Query from Database
        """
        #TODO: getting aiosql structures or sql-like function structures or query functions
        error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchone()
            if not self._result:
                return [None, NoDataFound]
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return [self._result, error]

    async def fetchone(self, sentence: str):
        """
        aliases for queryrow, without error support
        """
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchone()
            if not self._result:
                raise NoDataFound
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return self._result

    async def execute(self, sentence: str = Any, *args):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = await self._connection.cursor()
            result = await self._cursor.execute(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return [result, error]

    async def executemany(self, sentence: str, *args):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = await self._connection.cursor()
            result = await self._cursor.executemany(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(error)
        finally:
            await self._cursor.close()
            return [result, error]

    async def fetch(
        self, sentence: str, parameters: Iterable[Any] = None
    ) -> Iterable:
        """Helper to create a cursor and execute the given query, returns a Native Cursor"""
        if parameters is None:
            parameters = []
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(sentence, parameters)
        return self._cursor

    def cursor(self, sentence: str, parameters: Iterable[Any] = None) -> Iterable:
        """ Returns a iterable Cursor Object """
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if parameters is None:
            parameters = []
        try:
            return odbcCursor(self, sentence=sentence, parameters=parameters)
        except Exception as err:
            print(err)
            return False



# Registering this Provider
registerProvider(odbc)
