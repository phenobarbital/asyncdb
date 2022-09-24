#!/usr/bin/env python3
import asyncio
import time
from typing import (
    Any,
    Optional,
)
from collections.abc import Iterable
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
from asyncdb.interfaces import DBCursorBackend
from .sql import SQLDriver, SQLCursor


class odbcCursor(SQLCursor):
    async def __aenter__(self) -> "odbcCursor":
        "Redefining __aenter__ based on requirements of ODBC Cursors"
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(self._sentence, self._params)
        return self


class odbc(SQLDriver, DBCursorBackend):
    _provider = "odbc"
    _dsn = "Driver={driver};Database={database}"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        if "host" in params:
            self._dsn = "DRIVER={driver};Database={database};server={host};uid={user};pwd={password}"
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)

    async def prepare(self):
        raise NotImplementedError('Prepared Statements not supported yet.')

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await aioodbc.connect(dsn=self._dsn)
            if self._connection:
                if callable(self.init_func):
                    try:
                        await self.init_func(self._connection)
                    except Exception as err:
                        print("ODBC: Error on Init Connection: {}".format(err))
                self._connected = True
                self._initialized_on = time.time()
        except pyodbc.Error as err:
            print("ERR ", err)
            self._logger.exception(err)
            raise ProviderError("ODBC Internal Error: {}".format(str(err)))
        except Exception as err:
            print("ERR ", err)
            self._logger.exception(err)
            raise ProviderError("ODBC Unknown Error: {}".format(str(err)))
        finally:
            return self

    async def query(self, sentence: str = Any):
        """
        Getting a Query from Database
        """
        # TODO: getting aiosql structures or sql-like function structures or query functions
        error = None
        await self.valid_operation(sentence)
        try:
            # getting cursor:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchall()
            if not self._result:
                return [None, NoDataFound]
        except pyodbc.Error as err:
            error = "ODBC: Query Error: {}".format(err)
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return [self._result, error]

    async def fetchall(self, sentence: str):
        """
        aliases for query, without error support
        """
        await self.valid_operation(sentence)
        try:
            # getting cursor:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchall()
            if not self._result:
                raise NoDataFound
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return self._result

    async def fetchmany(self, sentence: str, size: int = None):
        """
        aliases for query, without error support
        """
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound
        except pyodbc.ProgrammingError as err:
            error = "ODBC Query Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return self._result

    async def queryrow(self, sentence: str = Any):
        """
        Getting a Query from Database
        """
        # TODO: getting aiosql structures or sql-like function structures or query functions
        error = None
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchone()
            if not self._result:
                return [None, NoDataFound]
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return [self._result, error]

    async def fetchone(self, sentence: str):
        """
        aliases for queryrow, without error support
        """
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.cursor()
            await self._cursor.execute(sentence)
            self._result = await self._cursor.fetchone()
            if not self._result:
                raise NoDataFound
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
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
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.cursor()
            result = await self._cursor.execute(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return [result, error]

    async def executemany(self, sentence: str, *args):
        error = None
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.cursor()
            result = await self._cursor.executemany(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            await self._cursor.close()
            return [result, error]

    async def fetch(self, sentence: str, parameters: Iterable[Any] = None) -> Iterable:
        """Helper to create a cursor and execute the given query, returns a Native Cursor"""
        if parameters is None:
            parameters = []
        await self.valid_operation(sentence)
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(sentence, parameters)
        return self._cursor
