#!/usr/bin/env python3
import asyncio
import os
import sqlite3
import time
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
)

import aiosqlite

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

from asyncdb.providers.sql import (
    SQLProvider,
    baseCursor
)

class sqliteCursor(baseCursor):
    _connection: aiosqlite.Connection = None

    async def __aenter__(self) -> "sqliteCursor":
        self._cursor = await self._connection.execute(
            self._sentence, self._params
        )
        return self

class sqlite(SQLProvider):
    _provider = "sqlite"
    _dsn = "{database}"

    async def prepare(self):
        "Ignoring prepared sentences on SQLite"
        pass

    async def close(self, timeout=5):
        """
        Closing Method for SQLite
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False
            return True

    def connect(self, **kwargs):
        """
        Get a proxy connection
        """
        self._connection = None
        self._connected = False
        try:
            print('Running Connect')
            self._connection = aiosqlite.connect(
                database=self._dsn, loop=self._loop, **kwargs
            )
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except aiosqlite.OperationalError:
            raise ProviderError(
                "Unable to Open Database File: {}".format(self._dsn)
            )
        except aiosqlite.DatabaseError as err:
            print("Connection Error: {}".format(str(err)))
            raise ProviderError(
                "Database Connection Error: {}".format(str(err))
            )
        except aiosqlite.Error as err:
            raise ProviderError("Internal Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("SQLite Unknown Error: {}".format(str(err)))
        finally:
            return self

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await aiosqlite.connect(
                database=self._dsn, loop=self._loop, **kwargs
            )
            if self._connection:
                if callable(self.init_func):
                    try:
                        await self.init_func(self._connection)
                    except Exception as err:
                        print("Error on Init Connection: {}".format(err))
                self._connected = True
                self._initialized_on = time.time()
        except aiosqlite.OperationalError:
            raise ProviderError(
                "Unable to Open Database File: {}".format(self._dsn)
            )
        except aiosqlite.DatabaseError as err:
            print("Connection Error: {}".format(str(err)))
            raise ProviderError(
                "Database Connection Error: {}".format(str(err))
            )
        except aiosqlite.Error as err:
            raise ProviderError("Internal Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("SQLite Unknown Error: {}".format(str(err)))
        finally:
            return self

    async def query(self, sentence: str = Any):
        """
        Getting a Query from Database
        """
        #TODO: getting aiosql structures or sql-like function structures or query functions
        error = None
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.execute(sentence)
            self._result = await self._cursor.fetchall()
            if not self._result:
                return [None, NoDataFound]
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
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.execute(sentence)
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
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.execute(sentence)
            self._result = await self._cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound
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
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.execute(sentence)
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
        aliases for query, without error support
        """
        await self.valid_operation(sentence)
        try:
            self._cursor = await self._connection.execute(sentence)
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
        await self.valid_operation(sentence)
        try:
            result = await self._connection.execute(sentence, *args)
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
        await self.valid_operation(sentence)
        try:
            result = await self._connection.executemany(sentence, *args)
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
        """Helper to create a cursor and execute the given query."""
        await self.valid_operation(sentence)
        if parameters is None:
            parameters = []
        result = await self._connection.execute(sentence, parameters)
        return result


# Registering this Provider
registerProvider(sqlite)
