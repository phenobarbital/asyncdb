#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime
import logging
from pymssql import _mssql

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
    BaseProvider,
    registerProvider,
)
from asyncdb.utils import (
    EnumEncoder,
    SafeDict,
)

from asyncdb.providers.sql import SQLProvider, baseCursor

types_map = {
    1: 'string',
    2: 'nvarchar',
    # Type #3 supposed to be an integer, but in some cases decimals are returned
    # with this type. To be on safe side, marking it as float.
    3: 'integer',
    4: 'datetime',
    5: 'float',
}


class mssqlCursor(baseCursor):
    _connection = None

    async def __aenter__(self) -> "mssqlCursor":
        if not self._connection:
            await self.connection()
        self._cursor = await self._connection.cursor(self._sentence, self._params)
        return self


class mssql(SQLProvider):
    """mssql.

    Microsoft SQL Server using low-level _mssql Protocol
    """

    _provider = "sqlserver"
    _dsn = ""
    _syntax = "sql"
    _test_query = "SELECT 1 as one"
    _parameters = ()
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    _timeout: int = 30
    _version: str = None
    application_name = "Navigator"
    _charset: str = "UTF8"
    _server_settings: dict = []

    def __init__(self, loop=None, pool=None, params={}, **kwargs):
        super(mssql, self).__init__(loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)
        try:
            if "host" in self._params:
                self._params["server"] = "{}:{}".format(
                    self._params["host"], self._params["port"]
                )
                del self._params["host"]
        except Exception as err:
            pass
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
            del self._server_settings["application_name"]

    def create_dsn(self, params):
        pass

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                self._logger.debug("SQL Server: Closing Connection")
                try:
                    self._connection.close()
                except Exception as err:
                    self._connection = None
                    raise ProviderError(
                        "Connection Error, Terminated: {}".format(str(err))
                    )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._params["appname"] = self.application_name
            self._params["charset"] = self._charset.upper()
            self._params["tds_version"] = "7.3"
            if self._server_settings:
                self._params["conn_properties"] = self._server_settings
            self._connection = _mssql.connect(**self._params)
            if self._connection.connected:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError("connection Error, Terminated: {}".format(str(err)))
        finally:
            return self

    def use(self, dbname: str = ""):
        self._connection.select_db(dbname)

    """
    Async Context magic Methods
    """

    async def __aenter__(self):
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self.close()
        except Exception as err:
            print(err)

    @property
    async def identity(self):
        return self._connection.identity

    async def test_connection(self):
        """
        Test Connnection.
        """
        if self._test_query is None:
            raise NotImplementedError()
        try:
            print(self._test_query)
            return await self.fetchone(self._test_query)
        except Exception as err:
            raise ProviderError(message=str(err), code=0)

    async def execute(self, sentence="", params: list = []):
        """
        Execute a sentence
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            self._result = self._connection.execute_non_query(sentence, params)
        except (_mssql.MSSQLDatabaseException) as err:
            error = "Database Error: {}".format(str(err))
            raise ProviderError(error)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    async def executemany(self, sentence="", params: list = []):
        """
        Execute multiple sentences
        """
        return await self.execute(sentence, params)

    async def query(self, sentence="", params: list = []):
        """
        Making a Query and return result
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            self._connection.execute_query(sentence, params)
            self._result = self._connection
        except (_mssql.MSSQLDatabaseException) as err:
            print(err)
            error = "Database Error: {}".format(str(err))
            raise ProviderError(error)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            print(error)
            return [self._result, error]

    async def queryrow(self, sentence="", params: list = []):
        return await self.fetchone(sentence, params)

    async def fetchone(self, sentence="", params: list = []):
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._result = self._connection.execute_row(sentence, params)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
                return [None, "SQL Server: No Data was Found"]
        except _mssql.MSSQLDatabaseException as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            return [self._result, error]

    async def fetchall(self, sentence="", params: list = []):
        return await self.query(sentence, params)

    async def fetch_scalar(self, sentence="", params: list = []):
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._result = self._connection.execute_scalar(sentence, params)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
                return [None, "SQL Server: No Data was Found"]
        except _mssql.MSSQLDatabaseException as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            return [self._result, error]


"""
Registering this Provider
"""
registerProvider(mssql)
