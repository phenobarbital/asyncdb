#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime

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

class mssql(BaseProvider):
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
    application_name = 'Navigator'
    _charset: str = 'UTF8'
    _server_settings: dict = []

    def __init__(self, loop=None, pool=None, params={}, **kwargs):
        super(mssql, self).__init__(loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)
        try:
            if 'host' in self._params:
                self._params['server'] = "{}:{}".format(
                    self._params['host'], self._params['port']
                )
                del self._params['host']
        except Exception as err:
            pass
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if 'application_name' in self._server_settings:
            self.application_name = self._server_settings['application_name']
            del self._server_settings['application_name']

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
                            "Connection Error, Terminated: {}".format(
                                str(err)
                            )
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
            self._params['appname'] = self.application_name
            self._params['charset'] = self._charset
            self._params['tds_version'] = '7.3'
            if self._server_settings:
                self._params['conn_properties'] = self._server_settings
            self._connection = _mssql.connect(
                **self._params
            )
            if self._connection.connected:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError(
                "connection Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    def use(self, dbname: str = ''):
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

    async def execute(self, sentence=""):
        """
        Execute a sentence
        """
        pass

    async def executemany(self, sentence=""):
        """
        Execute a sentence
        """
        pass

    async def query(self, sentence=""):
        """
        Making a Query and return result
        """
        pass

    async def queryrow(self, sentence=""):
        pass

    async def fetchone(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._result = self._connection.execute_row(sentence)
            print(self._result)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
                return [None, "SQL Server: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            return [self._result, error]

    async def fetchall(self, sentence=""):
        pass


"""
Registering this Provider
"""
registerProvider(mssql)
