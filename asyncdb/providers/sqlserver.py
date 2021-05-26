
#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime

from .mssql import mssql
import pymssql

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

from asyncdb.providers.sql import (
    SQLProvider,
    baseCursor
)


class sqlserverCursor(baseCursor):
    _connection = None

    async def __aenter__(self) -> "sqlserverCursor":
        if not self._connection:
            await self.connection()
        self._cursor = await self._connection.cursor(
            self._sentence, self._params
        )
        return self


class sqlserver(mssql):
    """sqlserver.

    Microsoft SQL Server using DB-API connection
    """
    _provider = "sqlserver"

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._params['appname'] = self.application_name
            self._params['as_dict'] = True
            self._params['timeout'] = self._timeout
            self._params['charset'] = self._charset.upper()
            self._params['tds_version'] = '7.3'
            self._connection = pymssql.connect(
                **self._params
            )
            if self._connection:
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
        try:
            self._cursor = self._connection.cursor()
            self._cursor.execute(f'USE {dbname!s}')
        except pymssql.Warning as warn:
            logging.warning(f'SQL Server Warning: {warn!s}')
            error = warn
        except(pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(error)
        return self

    async def execute(self, sentence="", params: dict = {}):
        """
        Execute a sentence
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        # getting a cursor
        try:
            self._cursor = self._connection.cursor()
            self._result = self._cursor.execute(sentence, *params)
            # self._connection.commit()
        except pymssql.Warning as warn:
            logging.warning(f'SQL Server Warning: {warn!s}')
            error = warn
        except(pymssql.StandardError, pymssql.Error) as err:
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
        """
        Execute a sentence
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        # getting a cursor
        try:
            self._cursor = self._connection.cursor()
            self._result = self._cursor.executemany(sentence, params)
            # self._connection.commit()
        except pymssql.Warning as warn:
            logging.warning(f'SQL Server Warning: {warn!s}')
            error = warn
        except(pymssql.StandardError, pymssql.Error) as err:
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

    async def query(self, sentence="", params: list = None):
        """
        Making a Query and return result
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._cursor = self._connection.cursor()
            self._cursor.execute(sentence, params)
            self._result = self._cursor.fetchall()
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
                return [None, "SQL Server: No Data was Found"]
        except(pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            return [self._result, error]

    async def queryrow(self, sentence=""):
        cursor.execute('SELECT * FROM persons WHERE salesrep=%s', 'John Doe')
        row = cursor.fetchone()

    async def fetchone(self, sentence="", params: list = []):
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._cursor = self._connection.cursor()
            self._cursor.execute(sentence, *params)
            self._result = self._cursor.fetchone()
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
registerProvider(sqlserver)
