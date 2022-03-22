#!/usr/bin/env python3
import os
import asyncio
import json
import time
from datetime import datetime
import logging
import pymssql
from pymssql import _mssql
from typing import (
    List,
    Dict,
    Optional,
    Iterable,
    Any
)
from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.utils import (
    EnumEncoder,
    SafeDict,
)
from .sql import SQLProvider, SQLCursor
from .interfaces import DBCursorBackend

types_map = {
    1: 'string',
    2: 'nvarchar',
    # Type #3 supposed to be an integer, but in some cases decimals are returned
    # with this type. To be on safe side, marking it as float.
    3: 'integer',
    4: 'datetime',
    5: 'float',
}


class mssqlCursor(SQLCursor):
    """ MS SQL Server Cursor. """
    pass


class mssql(SQLProvider, DBCursorBackend):
    """mssql.

    Microsoft SQL Server using low-level _mssql Protocol
    """

    _provider = "mssql"
    _syntax = "sql"
    _test_query = "SELECT 1 as one"
    _charset: str = "UTF8"

    def __init__(self, dsn: str = "", loop=None, params={}, **kwargs):
        self._dsn = ''
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._version: str = None
        self.application_name = os.getenv('APP_NAME', "NAV")
        self._server_settings: dict = []
        self._connected: bool = False
        super(mssql, self).__init__(
            loop=loop,
            params=params,
            **kwargs
        )
        try:
            if "host" in self.params:
                self.params["server"] = "{}:{}".format(
                    self.params["host"], self.params["port"]
                )
                del self.params["host"]
        except Exception as err:
            pass
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
            del self._server_settings["application_name"]

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
                        message="Connection Error, Terminated: {}".format(str(err))
                    )
        except Exception as err:
            raise ProviderError(
                message="Close Error: {}".format(str(err))
            )
        finally:
            self._connection = None
            self._connected = False

    disconnect = close

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self.params["appname"] = self.application_name
            self.params["charset"] = self._charset.upper()
            self.params["tds_version"] = "7.3"
            if self._server_settings:
                self.params["conn_properties"] = self._server_settings
            self._connection = _mssql.connect(**self.params)
            if self._connection.connected:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError(
                message="connection Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    def use(self, dbname: str = ""):
        self._connection.select_db(dbname)

    @property
    async def identity(self):
        return self._connection.identity

    async def test_connection(self):
        """
        Test Connnection.
        """
        error = None
        if self._test_query is None:
            raise NotImplementedError()
        try:
            result = await self.fetchone(self._test_query)
        except Exception as err:
            error = str(err)
        finally:
            return [result, error]

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
            raise ProviderError(message=error)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(message=error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    async def execute_many(self, sentence="", params: list = []):
        """
        Execute multiple sentences
        """
        return await self.execute(sentence, params)

    executemany = execute_many

    async def query(self, sentence="", params: list = []):
        """
        Making a Query and return result
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._connection.execute_query(sentence, params)
            self._result = self._connection
        except (_mssql.MSSQLDatabaseException) as err:
            print(err)
            error = "Database Error: {}".format(str(err))
            raise ProviderError(message=error)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(message=error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return await self._serializer(self._result, error)

    async def queryrow(self, sentence="", params: list = []):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_row(sentence, params)
            if not self._result:
                # raise NoDataFound("SQL Server: No Data was Found")
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except _mssql.MSSQLDatabaseException as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return await self._serializer(self._result, error)

    async def fetch_one(self, sentence="", params: list = []):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_row(sentence, params)
            if not self._result:
                # raise NoDataFound("SQL Server: No Data was Found")
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except _mssql.MSSQLDatabaseException as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return self._result

    fetchone = fetch_one

    async def fetch_all(self, sentence="", params: list = []):
        """
        Making a Query and return result
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._connection.execute_query(sentence, params)
            self._result = self._connection
        except (_mssql.MSSQLDatabaseException) as err:
            print(err)
            error = "Database Error: {}".format(str(err))
            raise ProviderError(message=error)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = "SQL Server Error: {}".format(str(err))
            raise ProviderError(message=error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return self._result

    fetchall = fetch_all

    async def fetch_scalar(self, sentence="", params: list = []):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_scalar(sentence, params)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
                return [None, "SQL Server: No Data was Found"]
        except _mssql.MSSQLDatabaseException as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    fetchval = fetch_scalar

    """
    Model Logic:
    """

    async def column_info(self, tablename: str, schema: str = None):
        """Column Info.

        Get Meta information about a table (column name, data type and PK).
        Useful to build a DataModel from Querying database.
        Parameters:
        @tablename: str The name of the table (including schema).
        """
        if schema:
            table = f"{schema}.{tablename}"
        else:
            table = tablename
        sql = f"SELECT a.attname AS name, a.atttypid::regtype AS type, \
        format_type(a.atttypid, a.atttypmod) as format_type, a.attnotnull::boolean as notnull, \
        coalesce((SELECT true FROM pg_index i WHERE i.indrelid = a.attrelid \
        AND i.indrelid = a.attrelid AND a.attnum = any(i.indkey) \
        AND i.indisprimary), false) as is_primary \
        FROM pg_attribute a WHERE a.attrelid = '{tablename!s}'::regclass \
        AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        if not self._connection:
            await self.connection()
        try:
            colinfo = await self._connection.fetch(sql)
            return colinfo
        except Exception as err:
            self._logger.exception(f"Wrong Table information {tablename!s}")

    """
    DDL Information.
    """
    async def create(
        self,
        object: str = 'table',
        name: str = '',
        fields: Optional[List] = None
    ) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if object == 'table':
            sql = "CREATE TABLE {name}({columns});"
            columns = ", ".join(["{name} {type}".format(**e) for e in fields])
            sql = sql.format(name=name, columns=columns)
            try:
                result = await self._connection.execute(sql)
                if result:
                    await self._connection.commit()
                    return True
                else:
                    return False
            except Exception as err:
                raise ProviderError(
                    message=f"Error in Object Creation: {err!s}"
                )
        else:
            raise RuntimeError(f'SQLite: invalid Object type {object!s}')

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def use(self, tablename: str):
        raise NotImplementedError(
            'AsyncPg Error: There is no Database in SQLite'
        )

    def prepare(self):
        pass
