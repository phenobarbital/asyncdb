#!/usr/bin/env python3
import os
import asyncio
import time
import logging
from typing import (
    Union,
    Optional,
    Any
)
from collections.abc import Iterable
import pymssql
from pymssql import _mssql
from asyncdb.interfaces import DBCursorBackend
from asyncdb.exceptions import (
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    DriverError
)
from .sql import SQLDriver, SQLCursor

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


class mssql(SQLDriver, DBCursorBackend):
    """mssql.

    Microsoft SQL Server using low-level _mssql Protocol.
    """

    _provider = "mssql"
    _syntax = "sql"
    _test_query = "SELECT 1 as one"
    _charset: str = "UTF8"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._dsn = ''
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._version: str = None
        self.application_name = os.getenv('APP_NAME', "NAV")
        self._server_settings: dict = []
        self._connected: bool = False
        try:
            self.tds_version = kwargs['tds_version']
            del kwargs["tds_version"]
        except KeyError:
            self.tds_version = "7.3"
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        try:
            if "host" in self.params:
                self.params["server"] = "{}:{}".format(
                    self.params["host"], self.params["port"]
                )
                del self.params["host"]
        except (TypeError, KeyError) as err:
            raise DriverError(
                f"MS SQL: Invalid Settings: {err}"
            ) from err
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
            del self._server_settings["application_name"]

    async def close(self): # pylint: disable=W0221
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
                        message=f"Connection Error, Terminated: {err}"
                    ) from err
        except Exception as err:
            raise ProviderError(
                message=f"Close Error: {err}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    disconnect = close

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError(
            "Prepared Sentences are not supported yet."
        )

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self.params["appname"] = self.application_name
            self.params["charset"] = self._charset.upper()
            self.params["tds_version"] = self.tds_version
            if self._server_settings:
                self.params["conn_properties"] = self._server_settings
            self._connection = _mssql.connect(**self.params) # pylint: disable=I1101
            if self._connection.connected:
                self._connected = True
                self._initialized_on = time.time()
            return self
        except _mssql.MSSQLDatabaseException as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            raise DriverError(
                f"MSSQL Error {num}: {msg}, state={state}"
            ) from ex
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            raise ProviderError(
                message=f"connection Error: {ex}"
            ) from ex
        except Exception as err:
            self._connection = None
            self._cursor = None
            raise ProviderError(
                message=f"connection Error, Terminated: {err}"
            ) from err

    async def use(self, database: str): # pylint: disable=W0236
        self._connection.select_db(database)

    @property
    async def identity(self):
        return self._connection.identity

    async def test_connection(self): # pylint: disable=W0221
        """
        Test Connnection.
        """
        error = None
        result = None
        if self._test_query is None:
            raise NotImplementedError()
        try:
            result = await self.fetchone(self._test_query)
        except Exception as err: # pylint: disable=W0703
            error = str(err)
        finally:
            return [result, error] # pylint: disable=W0150

    async def execute(self, sentence: str, *args, **kwargs):
        """
        Execute a sentence
        """
        error = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            self._result = self._connection.execute_non_query(sentence, args)
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return [self._result, error] # pylint: disable=W0150

    async def execute_many(self, sentence, *args):
        """
        Execute multiple sentences
        """
        return await self.execute(sentence, *args)

    executemany = execute_many

    async def query(self, sentence, *args, **kwargs):
        """
        Making a Query and return result
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._connection.execute_query(sentence, args)
            self._result = self._connection
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error) # pylint: disable=W0150

    async def queryrow(self, sentence, *args):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_row(sentence, args)
            if not self._result:
                # raise NoDataFound("SQL Server: No Data was Found")
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error) # pylint: disable=W0150

    async def fetch_one(self, sentence, *args, **kwargs):
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_row(sentence, args)
            if not self._result:
                # raise NoDataFound("SQL Server: No Data was Found")
                raise NoDataFound("SQL Server: No Data was Found")
            else:
                return self._result
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
            raise StatementError(error) from ex
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
            raise DataError(error) from ex
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
            raise DataError(error) from err
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
            raise ProviderError(error) from err
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
            raise ProviderError(error) from err

    fetchone = fetch_one

    async def fetch_all(self, sentence, *args, **kwargs):
        """
        Making a Query and return result
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._connection.execute_query(sentence, args)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
            return self._result
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
            raise StatementError(error) from ex
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
            raise DataError(error) from ex
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
            raise DataError(error) from err
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
            raise ProviderError(error) from err
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
            raise ProviderError(error) from err

    fetchall = fetch_all

    async def fetch_scalar(self, sentence, *args):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._result = self._connection.execute_scalar(sentence, args)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
            return self._result
        except (_mssql.MSSQLDatabaseException) as ex: # pylint: disable=I1101
            num = ex.number
            state = ex.state
            msg = ex.message
            error = f"MSSQL Database Error {num}: {msg}, state={state}"
            raise StatementError(error) from ex
        except _mssql.MSSQLDriverException as ex: # pylint: disable=I1101
            error=f"connection Error: {ex}"
            raise DataError(error) from ex
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
            raise DataError(error) from err
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
            raise ProviderError(error) from err
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
            raise ProviderError(error) from err

    fetchval = fetch_scalar

### Model Logic:
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
            self._logger.exception(
                f"Wrong Table information {tablename!s}: {err}"
            )

### DDL Information.
    async def create(
        self,
        obj: str = 'table',
        name: str = '',
        fields: Optional[list] = None
    ) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if obj == 'table':
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
                ) from err
        else:
            raise RuntimeError(f'SQLite: invalid Object type {object!s}')

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError
