""" sqlalchemy.

non-async SQL Alchemy Provider.
Notes on sqlalchemy Provider
--------------------
This provider implements a basic set of funcionalities from SQLAlchemy core
"""

import asyncio
from typing import Any, Dict, List, Optional
from collections.abc import Callable, Iterable
from sqlalchemy.exc import DatabaseError, OperationalError, SQLAlchemyError, ProgrammingError, InvalidRequestError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from asyncdb.meta import Record
from asyncdb.exceptions import (
    EmptyStatement,
    NoDataFound,
    DriverError,
    StatementError,
    TooManyConnections,
)
from asyncdb.interfaces import DBCursorBackend
from asyncdb.utils.encoders import json_encoder, json_decoder
from .sql import SQLDriver, SQLCursor


class saCursor(SQLCursor):
    _connection: Any = None

    async def __aenter__(self) -> "SQLCursor":
        try:
            self._cursor = await self._connection.execute(self._sentence, self._params)
        except Exception as e:
            raise DriverError(f"SQLAlchemy Error: {e}") from e
        return self


class sa(SQLDriver, DBCursorBackend):
    _provider = "sa"
    _syntax = "sql"
    _test_query = "SELECT 1 as one"
    _engine_options: Dict = {
        "connect_args": {"timeout": 360},
        "execution_options": {"isolation_level": "AUTOCOMMIT"},
        "echo": False,
        "future": True,
        "json_deserializer": json_decoder,
        "json_serializer": json_encoder,
    }
    setup_func: Optional[Callable] = None
    init_func: Optional[Callable] = None

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs):
        """sql_alchemy.

        Args:
            dsn (str, optional): Connection DSN. Defaults to "".
            loop (asyncio.AbstractEventLoop, optional): optional Event Loop. Defaults to None.
            params (dict, optional): Connection Parameters. Defaults to None.
        """
        self._session = None
        self._dsn = "{driver}://{user}:{password}@{host}:{port}/{database}"
        self._transaction = None
        self._driver = "postgresql"
        self.__cursor__ = None
        self._row_format = "dict"
        if params:
            try:
                if not params["driver"]:
                    params["driver"] = "postgresql+asyncpg"
                else:
                    self._driver = params["driver"]
            except KeyError:
                params["driver"] = "postgresql+asyncpg"
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        self._options = self._engine_options
        if kwargs:
            self._options = {**self._options, **kwargs}

    def engine(self):
        return self._connection

    def __del__(self):
        del self._connection
        del self._session

    async def close(self):
        await self.release()

    async def release(self):
        if self._connection:
            try:
                await self._connection.dispose()
            except Exception as err:
                self._connection = None
                raise DriverError(f"Engine Error, Terminated: {err!s}")
            finally:
                self._connection = None
                self._connected = False

    async def connection(self):
        """
        Get a connection
        """
        self._logger.info(f"SQLAlchemy: Connecting to {self._dsn}")
        self._connection = None
        self._connected = False
        try:
            self._connection = create_async_engine(self._dsn, **self._options)
            self._session = AsyncSession(bind=self._connection)
            self._connected = True
        except (SQLAlchemyError, OperationalError) as err:
            print(err)
            self._connection = None
            raise DriverError(f"Connection Error: {err!s}")
        except Exception as err:
            print(err)
            self._connection = None
            raise DriverError(f"Engine Error, Terminated: {err!s}")
        finally:
            return self

    def prepare(self, sentence=""):
        """
        Preparing a sentence.
        """
        raise NotImplementedError()

    async def get_result(self, resultset):
        result = None
        if self._row_format == "native":
            result = resultset.fetchone()
        if self._row_format == "dict":
            result = dict(resultset.mappings().one())
        elif self._row_format == "iterable":
            result = resultset.mappings().one()
        elif self._row_format == "record":
            row = resultset.mappings().one()
            result = Record(row, row.keys())
        else:
            result = resultset.fetchone()
        return result

    async def get_resultset(self, resultset):
        result = None
        if self._row_format == "list":
            result = resultset.mappings().all()
        elif self._row_format == "iterable":
            result = resultset.mappings().all()
        elif self._row_format == "record":
            rows = resultset.mappings().all()
            result = [Record(row, row.keys()) for row in rows]
        else:
            result = resultset.fetchall()
        return result

    async def test_connection(self):
        """
        Test Connnection
        """
        error = None
        row = {}
        if self._test_query is None:
            raise NotImplementedError()
        if not self._connection:
            await self.connection()
        try:
            async with self._connection.begin() as conn:
                result = await conn.execute(text(self._test_query))
                row = await self.get_result(result)
            if error:
                self._logger.info(f"Test Error: {err!s}")
        except Exception as err:
            error = str(err)
            raise DriverError(message=str(err), code=0)
        finally:
            return [row, error]

    def valid_operation(self, sentence: Any):
        """
        Returns if is a valid operation.
        TODO: add some validations.
        """
        if not sentence:
            raise EmptyStatement(f"{__name__!s} Error: cannot use an empty SQL sentence")
        if not self._connection:
            self.connection()

    def query(self, sentence: Any, params: List = None):
        """
        Running Query.
        """
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            self.start_timing()
            self._logger.debug("Running Query {}".format(sentence))
            result = self._connection.execute(sentence, params)
            if result:
                rows = result.fetchall()
                if self._row_format == "dict" or self._row_format == "iterable":
                    self._result = [dict(zip(row.keys(), row)) for row in rows]
                elif self._row_format == "record":
                    self._result = [Record(row, row.keys()) for row in rows]
                else:
                    self._result = rows
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            self.generated_at()
            return [self._result, error]

    def queryrow(self, sentence: Any):
        """
        Running Query and return only one row.
        """
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            self._logger.debug("Running Query {}".format(sentence))
            result = self._connection.execute(sentence)
            if result:
                row = result.fetchone()
                if self._row_format == "dict":
                    self._result = dict(row)
                elif self._row_format == "iterable":
                    self._result = dict(zip(row.keys(), row))
                elif self._row_format == "record":
                    self._result = Record(row, row.keys())
                else:
                    self._result = row
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            return [self._result, error]

    def fetch_all(self, sentence: Any, params: List = None):
        """
        Running Query.
        """
        result = None
        self.valid_operation(sentence)
        try:
            self.start_timing()
            self._logger.debug("Running Query {}".format(sentence))
            result = self._connection.execute(sentence, params)
            if result:
                rows = result.fetchall()
                if self._row_format == "dict" or self._row_format == "iterable":
                    result = [dict(zip(row.keys(), row)) for row in rows]
                elif self._row_format == "record":
                    result = [Record(row, row.keys()) for row in rows]
                else:
                    result = rows
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            self.generated_at()
            return result

    def fetch_one(self, sentence: Any):
        """
        Running Query and return only one row.
        """
        result = None
        self.valid_operation(sentence)
        try:
            self._logger.debug("Running Query {}".format(sentence))
            result = self._connection.execute(sentence)
            if result:
                row = result.fetchone()
                if self._row_format == "dict":
                    result = dict(row)
                elif self._row_format == "iterable":
                    result = dict(zip(row.keys(), row))
                elif self._row_format == "record":
                    result = Record(row, row.keys())
                else:
                    result = row
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            return result

    fetchone = fetch_one

    def execute(self, sentence, params: List = None):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            self._logger.debug("Execute Sentence {}".format(sentence))
            result = self._connection.execute(sentence, params)
            self._result = result
        except (DatabaseError, OperationalError) as err:
            error = "Execute Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            return [self._result, error]

    def execute_many(self, sentence, params: List):
        """Execute multiples transactions."""
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            self._logger.debug("Execute Sentence {}".format(sentence))
            result = self._connection.execute(sentence, params)
            self._result = result
        except (DatabaseError, OperationalError) as err:
            error = "Execute Error: {}".format(str(err))
            raise DriverError(message=error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise DriverError(message=error)
        finally:
            return [self._result, error]

    executemany = execute_many

    """
    Transaction Context
    """

    def transaction(self):
        if not self._connection:
            self.connection()
        self._transaction = self._connection.begin()
        return self

    def commit(self):
        if self._transaction:
            self._transaction.commit()

    def rollback(self):
        if self._transaction:
            self._transaction.rollback()

    def close_transaction(self):
        if self._transaction:
            try:
                self._transaction.commit()
                self._transaction.close()
            except InvalidRequestError:
                # transaction inactive
                pass
            except (SQLAlchemyError, DatabaseError, OperationalError) as err:
                error = "Exception Error on Transaction: {}".format(str(err))
                raise DriverError(message=error)
            finally:
                self._transaction = None

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._transaction:
            self.close_transaction()
        self.release()

    """
    DDL Information.
    """

    def create(self, obj: str = "table", name: str = "", fields: Optional[List] = None) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if obj == "table":
            sql = "CREATE TABLE IF NOT EXISTS {name}({columns});"
            columns = ", ".join(["{name} {type}".format(**e) for e in fields])
            sql = sql.format(name=name, columns=columns)
            try:
                result = self._connection.execute(sql)
                if result:
                    return True
                else:
                    return False
            except ProgrammingError as err:
                raise DriverError(f"SQLAlchemy: Relation already exists: {err!s}")
            except Exception as err:
                raise DriverError(f"SQLAlchemy: Error in Object Creation: {err!s}")
        else:
            raise RuntimeError(f"SQLAlchemy: invalid Object type {object!s}")

    """
    Model Logic:
    """

    def column_info(self, tablename: str, schema: str = None):
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
        if self._driver == "postgresql":
            sql = f"SELECT a.attname AS name, a.atttypid::regtype AS type, \
            format_type(a.atttypid, a.atttypmod) as format_type, a.attnotnull::boolean as notnull, \
            coalesce((SELECT true FROM pg_index i WHERE i.indrelid = a.attrelid \
            AND i.indrelid = a.attrelid AND a.attnum = any(i.indkey) \
            AND i.indisprimary), false) as is_primary \
            FROM pg_attribute a WHERE a.attrelid = '{table!s}'::regclass \
            AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        else:
            raise NotImplementedError
        if not self._connection:
            self.connection()
        try:
            f = self._row_format
            self._row_format = "dict"
            colinfo = self.fetch_all(sql)
            self._row_format == f
            return colinfo
        except Exception as err:
            self._logger.exception(f"Wrong Table information {tablename!s}: {err}")

    """
    Metadata information.
    """

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def use(self, tablename: str):
        raise NotImplementedError("SQLAlchemy Error: There is no Database.")
