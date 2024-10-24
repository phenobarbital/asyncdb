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
from asyncdb.meta.record import Record
from ..exceptions import (
    EmptyStatement,
    DriverError,
)
from ..interfaces.cursors import DBCursorBackend
from ..utils.encoders import json_encoder, json_decoder
from .sql import SQLDriver, SQLCursor


class saCursor(SQLCursor):
    _connection: Any = None

    async def __aenter__(self) -> "SQLCursor":
        try:
            async with self._connection.connect() as conn:
                self._cursor = await conn.execute(text(self._sentence), self._params)
                # Create an iterator from the result
                self._result_iter = self._cursor.__iter__()
        except Exception as e:
            raise DriverError(f"SQLAlchemy Error: {e}") from e
        return self

    async def __anext__(self):
        # Retrieve the next item in the result set
        try:
            return next(self._result_iter)
        except StopIteration as e:
            raise StopAsyncIteration from e


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
        """sa.

        The SQL Alchemy based driver for AsyncDB.
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
            self._driver = params.get("driver", "postgresql+asyncpg")
        else:
            params = {"driver": "postgresql+asyncpg"}
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
                raise DriverError(f"Engine Error, Terminated: {err!s}") from err
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
            self._connection = create_async_engine(self._dsn, **self._options)
            self._session = AsyncSession(bind=self._connection)
            self._connected = True
        except (SQLAlchemyError, OperationalError) as err:
            self._connection = None
            raise DriverError(f"Connection Error: {err!s}")
        except Exception as err:
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

    def get_connection(self):
        return self._connection.connect()

    def get_engine(self):
        return self._session

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

    def text(self, sentence: str):
        return text(sentence)

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
                self._logger.info(f"Test Error: {error!s}")
        except Exception as err:
            error = str(err)
            raise DriverError(message=str(err), code=0)
        finally:
            return [row, error]

    async def valid_operation(self, sentence: Any):
        """
        Returns if is a valid operation.
        TODO: add some validations.
        """
        if not sentence:
            raise EmptyStatement(f"{__name__!s} Error: cannot use an empty SQL sentence")
        if not self._connection:
            await self.connection()

    def _construct_record(self, row, column_names):
        return Record(dict(zip(column_names, row)), column_names)

    async def query(self, sentence: Any, params: List = None, query_format: str = None):
        """
        Running Query.
        """
        self._result = None
        error = None
        await self.valid_operation(sentence)
        if not query_format:
            query_format = self._row_format
        try:
            self.start_timing()
            async with self._connection.connect() as conn:
                if isinstance(sentence, str):
                    sentence = text(sentence)
                result = await conn.execute(sentence, params)
                rows = result.fetchall()
                # Get the column names from the result metadata
                column_names = result.keys()
                if query_format in ("dict", "iterable"):
                    self._result = [dict(zip(column_names, row)) for row in rows]
                elif query_format == "record":
                    self._result = [self._construct_record(row, column_names) for row in rows]
                else:
                    self._result = rows
        except (DatabaseError, OperationalError) as err:
            error = DriverError(f"Query Error: {err}")
        except Exception as err:
            print("ERROR > ", err)
            error = DriverError(f"Query Error, Terminated: {err}")
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def queryrow(self, sentence: Any, params: Any = None, query_format: Optional[str] = None):
        """
        Running Query and return only one row.
        """
        self._result = None
        error = None
        await self.valid_operation(sentence)
        try:
            if not query_format:
                query_format = self._row_format
            result = None
            async with self._connection.connect() as conn:
                if isinstance(sentence, str):
                    sentence = text(sentence)
                result = await conn.execute(sentence, params)
                column_names = result.keys()
                row = result.fetchone()
                if query_format in ("dict", "iterable"):
                    self._result = dict(zip(column_names, row))
                elif query_format == "record":
                    self._result = self._construct_record(row, column_names)
                else:
                    self._result = row
        except (DatabaseError, OperationalError) as err:
            error = DriverError(f"Query Error: {err}")
        except Exception as err:
            error = DriverError(f"Query Error, Terminated: {err}")
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def fetch_all(self, sentence: Any, params: List = None, query_format: Optional[str] = None):
        """
        Fetch All Rows in a Query.
        """
        result = None
        await self.valid_operation(sentence)
        try:
            if not query_format:
                query_format = self._row_format
            async with self._connection.connect() as conn:
                if isinstance(sentence, str):
                    sentence = text(sentence)
                rst = await conn.execute(sentence, params)
                column_names = rst.keys()
                rows = rst.fetchall()
                if rows is None:
                    return None
                if query_format in ("dict", "iterable"):
                    result = [dict(zip(column_names, row)) for row in rows]
                elif query_format == "record":
                    result = [self._construct_record(row, column_names) for row in rows]
                else:
                    result = rows
        except (DatabaseError, OperationalError) as err:
            raise DriverError(f"Query Error: {err}")
        except Exception as err:
            raise DriverError(f"Query Error, Terminated: {err}")
        finally:
            self.generated_at()
            return result

    async def fetch_many(self, sentence: Any, size: int = 1, params: List = None, query_format: Optional[str] = None):
        """
        Fetch Many Rows from a Query as requested.
        """
        result = None
        await self.valid_operation(sentence)
        try:
            if not query_format:
                query_format = self._row_format
            async with self._connection.connect() as conn:
                if isinstance(sentence, str):
                    sentence = text(sentence)
                rst = await conn.execute(sentence, params)
                column_names = rst.keys()
                rows = rst.fetchmany(size)
                if rows is None:
                    return None
                if query_format in ("dict", "iterable"):
                    result = [dict(zip(column_names, row)) for row in rows]
                elif query_format == "record":
                    result = [self._construct_record(row, column_names) for row in rows]
                else:
                    result = rows
        except (DatabaseError, OperationalError) as err:
            raise DriverError(f"Query Error: {err}")
        except Exception as err:
            raise DriverError(f"Query Error, Terminated: {err}")
        finally:
            self.generated_at()
            return result

    fetchmany = fetch_many

    async def fetch_one(self, sentence: Any, params: List = None, query_format: Optional[str] = None):
        """
        Running Query and return only one row.
        """
        result = None
        await self.valid_operation(sentence)
        try:
            if not query_format:
                query_format = self._row_format
            async with self._connection.connect() as conn:
                if isinstance(sentence, str):
                    sentence = text(sentence)
                rst = await conn.execute(sentence, params)
                column_names = rst.keys()
                row = rst.fetchone()
                if row is None:
                    return None
                if query_format in ("dict", "iterable"):
                    result = dict(zip(column_names, row))
                elif query_format == "record":
                    result = Record(dict(zip(column_names, row)), column_names)
                else:
                    result = row
        except (DatabaseError, OperationalError) as err:
            raise DriverError(f"Query Error: {err}")
        except Exception as err:
            raise DriverError(f"Query Error, Terminated: {err}")
        finally:
            self.generated_at()
            return result

    fetchone = fetch_one

    async def execute(self, sentence, params: List = None):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            if isinstance(sentence, str):
                sentence = text(sentence)
            async with self._connection.begin() as conn:
                result = await conn.execute(sentence, params)
                # row = await self.get_result(result)
                self._result = result
        except (DatabaseError, OperationalError) as err:
            error = DriverError(f"Execute Error: {err!s}")
        except Exception as err:
            error = DriverError(f"Exception Error on Execute: {err!s}")
        finally:
            return [self._result, error]

    async def execute_many(self, sentence: list, params: List):
        """Execute multiples transactions."""
        self._result = None
        error = None
        self.valid_operation(sentence)
        try:
            async with self._connection.begin() as conn:
                results = []
                for query in sentence:
                    if isinstance(query, str):
                        query = text(query)
                    result = await conn.execute(query, params)
                    results.append(result)
                self._result = results
        except (DatabaseError, OperationalError) as err:
            error = DriverError(f"Execute Error: {err}")
        except Exception as err:
            error = DriverError(f"Exception on Execute Many: {err!s}")
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
                error = f"Exception Error on Transaction: {err}"
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
