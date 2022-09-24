""" sqlalchemy.

non-async SQL Alchemy Provider.
Notes on sqlalchemy Provider
--------------------
This provider implements a basic set of funcionalities from SQLAlchemy core
"""

import asyncio
from typing import (
    Any,
    Dict,
    List,
    Optional
)
from collections.abc import Callable, Iterable
from psycopg2.extras import NamedTupleCursor, DictCursor
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.exc import (
    DatabaseError,
    OperationalError,
    SQLAlchemyError,
    ProgrammingError,
    InvalidRequestError
)
from sqlalchemy.pool import NullPool
from asyncdb.meta import Record
from asyncdb.exceptions import (
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.interfaces import (
    DBCursorBackend
)
from .sql import SQLDriver, SQLCursor


class sql_alchemyCursor(SQLCursor):
    _connection: Any = None

    def __enter__(self) -> "SQLCursor":
        self._cursor = self._connection.execute(
            self._sentence,
            self._params
        )
        return self

    def __next__(self):
        """Use `cursor.fetchrow()` to provide an iterable."""
        row = self._cursor.fetchone()
        if row is not None:
            return dict(row)
        else:
            raise StopIteration

class sql_alchemy(SQLDriver, DBCursorBackend):
    _provider = "sql_alchemy"
    _syntax = "sql"
    _test_query = "SELECT 1 as one"
    _engine_options: Dict = {
        "connect_args": {"connect_timeout": 360},
        "isolation_level": "AUTOCOMMIT",
        "echo": False,
        "encoding": "utf8",
        "implicit_returning": True,
    }
    setup_func: Optional[Callable] = None
    init_func: Optional[Callable] = None

    def __init__(
            self,
            dsn: str = "",
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
        ):
        """sql_alchemy.

        Args:
            dsn (str, optional): Connection DSN. Defaults to "".
            loop (asyncio.AbstractEventLoop, optional): optional Event Loop. Defaults to None.
            params (dict, optional): Connection Parameters. Defaults to None.
        """
        self._engine = None
        self._dsn = "{driver}://{user}:{password}@{host}:{port}/{database}"
        self._transaction = None
        self._driver = 'postgresql'
        self.__cursor__ = None
        if params:
            try:
                if not params["driver"]:
                    params["driver"] = "postgresql"
                else:
                    self._driver = params["driver"]
            except KeyError:
                params["driver"] = "postgresql"
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        self._options = self._engine_options
        if kwargs:
            self._options = {**self._options, **kwargs}
        self.set_engine()

    def engine(self):
        return self._engine

    def set_engine(self):
        self._connection = None
        try:
            self._engine = create_engine(self._dsn, poolclass=NullPool, **self._options)
        except (SQLAlchemyError, OperationalError) as err:
            self._connection = None
            raise ProviderError("Connection Error: {}".format(str(err)))
        except Exception as err:
            self._connection = None
            raise ProviderError(
                "Engine Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    def __del__(self):
        self.close()
        del self._connection
        del self._engine

    def close(self):
        if self._connection:
            self._connection.close()
        self._logger.info("Closing Connection: {}".format(self._engine))
        self._connected = False

    def terminate(self):
        self.close()

    def release(self):
        if self._connection:
            try:
                if not self._connection.closed:
                    self._connection.close()
            except Exception as err:
                self._connection = None
                raise ProviderError(
                    "Engine Error, Terminated: {}".format(str(err)))
            finally:
                self._connection = None
                self._connected = False
                return True

    def connection(self):
        """
        Get a connection
        """
        self._logger.info("SQLAlchemy: Connecting to {}".format(self._dsn))
        self._connection = None
        self._connected = False
        try:
            self._connection = self._engine.connect()
            self._connected = True
        except (SQLAlchemyError, DatabaseError, OperationalError) as err:
            self._connection = None
            raise ProviderError(
                "SQL Alchemy: Connection Error: {}".format(str(err))
            )
        except Exception as err:
            self._connection = None
            raise ProviderError(
                "SQL Alchemy: Engine Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    def prepare(self, sentence=""):
        """
        Preparing a sentence.
        """
        error = None
        raise NotImplementedError()

    def test_connection(self):
        """
        Test Connnection
        """
        error = None
        row = {}
        if self._test_query is None:
            raise NotImplementedError()
        if not self._connection:
            self.connection()
        try:
            result = self._connection.execute(self._test_query)
            row = result.fetchone()
            if row:
                if self._row_format == 'dict' or self._row_format == 'iterable':
                    row = dict(row)
                elif self._row_format == 'record':
                    row = Record(row, row.keys())
                else:
                    pass
            if error:
                self._logger.info("Test Error: {}".format(error))
        except Exception as err:
            error = str(err)
            raise ProviderError(message=str(err), code=0)
        finally:
            return [row, error]

    def valid_operation(self, sentence: Any):
        """
        Returns if is a valid operation.
        TODO: add some validations.
        """
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: cannot use an empty SQL sentence"
            )
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
                if self._row_format == 'dict' or self._row_format == 'iterable':
                    self._result = [dict(zip(row.keys(), row)) for row in rows]
                elif self._row_format == 'record':
                    self._result = [Record(row, row.keys()) for row in rows]
                else:
                    self._result = rows
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
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
                if self._row_format == 'dict':
                    self._result = dict(row)
                elif self._row_format == 'iterable':
                    self._result = dict(zip(row.keys(), row))
                elif self._row_format == 'record':
                    self._result = Record(row, row.keys())
                else:
                    self._result = row
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
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
                if self._row_format == 'dict' or self._row_format == 'iterable':
                    result = [dict(zip(row.keys(), row)) for row in rows]
                elif self._row_format == 'record':
                    result = [Record(row, row.keys()) for row in rows]
                else:
                    result = rows
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
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
                if self._row_format == 'dict':
                    result = dict(row)
                elif self._row_format == 'iterable':
                    result = dict(zip(row.keys(), row))
                elif self._row_format == 'record':
                    result = Record(row, row.keys())
                else:
                    result = row
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
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
            raise ProviderError(message=error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            return [self._result, error]

    def execute_many(self, sentence, params: List):
        """Execute multiples transactions.
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
            raise ProviderError(message=error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise ProviderError(message=error)
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
                raise ProviderError(message=error)
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
    def create(
        self,
        obj: str = 'table',
        name: str = '',
        fields: Optional[List] = None
    ) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if obj == 'table':
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
                raise ProviderError(
                    f"SQLAlchemy: Relation already exists: {err!s}"
                )
            except Exception as err:
                raise ProviderError(
                    f"SQLAlchemy: Error in Object Creation: {err!s}"
                )
        else:
            raise RuntimeError(f'SQLAlchemy: invalid Object type {object!s}')

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
        if self._driver == 'postgresql':
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
            self._row_format = 'dict'
            colinfo = self.fetch_all(sql)
            self._row_format == f
            return colinfo
        except Exception as err:
            self._logger.exception(
                f"Wrong Table information {tablename!s}: {err}"
            )

    """
    Metadata information.
    """
    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def use(self, tablename: str):
        raise NotImplementedError(
            'SQLAlchemy Error: There is no Database.'
        )
