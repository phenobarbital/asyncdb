""" sqlalchemy.

non-async SQL Alchemy Provider.
Notes on sqlalchemy Provider
--------------------
This provider implements a basic set of funcionalities from SQLAlchemy core
"""

import asyncio
import time

from psycopg2.extras import NamedTupleCursor
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.exc import (
    DatabaseError,
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.pool import NullPool

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


class sql_alchemy(BaseProvider):
    _provider = "sqlalchemy"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _dsn = "{driver}://{user}:{password}@{host}:{port}/{database}"
    _loop = None
    _pool = None
    # _engine = None
    _connection = None
    _connected = False
    _initialized_on = None
    _engine = None
    _engine_options = {
        "connect_args": {
            "connect_timeout": 360
        },
        "isolation_level": "AUTOCOMMIT",
        "echo": False,
        "encoding": "utf8",
        "implicit_returning": True,
    }
    _transaction = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        if params:
            try:
                if not params["driver"]:
                    params["driver"] = "postgresql"
            except KeyError:
                params["driver"] = "postgresql"
        super(sql_alchemy, self).__init__(dsn, loop, params, **kwargs)
        asyncio.set_event_loop(self._loop)
        if kwargs:
            self._engine_options = {**self._engine_options, **kwargs}
        self.set_engine()

    def engine(self):
        return self._engine

    def set_engine(self):
        self._connection = None
        try:
            self._engine = create_engine(self._dsn, **self._engine_options)
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
        self._engine.dispose()

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
                    "Engine Error, Terminated: {}".format(str(err))
                )
            finally:
                self._connection = None
                return True

    def connection(self):
        """
        Get a connection
        """
        self._logger.info("SQLAlchemy: Connecting to {}".format(self._dsn))
        self._connection = None
        self._connected = False
        try:
            if self._engine:
                self._connection = self._engine.connect()
                self._connected = True
        except (SQLAlchemyError, DatabaseError, OperationalError) as err:
            self._connection = None
            print(err)
            raise ProviderError("Connection Error: {}".format(str(err)))
        except Exception as err:
            print(err)
            self._connection = None
            raise ProviderError(
                "Engine Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    def prepare(self, sentence=""):
        """
        Preparing a sentence
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
                row = dict(row)
            if error:
                self._logger.info("Test Error: {}".format(error))
        except Exception as err:
            error = str(err)
            raise ProviderError(message=str(err), code=0)
        finally:
            return [row, error]

    def query(self, sentence):
        """
        Running Query
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.info("Running Query {}".format(sentence))
            result = self._connection.execute(sentence)
            if result:
                rows = result.fetchall()
                self._result = [dict(zip(row.keys(), row)) for row in rows]
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise ProviderError(error)
        finally:
            return [self._result, error]

    def queryrow(self, sentence=""):
        """
        Running Query and return only one row
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.debug("Running Query {}".format(sentence))
            result = self._connection.execute(sentence)
            if result:
                row = result.fetchone()
                self._result = dict(zip(row.keys(), row))
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise ProviderError(error)
        finally:
            return [self._result, error]

    def execute(self, sentence):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.debug("Execute Sentence {}".format(sentence))
            result = self._connection.execute(sentence)
            self._result = result
        except (DatabaseError, OperationalError) as err:
            error = "Execute Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise ProviderError(error)
        finally:
            return [self._result, error]

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
            except (SQLAlchemyError, DatabaseError, OperationalError) as err:
                print(err)
                error = "Exception Error on Transaction: {}".format(str(err))
                raise ProviderError(error)

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
Registering this Provider
"""
registerProvider(sql_alchemy)
