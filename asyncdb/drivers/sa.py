""" sa, sqlalchemy async Provider.
Notes on sqlalchemy async Provider
--------------------
This provider implements a basic set of funcionalities from SQLAlchemy core and use threads
"""
import asyncio
from threading import Thread
from psycopg2.extras import NamedTupleCursor
from sqlalchemy import create_engine, select
from sqlalchemy.exc import DatabaseError, OperationalError, SQLAlchemyError
# from sqlalchemy_aio import ASYNCIO_STRATEGY
from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)

from .sql import SQLDriver

class sa(SQLDriver, Thread):
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

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        super(sa, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        # create the variables
        self._result = None
        self._connection = None
        self._engine = None
        self._loop = None
        # create a new loop before thread
        self._loop = asyncio.new_event_loop()
        # calling parent Thread
        Thread.__init__(self)
        self.connect()

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._loop.run_until_complete(self.release())

    """
    Thread Methodss
    """

    def start(self):
        self._logger.debug("Running Start")
        Thread.start(self)

    def join(self):
        self._logger.debug("Running Join")
        Thread.join(self)

    def connect(self):
        self._logger.debug("Running Connect")
        try:
            self._engine = create_engine(self._dsn)
        except (SQLAlchemyError, DatabaseError, OperationalError) as err:
            self._engine = None
            raise ProviderError("Connection Error: {}".format(str(err)))
        except Exception as err:
            self._engine = None
            raise ProviderError("Engine Error, Terminated: {}".format(str(err)))

    def close(self):
        self._logger.debug("Running Close")
        if self._loop:
            try:
                self._loop.run_until_complete(
                    asyncio.wait_for(self.terminate(), timeout=5)
                )
            finally:
                # close loop
                self._loop.close()

    async def terminate(self):
        """
        Closing a Connection
        """
        if self._connection:
            try:
                await self._engine.release(self._connection)
            except Exception as err:
                await self._connection.close()
        if self._engine:
            self._engine.close()
            try:
                await self._engine.wait_closed()
            finally:
                self._engine.terminate()

    def connection(self):
        """
        Get a connection
        """
        self._logger.debug("SQLAlchemy: Connecting to {}".format(self._dsn))
        self._connection = None
        self._connected = False
        self.start()
        try:
            if self._engine:
                self._connection = self._loop.run_until_complete(self._engine.connect())
        except (SQLAlchemyError, DatabaseError, OperationalError) as err:
            self._connection = None
            raise ProviderError("Connection Error: {}".format(str(err)))
        except Exception as err:
            self._connection = None
            raise ProviderError("Engine Error, Terminated: {}".format(str(err)))
        finally:
            return self

    async def release(self):
        """
        Release a Connection object
        """
        try:
            await self._connection.close()
        except Exception as err:
            raise ProviderError("Release Error, Terminated: {}".format(str(err)))
        finally:
            self._connection = None

    async def prepare(self, sentence=""):
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
        self._logger.debug("{}: Running Test".format(self._provider))
        try:
            result = self._loop.run_until_complete(
                self._connection.execute(self._test_query)
            )
            row = self._loop.run_until_complete(result.fetchone())
            if row:
                row = dict(row)
            if error:
                self._logger.debug("Test Error: {}".format(error))
        except Exception as err:
            error = str(err)
            raise ProviderError(message=str(err), code=0)
        finally:
            return [row, error]

    async def query(self, sentence):
        """
        Running Query
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.debug("Running Query {}".format(sentence))
            result = await self._connection.execute(sentence)
            if result:
                rows = await result.fetchall()
                self._result = [dict(row.items()) for row in rows]
        except (DatabaseError, OperationalError) as err:
            error = "Query Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            return [self._result, error]

    async def queryrow(self, sentence=""):
        """
        Running Query and return only one row
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.debug("Running Query {}".format(sentence))
            result = await self._connection.execute(sentence)
            if result:
                row = await result.fetchone()
                self._result = dict(row)
        except (DatabaseError, OperationalError) as err:
            error = "Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Query Row Error, Terminated: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            return [self._result, error]

    async def execute(self, sentence):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        if not self._connection:
            self.connection()
        try:
            self._logger.debug("Execute Sentence {}".format(sentence))
            result = await self._engine.execute(sentence)
            self._result = result
        except (DatabaseError, OperationalError) as err:
            error = "Execute Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Exception Error on Execute: {}".format(str(err))
            raise ProviderError(message=error)
        finally:
            return [self._result, error]
