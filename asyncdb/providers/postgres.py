""" postgres PostgreSQL Provider.
Notes on pg Provider
--------------------
This provider implements all funcionalities from asyncpg
(cursors, transactions, copy from and to files, pools, native data types, etc) but using Threads
"""
import sys
from datetime import datetime
import time
import json

import asyncio
import asyncpg
from threading import Thread

from asyncpg.exceptions import InvalidSQLStatementNameError, TooManyConnectionsError, InternalClientError, ConnectionDoesNotExistError, InterfaceError, InterfaceWarning, PostgresError, PostgresSyntaxError, FatalPostgresError, UndefinedTableError, UndefinedColumnError
from asyncdb.providers import BasePool, BaseProvider, registerProvider, exception_handler, logger_config

from asyncdb.providers.exceptions import EmptyStatement, ConnectionTimeout, ProviderError, NoDataFound, StatementError, TooManyConnections, DataError
from asyncdb.utils import EnumEncoder, SafeDict
from asyncdb.meta import asyncResult, asyncRecord

from logging.config import dictConfig
dictConfig(logger_config)

class postgres(BaseProvider, Thread):
    _provider = 'postgresql'
    _syntax = 'sql'
    _test_query = "SELECT 1"
    _dsn = 'postgres://{user}:{password}@{host}:{port}/{database}'
    _loop = None
    _connection = None
    _connected = False
    _prepared = None
    _parameters = ()
    _cursor = None
    _transaction = None
    _initialized_on = None
    init_func = None
    _query_raw = 'SELECT {fields} FROM {table} {where_cond}'
    _is_started = False

    def __init__(self, dsn='', loop=None, pool=None, params={}, **kwargs):
        self._params = params
        if not dsn:
            self._dsn = self.create_dsn(self._params)
        else:
            self._dsn = dsn
        try:
            self._timeout = kwargs['timeout']
        except KeyError:
            pass
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.new_event_loop()
        #asyncio.set_event_loop(self._loop)
        #self._loop.set_exception_handler(exception_handler)
        #self._loop.set_debug(self._DEBUG)
        # calling parent Thread
        Thread.__init__(self)
        self._logger = logging.getLogger(__name__)

    """
    Thread Methodss
    """
    def start(self):
        #print('Running Start')
        super(postgres, self).start()

    def join(self, timeout=5):
        #print('Running Join')
        super(postgres, self).join(timeout=timeout)

    def run(self):
      #print ("Starting Connection")
      super(postgres, self).run()

    """
    Async Context magic Methods
    """
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # clean up anything you need to clean up
        await self.release(wait_close=5)
        pass

    """
    Context magic Methods
    """
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._loop.run_until_complete(self.release())

    async def init_connection(self, connection):
        # Setup jsonb encoder/decoder
        def _encoder(value):
            return json.dumps(value, cls=EnumEncoder)
        def _decoder(value):
            return json.loads(value)
        def interval_encoder(delta):
            ndelta = delta.normalized()
            return (ndelta.years * 12 + ndelta.months, ndelta.days,
                 ((ndelta.hours * 3600 +
                    ndelta.minutes * 60 +
                    ndelta.seconds) * 1000000 +
                  ndelta.microseconds))
        def interval_decoder(tup):
            return relativedelta(months=tup[0], days=tup[1], microseconds=tup[2])
        await connection.set_type_codec('json', encoder=_encoder, decoder=_decoder, schema='pg_catalog')
        await connection.set_type_codec('jsonb', encoder=_encoder, decoder=_decoder, schema='pg_catalog' )
        await connection.set_builtin_type_codec('hstore', codec_name='pg_contrib.hstore')
        await connection.set_type_codec('interval', schema='pg_catalog', encoder=interval_encoder, decoder=interval_decoder, format='tuple')
        if self.init_func:
            try:
                await self.init_func(connection)
            except Exception as err:
                print('Error on Init Connection: {}'.format(err))
                pass

    def terminate(self):
        if self._loop.is_running():
            self._loop.stop()
        if not self._loop.is_closed():
            self._loop.close()
        # finish the main thread
        try:
            self.join(timeout=5)
        finally:
            self._logger.info('Thread Killed')
            return


    def connect(self):
        """
        connect.

        sync-version of connection, for use with sync-methods
        """
        return self._loop.run_until_complete(self.connection())
        #self._loop.run_until_complete(asyncio.wait([task]))

    async def connection(self):
        """
        connection.

        Get a connection from DB
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await asyncpg.connect(
                dsn=self._dsn,
                loop=self._loop,
                command_timeout=self._timeout,
                timeout= self._timeout
            )
            if self._connection:
                self._logger.info("Open Connection to {}, id: {}".format(self._dsn, self._connection.get_server_pid()))
                await self.init_connection(self._connection)
                self._connected = True
                self._initialized_on = time.time()
        except TooManyConnectionsError as err:
            print(err)
            raise TooManyConnections("Too Many Connections Error: {}".format(str(err)))
        except ConnectionDoesNotExistError as err:
            print(err)
            print("Connection Error: {}".format(str(err)))
            raise ProviderError("Connection Error: {}".format(str(err)))
        except InternalClientError as err:
            print("Internal Error: {}".format(str(err)))
            raise ProviderError("Internal Error: {}".format(str(err)))
        except InterfaceError as err:
            print("Interface Error: {}".format(str(err)))
            raise ProviderError("Interface Error: {}".format(str(err)))
        except InterfaceWarning as err:
            print("Interface Warning: {}".format(str(err)))
        except Exception as err:
            print(err)
        finally:
            if not self._is_started:
                self.start() # start the thread
                self._is_started = True
            return self

    async def close(self, timeout=5):
        """
        close.
            Closing a Connection
        """
        try:
            if self._connection:
                if not self._connection.is_closed():
                    print('CLOSING')
                    self._logger.info("Closing Connection, id: {}".format(self._connection.get_server_pid()))
                    try:
                        await self._connection.close(timeout=timeout)
                        self.join(timeout=timeout)
                    except InterfaceError as err:
                        raise ProviderError("Close Error: {}".format(str(err)))
                    except Exception as err:
                        await self._connection.terminate()
                        self._connection = None
                        raise ProviderError("Connection Error, Terminated: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False


    async def release(self, wait_close=10):
        """
        Release a Connection
        """
        if self._connection:
            try:
                if not self._connection.is_closed():
                    await self._connection.close(timeout = wait_close)
            except (InterfaceError, RuntimeError) as err:
                raise ProviderError("Release Interface Error: {}".format(str(err)))
                return False
            finally:
                self.join(timeout=wait_close)
                self._connected = False
                self._connection = None

    @property
    def connected(self):
        if self._connection:
            return not self._connection.is_closed()


    async def prepare(self, sentence='', *args):
        """
        Preparing a sentence
        """
        stmt = None
        self._columns = []
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence, *args)
            self._columns = [a.name for a in stmt.get_attributes()]
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence on Query Row Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        finally:
            # return the prepared statement
            return stmt

    async def columns(self, sentence, *args):
        self._columns = []
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence, *args)
            self._columns = [a.name for a in stmt.get_attributes()]
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence on Query Row Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        finally:
            return self._columns

    async def query(self, sentence=''):
        """
        Query.

            Make a query to DB
        """
        error = None
        try:
            startTime = datetime.now()
            self._result = await self._connection.fetch(sentence)
            if not self._result:
                return [None, 'Data was not found']
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            startTime = 0
            return [self._result, error]

    async def queryrow(self, sentence=''):
        """
        queryrow.

            Make a query to DB returning only one row
        """
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence)
            self._columns = [a.name for a in stmt.get_attributes()]
            self._result = await stmt.fetchrow()
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence on Query Row Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    async def execute(self, sentence='', *args):
        """execute.

        Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            result = await self._connection.execute(sentence, *args)
            return [result, None]
        except InterfaceWarning as err:
            error = "Interface Warning: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
        finally:
            self.join()
            return [result, error]

    async def executemany(self, sentence='', *args, timeout=None):
        """execute.

        Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            async with self._connection.transaction():
                await self._connection.executemany(sentence, timeout=timeout, *args)
            return [True, None]
        except InterfaceWarning as err:
            error = "Interface Warning: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
        finally:
            self.join()
            return [result, error]

    """
    Transaction Context
    """
    async def transaction(self):
        if not self._connection:
            await self.connection()
        self._transaction = self._connection.transaction()
        await self._transaction.start()
        return self

    async def commit(self):
        if self._transaction:
            await self._transaction.commit()

    async def rollback(self):
        if self._transaction:
            await self._transaction.rollback()

    """
    Cursor Context
    """
    async def cursor(self, sentence):
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        self._transaction = self._connection.transaction()
        await self._transaction.start()
        self._cursor = await self._connection.cursor(sentence)
        return self

    async def forward(self, number):
        try:
            return await self._cursor.forward(number)
        except Exception as err:
            error = "Error forward Cursor: {}".format(str(err))
            raise Exception(error)

    async def get(self, number = 1):
        try:
            return await self._cursor.fetch(number)
        except Exception as err:
            error = "Error Fetch Cursor: {}".format(str(err))
            raise Exception(error)

    async def getrow(self):
        try:
            return await self._cursor.fetchrow()
        except Exception as err:
            error = "Error Fetchrow Cursor: {}".format(str(err))
            raise Exception(error)

    """
    Cursor Iterator Context
    """
    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

    """
    Non-Async Methods
    """
    def fetchall(self, sentence):
        error = None
        self._result = None
        try:
            stmt = self._loop.run_until_complete(self.prepare(sentence))
            if stmt:
                result = self._loop.run_until_complete(stmt.fetch())
                self._result = asyncResult(result=result, columns=self._columns)
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    def fetchone(self, sentence):
        error = None
        self._result = None
        try:
            row = self._loop.run_until_complete(self._connection.fetchrow(sentence))
            if row:
                self._result = asyncRecord(dict(row))
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence on Query Row Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.InvalidSQLStatementNameError, asyncpg.exceptions.UndefinedTableError) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]


"""
Registering this Provider
"""
registerProvider(postgres)
