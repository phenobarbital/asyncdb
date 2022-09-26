""" postgres PostgreSQL Provider.
Notes on pg Provider
--------------------
This provider implements all funcionalities from asyncpg
(cursors, transactions, copy from and to files, pools, native data types, etc)
but using Threads.
"""
import os
import asyncio
import json
import threading
import time
from threading import Thread
from typing import (
    Any,
    Optional
)
from collections.abc import Iterable
from dateutil.relativedelta import relativedelta
import asyncpg
import uvloop
from asyncpg.exceptions import (
    ConnectionDoesNotExistError,
    InterfaceError,
    InterfaceWarning,
    InternalClientError,
    PostgresError,
    PostgresSyntaxError,
    TooManyConnectionsError,
    UndefinedColumnError,
    InvalidSQLStatementNameError,
    UndefinedTableError
)
from asyncdb.exceptions import (
    UninitializedError,
    EmptyStatement,
    ConnectionTimeout,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
    DriverError
)

from asyncdb.utils.encoders import (
    BaseEncoder,
)
from asyncdb.meta import Recordset
from .sql import SQLDriver
# from .abstract import BaseCursor


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


class postgres(threading.Thread, SQLDriver):
    _provider = "postgres"
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._dsn = "postgres://{user}:{password}@{host}:{port}/{database}"
        self.application_name = os.getenv('APP_NAME', "NAV")
        self._is_started = False
        self._error = None
        self._params = params
        self._result = None
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        # calling parent Thread
        Thread.__init__(self, name=self._provider)
        self.stop_event = threading.Event()

    def get_connection(self):
        self.join(timeout=self._timeout)
        return self._connection

## Thread Methodss
    def start(self, target=None, args=()):
        if target:
            Thread.__init__(self, target=target, args=args)
        else:
            Thread.__init__(self, name="postgres")
        super(postgres, self).start()

    def join(self, timeout=5):
        super(postgres, self).join(timeout=timeout)

    def stop(self):
        self.stop_event.set()


## Async Context magic Methods
    async def __aenter__(self):
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # clean up anything you need to clean up
        await self.close(timeout=5)

### Context magic Methods
    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback, *args):
        self.start(target=self.release)
        self.stop()
        self.join(timeout=self._timeout)

    async def init_connection(self, connection):
        # Setup jsonb encoder/decoder
        def _encoder(value):
            return json.dumps(value, cls=BaseEncoder)

        def _decoder(value):
            return json.loads(value)

        def interval_encoder(delta):
            ndelta = delta.normalized()
            return (
                ndelta.years * 12 + ndelta.months,
                ndelta.days,
                (
                    (ndelta.hours * 3600 + ndelta.minutes * 60 + ndelta.seconds)
                    * 1000000
                    + ndelta.microseconds
                ),
            )

        def interval_decoder(tup):
            return relativedelta(months=tup[0], days=tup[1], microseconds=tup[2])

        await connection.set_type_codec(
            "json", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_type_codec(
            "jsonb", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_builtin_type_codec(
            "hstore", codec_name="pg_contrib.hstore"
        )
        await connection.set_type_codec(
            "interval",
            schema="pg_catalog",
            encoder=interval_encoder,
            decoder=interval_decoder,
            format="tuple",
        )
        if self._init_func and callable(self._init_func):
            try:
                await self._init_func(connection) # pylint: disable=E1102
            except (RuntimeError, ValueError) as err:
                self._logger.debug(
                    f"Error on Init Connection: {err}"
                )

    def disconnect(self):
        if self._loop.is_running():
            self._loop.stop()
        self._loop.close()
        # finish the main thread
        try:
            self.join(timeout=5)
        finally:
            self._connection = None
            self._connected = False

    terminate = disconnect

    def is_closed(self):
        return not self._connection

    def connect(self):
        """
        connect.

        sync-version of connection, for use with sync-methods
        """
        self._connection = None
        self._connected = False
        if not self._is_started:
            self.start(target=self._connect)  # start a thread
            self._is_started = True
            self.join(timeout=self._timeout)
            self._connected = True
        return self

    open = connect

    def _connect(self):
        if not self._connection:
            self._loop.run_until_complete(
                self.connection()
            )

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
                timeout=self._timeout,
            )
            if self._connection:
                await self.init_connection(self._connection)
                self._connected = True
                self._initialized_on = time.time()
            return self
        except ConnectionRefusedError as err:
            raise UninitializedError(
                f"Unable to connect to database, connection Refused: {err}"
            ) from err
        except TooManyConnectionsError as err:
            self._logger.error(
                f"Too Many Connections Error: {err}"
            )
            raise TooManyConnections(
                f"Too Many Connections Error: {err}"
            ) from err
        except TimeoutError as err:
            raise ConnectionTimeout(
                f"Unable to connect to database: {err}"
            ) from err
        except ConnectionDoesNotExistError as err:
            raise ProviderError(
                f"Connection Error: {err}"
            ) from err
        except ConnectionError as ex:
            self._logger.error(
                f"Connection Error: {ex}"
            )
            raise UninitializedError(
                f"Connection Error: {ex}"
            ) from ex
        except InternalClientError as err:
            raise ProviderError(
                f"Internal Error: {err}"
            ) from err
        except InterfaceError as err:
            raise ProviderError(
                f"Interface Error: {err}"
            ) from err
        except InterfaceWarning as err:
            self._logger.warning(
                f"Interface Warning: {err}"
            )
            return False
        except Exception as ex:
            self._logger.exception(
                f"Asyncpg Unknown Error: {ex}",
                stack_info=True
            )
            raise DriverError(
                f"Asyncpg Unknown Error: {ex}"
            ) from ex
        finally:
            if not self._is_started:
                self.start()  # start a thread
                self._is_started = True

    async def close(self, timeout=5):
        """
        close.
            Closing a Connection
        """
        try:
            if self._connection:
                if not self._connection.is_closed():
                    await self._connection.close(timeout=timeout)
                    self.join(timeout=timeout)
        except InterfaceError as err:
            raise ProviderError(
                f"Close Error: {err}"
            ) from err
        except Exception as err:
            await self._connection.terminate()
            self._connection = None
            raise ProviderError(
                f"Connection Error, Terminated: {err}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    def release(self, wait_close=10):
        """
        Release a Connection
        """
        if self._connection:
            try:
                if not self._connection.is_closed():
                    self._loop.run_until_complete(
                        self._connection.close(timeout=wait_close)
                    )
            except (InterfaceError, RuntimeError) as err:
                raise ProviderError(
                    message=f"Release Interface Error: {err!s}"
                ) from err
            except Exception as err:
                raise ProviderError(
                    f"Connection Error, Terminated: {err}"
                ) from err
            finally:
                self._connected = False
                self._connection = None

    @property
    def connected(self):
        if self._connection:
            return not self._connection.is_closed()

    async def prepare(self, sentence: Any, *args):
        """
        Preparing a sentence
        """
        stmt = None
        error = None
        self._columns = []
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence, *args)
            self._columns = [a.name for a in stmt.get_attributes()]
            self._prepared = stmt
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Query Row Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Sentence on Query Row Error: {err}"
            ) from err
        except PostgresError as err:
            raise DriverError(
                f"Postgres Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on prepare Row: {err}"
            ) from err
        finally:
            return [self._prepared, error] # pylint: disable=W0150

    async def columns(self, sentence, *args): # pylint: disable=W0236,W0221
        self._columns = []
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence, *args)
            self._columns = [a.name for a in stmt.get_attributes()]
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Query Row Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Sentence Error: {err}"
            ) from err
        except PostgresError as err:
            raise DriverError(
                f"Postgres Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Column: {err}"
            ) from err

    async def query(self, sentence: Any, **kwargs):
        """
        Query.

            Make a query to DB
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            self._result = await self._connection.fetch(sentence)
            if not self._result:
                return [None, NoDataFound("No data was found")]
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Query Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Sentence Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Query: {err}"
            ) from err
        finally:
            self.generated_at()
            return await self._serializer(self._result, error) # pylint: disable=W0150

    async def queryrow(self, sentence: Any):
        """
        queryrow.

            Make a query to DB returning only one row
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            stmt = await self._connection.prepare(sentence)
            self._columns = [a.name for a in stmt.get_attributes()]
            self._result = await stmt.fetchrow()
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Query Row Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Sentence Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Query Row: {err}"
            ) from err
        finally:
            self.generated_at()
            return await self._serializer(self._result, error) # pylint: disable=W0150

    async def execute(self, sentence: Any, *args, **kwargs):
        """execute.

        Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        self._error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._result = await self._connection.execute(sentence, *args)
            return [self._result, None]
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Execute Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Execute Sentence Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Execute: {err}"
            ) from err
        finally:
            return [self._result, self._error] # pylint: disable=W0150

    async def execute_many(self, sentence: Any, *args, timeout=None):
        """execute.

        Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        self._error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            async with self._connection.transaction():
                await self._connection.executemany(sentence, timeout=timeout, *args)
            return [True, None]
        except RuntimeError as err:
            raise ProviderError(
                f"Runtime on Execute Error: {err}"
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Execute Sentence Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Execute: {err}"
            ) from err
        finally:
            return [True, self._error] # pylint: disable=W0150

    executemany = execute_many

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


### Cursor Context
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
            raise Exception(
                f"Error forward Cursor: {err}"
            ) from err

    async def get(self, number=1):
        try:
            return await self._cursor.fetch(number)
        except Exception as err:
            raise Exception(
                f"Error Fetch Cursor: {err}"
            ) from err

    async def getrow(self):
        try:
            return await self._cursor.fetchrow()
        except Exception as err:
            raise Exception(
                f"Error Fetchrow Cursor: {err}"
            ) from err


### Cursor Iterator Context
    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

### Non-Async Methods
    async def test_connection(self, **kwargs):
        result = None
        error = None
        try:
            result = await self.queryrow(self._test_query)
        except Exception as err: # pylint: disable=W0703
            error = err
        return [result, error]

    def _test_connection(self):
        self._error = None
        self._result = None
        self.start(target=self._fetchone, args=(self._test_query,))
        self.join(timeout=self._timeout)
        return [self._result, self._error]

    def perform(self, sentence):
        self.start(target=self._execute, args=(sentence,))
        if self.is_alive():
            self.join(timeout=self._timeout)
            return [self._result, self._error]

    def _execute(self, sentence):
        self._error = None
        self._result = None
        return self._loop.run_until_complete(self.execute(sentence))

    def fetchall(self, sentence):
        self.start(target=self._fetchall, args=(sentence,))
        if self.is_alive():
            self.join(timeout=self._timeout)
            return [self._result, self._error]

    fetch_all = fetchall

    def _fetchall(self, sentence):
        self._error = None
        self._result = None
        try:
            stmt, error = self._loop.run_until_complete(self.prepare(sentence))
            self._error = error
            if stmt:
                result = self._loop.run_until_complete(stmt.fetch())
                self._result = Recordset(
                    result=result, columns=self._columns)
        except RuntimeError as err:
            self._error = f"Fetch Error: {err}"
            raise ProviderError(
                message=self._error
            ) from err
        except (
                PostgresSyntaxError,
                UndefinedColumnError,
                InvalidSQLStatementNameError,
                UndefinedTableError
            ) as err:
            raise StatementError(
                f"Execute Sentence Error: {err}"
            ) from err
        except PostgresError as err:
            raise DriverError(
                f"Error on Fetch: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Error on Execute: {err}"
            ) from err
        finally:
            return [self._result, self._error] # pylint: disable=W0150

    def fetchone(self, sentence):
        self.start(target=self._fetchone, args=(sentence,))
        self.join(timeout=self._timeout)
        return [self._result, self._error]

    fetch_one = fetchone

    def _fetchone(self, sentence):
        self._error = None
        self._result = None
        try:
            row = self._loop.run_until_complete(
                self._connection.fetchrow(sentence))
            if row:
                self._result = row
        except Exception as err:
            self._error = f"Error on Query Row: {err}"
            raise Exception(
                self._error
            ) from err
        finally:
            return [self._result, self._error] # pylint: disable=W0150

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
        FROM pg_attribute a WHERE a.attrelid = '{table!s}'::regclass \
        AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        if not self._connection:
            await self.connection()
        try:
            colinfo = await self._connection.fetch(sql)
            return colinfo
        except Exception as err: # pylint: disable=W0703
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
            columns = ", ".join(["{name} {type}".format(**e) for e in fields]) # pylint: disable=C0209
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
                    f"Error in Object Creation: {err!s}"
                ) from err
        else:
            raise RuntimeError(
                f'Pg: invalid Object type {object!s}'
            )

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    async def use(self, database: str):
        raise NotImplementedError(
            'AsyncPg Error: You cannot change database on realtime.'
        )
