""" pg PostgreSQL Provider.
Notes on pg Provider
--------------------
This provider implements all funcionalities from asyncpg (cursors, transactions, copy from and to files, pools, native data types, etc)
"""

import asyncio
import json
import time
from datetime import datetime
import traceback
import uuid
import asyncpg
from asyncpg.pgproto import pgproto
from dateutil.relativedelta import relativedelta
from typing import Tuple

from asyncpg.exceptions import (
    ConnectionDoesNotExistError,
    FatalPostgresError,
    InterfaceError,
    InterfaceWarning,
    InternalClientError,
    InvalidSQLStatementNameError,
    PostgresError,
    PostgresSyntaxError,
    TooManyConnectionsError,
    UndefinedColumnError,
    UndefinedTableError,
)

from asyncdb.exceptions import (
    _handle_done_tasks,
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)
from asyncdb.utils.encoders import (
    BaseEncoder,
)
from asyncdb.utils import (
    SafeDict,
    _escapeString,
)

from asyncdb.providers.sql import SQLProvider, baseCursor

max_cached_statement_lifetime = 600
max_cacheable_statement_size = 1024 * 15


class pgPool(BasePool):
    _max_queries = 100
    _dsn = "postgres://{user}:{password}@{host}:{port}/{database}"
    _server_settings = {}
    init_func = None
    setup_func = None
    _max_clients = 1500
    _min_size = 5
    application_name = "Navigator"
    _numeric_as_float: bool = False

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        super(pgPool, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
        if "max_clients" in kwargs:
            self._max_clients = kwargs["max_clients"]
        if "min_size" in kwargs:
            self._min_size = kwargs["min_size"]
        if "numeric_as_float" in kwargs:
            self._numeric_as_float = kwargs['numeric_as_float']

    def get_event_loop(self):
        return self._loop

    async def setup_connection(self, connection):
        if self.setup_func:
            try:
                await self.setup_func(connection)
            except Exception as err:
                print("Error on Setup Connection: {}".format(err))
                pass

    async def init_connection(self, connection):
        # Setup jsonb encoder/decoder
        def _encoder(value):
            return json.dumps(value, cls=BaseEncoder)

        def _decoder(value):
            return json.loads(value)

        await connection.set_type_codec(
            "json", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_type_codec(
            "jsonb", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_builtin_type_codec(
            "hstore", codec_name="pg_contrib.hstore"
        )

        def timedelta_decoder(delta: Tuple) -> relativedelta:
            return relativedelta(months=delta[0], days=delta[1], microseconds=delta[2])

        def timedelta_encoder(delta: relativedelta):
            ndelta = delta.normalized()
            return (
                ndelta.years * 12 + ndelta.months,
                ndelta.days,
                (ndelta.hours * 3600 + ndelta.minutes * 60 + ndelta.seconds) * 1_000_000
                + ndelta.microseconds,
            )

        if self._numeric_as_float is True:
            await connection.set_type_codec(
                "interval",
                schema="pg_catalog",
                encoder=timedelta_encoder,
                decoder=timedelta_decoder,
                format="tuple",
            )

        def _uuid_encoder(value):
            if value:
                val = uuid.UUID(value).bytes
            else:
                val = b""
            return val

        await connection.set_type_codec(
            "uuid",
            encoder=_uuid_encoder,
            decoder=lambda u: pgproto.UUID(u),
            schema="pg_catalog",
            format="binary",
        )
        if self.init_func:
            try:
                await self.init_func(connection)
            except Exception as err:
                print("Error on Init Connection: {}".format(err))
                pass

    """
    __init async db initialization
    """

    # Create a database connection pool
    async def connect(self):
        self._logger.debug("AsyncPg (Pool): Connecting to {}".format(self._dsn))
        try:
            # TODO: pass a setup class for set_builtin_type_codec and a setup for add listener
            server_settings = {
                "application_name": self.application_name,
                "idle_in_transaction_session_timeout": "3600",
                "tcp_keepalives_idle": "3600",
                "max_parallel_workers": "24",
            }
            server_settings = {**server_settings, **self._server_settings}
            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                max_queries=self._max_queries,
                min_size=self._min_size,
                max_size=self._max_clients,
                max_inactive_connection_lifetime=3600,
                timeout=10,
                command_timeout=self._timeout,
                init=self.init_connection,
                setup=self.setup_connection,
                loop=self._loop,
                server_settings=server_settings,
            )
        except TooManyConnectionsError as err:
            print("Too Many Connections Error: {}".format(str(err)))
            raise TooManyConnections(str(err))
            return False
        except TimeoutError as err:
            raise ConnectionTimeout(
                "Unable to connect to database: {}".format(str(err))
            )
        except ConnectionRefusedError as err:
            raise ProviderError(
                "Unable to connect to database, connection Refused: {}".format(str(err))
            )
        except ConnectionDoesNotExistError as err:
            raise ProviderError("Connection Error: {}".format(str(err)))
            return False
        except InternalClientError as err:
            raise ProviderError("Internal Error: {}".format(str(err)))
            return False
        except InterfaceError as err:
            raise ProviderError("Interface Error: {}".format(str(err)))
            return False
        except InterfaceWarning as err:
            print("Interface Warning: {}".format(str(err)))
            return False
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()

    """
    Take a connection from the pool.
    """

    async def acquire(self):
        db = None
        self._connection = None
        # Take a connection from the pool.
        try:
            self._connection = await self._pool.acquire()
        except TooManyConnectionsError as err:
            self._logger.error("Too Many Connections Error: {}".format(str(err)))
            return False
        except ConnectionDoesNotExistError as err:
            self._logger.error("Connection Error: {}".format(str(err)))
            return False
        except InternalClientError as err:
            self._logger.error("Internal Error: {}".format(str(err)))
            return False
        except InterfaceError as err:
            self._logger.error("Interface Error: {}".format(str(err)))
            return False
        except InterfaceWarning as err:
            self._logger.error("Interface Warning: {}".format(str(err)))
            return False
        except Exception as err:
            self._logger.error("Unknown Error on Acquire: {}".format(str(err)))
        if self._connection:
            db = pg(pool=self)
            db.set_connection(self._connection)
        return db

    """
    Release a connection from the pool
    """

    async def release(self, connection=None, timeout=5):
        if not connection:
            conn = self._connection
        else:
            conn = connection
        if isinstance(connection, pg):
            conn = connection.engine()
        if not conn:
            return True
        try:
            await self._pool.release(conn, timeout=timeout)
            return True
        except InterfaceError as err:
            raise ProviderError("Release Interface Error: {}".format(str(err)))
        except InternalClientError as err:
            self._logger.debug(
                "Connection already released, PoolConnectionHolder.release() called on a free connection holder"
            )
            # print("PoolConnectionHolder.release() called on a free connection holder")
            return False
        except Exception as err:
            raise ProviderError("Release Error: {}".format(str(err)))

    """
    close
        Close Pool Connection
    """

    async def wait_close(self, gracefully=True, timeout=1):
        if self._pool:
            # try to closing main connection
            try:
                if self._connection:
                    await self._pool.release(self._connection, timeout=timeout)
                    self._connection = None
            except (InternalClientError, InterfaceError) as err:
                raise ProviderError("Release Interface Error: {}".format(str(err)))
            except Exception as err:
                raise ProviderError("Release Error: {}".format(str(err)))
            try:
                if gracefully:
                    await self._pool.expire_connections()
                    await self._pool.close()
                # # until end, close the pool correctly:
                self._pool.terminate()
            except Exception as err:
                error = f"Pool Exception: {err.__class__.__name__}: {err}"
                print("Pool Error: {}".format(error))
                raise ProviderError("Pool Error: {}".format(error))
            finally:
                self._connected = False

    """
    Close Pool
    """

    async def close(self):
        try:
            if self._connection:
                await self._pool.release(self._connection, timeout=1)
                self._connection = None
        except InterfaceError as err:
            raise ProviderError("Release Interface Error: {}".format(str(err)))
        except Exception as err:
            raise ProviderError("Release Error: {}".format(str(err)))
        try:
            await self._pool.expire_connections()
            await self._pool.close()
        except Exception as err:
            print("Pool Closing Error: {}".format(str(err)))
        finally:
            self._pool.terminate()
            self._connected = False

    def terminate(self, gracefully=True):
        self._loop.run_until_complete(
            self.wait_close(gracefully=gracefully, timeout=1)
        )

    """
    Execute a connection into the Pool
    """

    async def execute(self, sentence, *args):
        if self._pool:
            try:
                result = await self._pool.execute(sentence, *args)
                return result
            except InterfaceError as err:
                raise ProviderError("Execute Interface Error: {}".format(str(err)))
            except Exception as err:
                raise ProviderError("Execute Error: {}".format(str(err)))


class pglCursor(baseCursor):
    _connection: asyncpg.Connection = None

    async def __aenter__(self) -> "pglCursor":
        if not self._connection:
            await self.connection()
        self._cursor = await self._connection.cursor(self._sentence, self._params)
        return self


class pg(SQLProvider):
    _provider = "postgresql"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _dsn = "postgres://{user}:{password}@{host}:{port}/{database}"
    _prepared = None
    _cursor = None
    _transaction = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    _server_settings = {}
    application_name: str = "Navigator"
    _numeric_as_float: bool = False

    def __init__(self, dsn="", loop=None, params={}, pool=None, **kwargs):
        super(pg, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        if pool:
            self._pool = pool
            self._loop = self._pool.get_event_loop()
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
        if "numeric_as_float" in kwargs:
            self._numeric_as_float = kwargs['numeric_as_float']

    async def close(self, timeout=5):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                if not self._connection.is_closed():
                    self._logger.debug(
                        "Closing Connection, id: {}".format(
                            self._connection.get_server_pid()
                        )
                    )
                    try:
                        if self._pool:
                            await self._pool.pool().release(self._connection)
                        else:
                            await self._connection.close(timeout=timeout)
                    except InterfaceError as err:
                        raise ProviderError("Close Error: {}".format(str(err)))
                    except Exception as err:
                        await self._connection.terminate()
                        self._connection = None
                        raise ProviderError(
                            "Connection Error, Terminated: {}".format(str(err))
                        )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False

    def terminate(self):
        self._loop.run_until_complete(self.close())

    async def is_in_transaction(self):
        return self._connection.is_in_transaction()

    def is_connected(self):
        try:
            self._connected = not (self._connection.is_closed())
            return self._connected
        except Exception as err:
            self._logger.exception(err)
            return False

    async def connection(self):
        """connection.

        Get an asyncpg connection
        """
        if self._connection is not None:
            if not self._connection.is_closed():
                self._connected = True
                return self

        self._connection = None
        self._connected = False

        # Setup jsonb encoder/decoder
        def _encoder(value):
            return json.dumps(value, cls=BaseEncoder)

        def _decoder(value):
            return json.loads(value)

        server_settings = {
            "application_name": self.application_name,
            "idle_in_transaction_session_timeout": "600",
            "tcp_keepalives_idle": "600",
            "max_parallel_workers": "16",
        }
        server_settings = {**server_settings, **self._server_settings}

        try:
            if self._pool:
                self._connection = await self._pool.pool().acquire()
            else:
                self._connection = await asyncpg.connect(
                    dsn=self._dsn,
                    command_timeout=self._timeout,
                    timeout=self._timeout,
                    max_cached_statement_lifetime=max_cached_statement_lifetime,
                    max_cacheable_statement_size=max_cacheable_statement_size,
                    server_settings=server_settings,
                )
                await self._connection.set_type_codec(
                    "json", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
                )
                await self._connection.set_type_codec(
                    "jsonb", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
                )
                await self._connection.set_builtin_type_codec(
                    "hstore", codec_name="pg_contrib.hstore"
                )
                # await connection.set_type_codec(
                #     "interval",
                #     schema="pg_catalog",
                #     encoder=timedelta_encoder,
                #     decoder=timedelta_decoder,
                #     format="tuple",
                # )
                #
                # if self._numeric_as_float is True:
                #     await connection.set_type_codec(
                #         "interval",
                #         schema="pg_catalog",
                #         encoder=timedelta_encoder,
                #         decoder=timedelta_decoder,
                #         format="tuple",
                #     )
                #
                # def timedelta_decoder(delta: Tuple) -> relativedelta:
                #     return relativedelta(
                #         months=delta[0], days=delta[1], microseconds=delta[2]
                #     )
                #
                # def timedelta_encoder(delta: relativedelta):
                #     ndelta = delta.normalized()
                #     return (
                #         ndelta.years * 12 + ndelta.months,
                #         ndelta.days,
                #         (ndelta.hours * 3600 + ndelta.minutes * 60 + ndelta.seconds)
                #         * 1_000_000
                #         + ndelta.microseconds,
                #     )

                def _uuid_encoder(value):
                    if value:
                        val = uuid.UUID(value).bytes
                    else:
                        val = b""
                    return val

                await connection.set_type_codec(
                    "uuid",
                    encoder=_uuid_encoder,
                    decoder=lambda u: pgproto.UUID(u),
                    schema="pg_catalog",
                    format="binary",
                )
            if self._connection:
                self._connected = True
                if self.init_func is not None:
                    try:
                        await self.init_func(self._connection)
                    except Exception as err:
                        print("Error on Init Connection: {}".format(err))
                        pass
                self._initialized_on = time.time()
                self._logger.debug(f'Initialized on: {self._initialized_on}')
        except TooManyConnectionsError as err:
            raise TooManyConnections("Too Many Connections Error: {}".format(str(err)))
        except ConnectionDoesNotExistError as err:
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
        finally:
            return self

    """
    Release a Connection
    """

    async def release(self):
        try:
            if not await self._connection.is_closed():
                if self._pool:
                    release = asyncio.create_task(
                        self._pool.release(self._connection, timeout=10)
                    )
                    asyncio.ensure_future(release, loop=self._loop)
                    return await release
                else:
                    await self._connection.close(timeout=5)
        except (InterfaceError, RuntimeError) as err:
            raise ProviderError("Release Interface Error: {}".format(str(err)))
            return False
        finally:
            self._connected = False
            self._connection = None

    def prepared_statement(self):
        return self._prepared

    @property
    def connected(self):
        if self._pool:
            return self._pool._connected
        elif self._connection:
            return not self._connection.is_closed()

    """
    Preparing a sentence
    """

    async def prepare(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")

        try:
            if not self._connection:
                await self.connection()
            try:
                stmt = await asyncio.shield(self._connection.prepare(sentence))
                try:
                    self._attributes = stmt.get_attributes()
                    self._columns = [a.name for a in self._attributes]
                    # self._columns = [a.name for a in stmt.get_attributes()]
                    self._prepared = stmt
                    self._parameters = stmt.get_parameters()
                except TypeError:
                    self._columns = []
            except FatalPostgresError as err:
                error = "Fatal Runtime Error: {}".format(str(err))
                raise StatementError(error)
            except PostgresSyntaxError as err:
                error = "Sentence Syntax Error: {}".format(str(err))
                raise StatementError(error)
            except PostgresError as err:
                error = "PostgreSQL Error: {}".format(str(err))
                raise StatementError(error)
            except RuntimeError as err:
                error = "Prepare Runtime Error: {}".format(str(err))
                raise StatementError(error)
            except Exception as err:
                error = "Unknown Error: {}".format(str(err))
                raise ProviderError(error)
        finally:
            return [self._prepared, error]

    async def query(self, sentence=""):
        error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._result = await self._connection.fetch(sentence)
            if not self._result:
                return [None, "Data was not found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Sentence Error: {}".format(str(err))
            raise StatementError(error)
        except (
            asyncpg.exceptions.InvalidSQLStatementNameError,
            asyncpg.exceptions.UndefinedTableError,
        ) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            startTime = 0
            return [self._result, error]

    async def queryrow(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            stmt = await self._connection.prepare(sentence)
            self._attributes = stmt.get_attributes()
            self._columns = [a.name for a in self._attributes]
            self._result = await stmt.fetchrow()
        except RuntimeError as err:
            error = "Query Row Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except (PostgresSyntaxError, UndefinedColumnError, PostgresError) as err:
            error = "Statement Error: {}".format(str(err))
            raise StatementError(error)
        except (
            asyncpg.exceptions.InvalidSQLStatementNameError,
            asyncpg.exceptions.UndefinedTableError,
        ) as err:
            error = "Invalid Statement Error: {}".format(str(err))
            self._loop.call_exception_handler(err)
            raise StatementError(error)
        except Exception as err:
            error = "Query Row Error: {}".format(str(err))
            self._loop.call_exception_handler(err)
            raise Exception(error)
        # finally:
        # await self.close()
        return [self._result, error]

    async def execute(self, sentence=""):
        """Execute a transaction
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
            result = await self._connection.execute(sentence)
            return [result, None]
        except InterfaceWarning as err:
            error = "Interface Warning: {}".format(str(err))
            raise ProviderError(error)
        except asyncpg.exceptions.DuplicateTableError as err:
            error = err
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            # self._loop.call_exception_handler(err)
            raise ProviderError(error)
        finally:
            return [result, error]

    async def executemany(self, sentence="", *args):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            await self._connection.executemany(sentence, *args)
        except InterfaceWarning as err:
            error = "Interface Warning: {}".format(str(err))
            raise ProviderError(error)
            return False
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            # self._loop.call_exception_handler(err)
            raise Exception(error)
        finally:
            return error

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

    async def fetch(self, number=1):
        try:
            return await self._cursor.fetch(number)
        except Exception as err:
            error = "Error Fetch Cursor: {}".format(str(err))
            raise Exception(error)

    async def fetchrow(self):
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
    COPY Functions
    type: [ text, csv, binary ]
    """

    async def copy_from_table(
        self, table="", schema="public", output=None, type="csv", columns=None
    ):
        """table_copy
        get a copy of table data into a file, file-like object or a coroutine passed on "output"
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        try:
            result = await self._connection.copy_from_table(
                table_name=table,
                schema_name=schema,
                columns=columns,
                format=type,
                output=output,
            )
            print(result)
            return result
        except (asyncpg.exceptions.UndefinedTableError):
            error = "Error on Copy, Table doesnt exists: {}".format(str(table))
            raise StatementError(error)
        except (
            asyncpg.exceptions.InvalidSQLStatementNameError,
            asyncpg.exceptions.UndefinedTableError,
        ) as err:
            error = "Error on Copy, Invalid Statement Error: {}".format(str(err))
            self._loop.call_exception_handler(err)
            raise StatementError(error)
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

    async def copy_to_table(
        self, table="", schema="public", source=None, type="csv", columns=None
    ):
        """copy_to_table
        get data from a file, file-like object or a coroutine passed on "source" and copy into table
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        if self._transaction:
            # a transaction exists:
            await self._transaction.commit()
        try:
            result = await self._connection.copy_to_table(
                table_name=table,
                schema_name=schema,
                columns=columns,
                format=type,
                source=source,
            )
            print(result)
            return result
        except (asyncpg.exceptions.UndefinedTableError):
            error = "Error on Copy, Table doesnt exists: {}".format(str(table))
            raise StatementError(error)
        except (
            asyncpg.exceptions.InvalidSQLStatementNameError,
            asyncpg.exceptions.UndefinedTableError,
        ) as err:
            error = "Error on Copy, Invalid Statement Error: {}".format(str(err))
            self._loop.call_exception_handler(err)
            raise StatementError(error)
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

    async def copy_into_table(
        self, table="", schema="public", source=None, columns=None
    ):
        """copy_into_table
        get data from records (any iterable object) and save into table
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        if self._transaction:
            # a transaction exists:
            await self._transaction.commit()
        try:
            result = await self._connection.copy_records_to_table(
                table_name=table, schema_name=schema, columns=columns, records=source
            )
            return result
        except (asyncpg.exceptions.UndefinedTableError) as err:
            error = "Error on Copy: {}, Table doesnt exists: {}".format(
                str(err), str(table)
            )
            raise StatementError(error)
            return False
        except (InvalidSQLStatementNameError, UndefinedColumnError) as err:
            error = "Error on Copy, Invalid Statement Error: {}".format(str(err))
            raise StatementError(error)
        except (asyncpg.exceptions.UniqueViolationError) as err:
            error = "Error on Copy, Constraint Violated: {}".format(str(err))
            raise DataError(error)
        except (asyncpg.exceptions.InterfaceError) as err:
            error = "Error on Copy into Table Function: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

    """
    Model Logic:
    """

    async def column_info(self, tablename):
        """Column Info.

        Get Meta information about a table (column name, data type and PK).
        Useful to build a DataModel from Querying database.
        Parameters:
        @tablename: str The name of the table (including schema).
        """
        sql = f"SELECT a.attname AS column_name, a.atttypid::regtype AS data_type, \
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
Registering this Provider
"""
registerProvider(pg)
