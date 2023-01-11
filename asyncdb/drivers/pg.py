""" pg PostgreSQL Provider.
Notes on pg Provider
--------------------
This provider implements basic funcionalities from asyncpg
(cursors, transactions, copy from and to files, pools, native data types, etc).
"""
import asyncio
from multiprocessing import Value
import os
import ssl
import time
import uuid
from collections.abc import Callable, Iterable
from typing import Any, Optional, Union

import asyncpg
import uvloop
from asyncpg.exceptions import (
    ConnectionDoesNotExistError,
    DuplicateTableError,
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
    UniqueViolationError,
)
from asyncpg.pgproto import pgproto

from asyncdb.exceptions import (
    ConnectionTimeout,
    DriverError,
    EmptyStatement,
    ProviderError,
    StatementError,
    TooManyConnections,
    UninitializedError,
)
from asyncdb.interfaces import DBCursorBackend, ModelBackend
from asyncdb.models import Model
from asyncdb.utils.encoders import DefaultEncoder
from asyncdb.utils.types import Entity

from .abstract import BasePool
from .sql import SQLCursor, SQLDriver

max_cached_statement_lifetime = 600
max_cacheable_statement_size = 1024 * 15

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class NAVConnection(asyncpg.Connection):
    def _get_reset_query(self):
        return None


class pgPool(BasePool):
    _setup_func: Optional[Callable] = None
    _init_func: Optional[Callable] = None

    def __init__(self, dsn: str = None, loop: asyncio.AbstractEventLoop = None, params: Optional[dict] = None, **kwargs):
        self._test_query = 'SELECT 1'
        self.application_name = os.getenv('APP_NAME', "NAV")
        self._max_clients = 300
        self._min_size = 10
        self._server_settings = {}
        self._dsn = "postgres://{user}:{password}@{host}:{port}/{database}"
        super(pgPool, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs
        )
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
        # set the JSON encoder:
        self._encoder = DefaultEncoder()
        ### SSL Support:
        self.ssl: bool = False
        if params and 'ssl' in params:
            ssloptions = params['ssl']
        elif 'ssl' in kwargs:
            ssloptions = kwargs['ssl']
        else:
            ssloptions = None
        if ssloptions:
            self.ssl: bool = True
            try:
                check_hostname = ssloptions['check_hostname']
            except KeyError:
                check_hostname = False
            ### certificate Support:
            try:
                ca_file = ssloptions['cafile']
            except KeyError:
                ca_file = None
            args = {
                "cafile": ca_file
            }
            self.sslctx = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                **args
            )
            # Certificate Chain:
            try:
                certs = {
                    "certfile": ssloptions['certfile'],
                    "keyfile": ssloptions['keyfile']
                }
            except KeyError:
                certs = {
                    "certfile": None,
                    "keyfile": None
                }
            if certs['certfile']:
                self.sslctx.load_cert_chain(
                    **certs
                )
            self.sslctx.check_hostname = check_hostname

    async def setup_connection(self, connection):
        if self._setup_func:
            try:
                await self.setup_func(connection)
            except (ValueError, RuntimeError) as err:
                self._logger.error(
                    f"Error on Setup Function: {err}"
                )

    async def test_connection(self, *args):
        """Test Connnection.
        Making a connection Test using the basic Query Method.
        """
        result = None
        error = None
        if self._test_query is None:
            return [None, NotImplementedError()]
        try:
            result = await self.execute(self._test_query, *args)
        except ProviderError as err:
            error = err
        finally:
            return [result, error]  # pylint: disable=W0150

    async def init_connection(self, connection):
        # Setup jsonb encoder/decoder
        def _encoder(value):
            # return json.dumps(value, cls=BaseEncoder)
            return self._encoder.dumps(value)  # pylint: disable=E1120

        def _decoder(value):
            return self._encoder.loads(value)  # pylint: disable=E1120

        await connection.set_type_codec(
            "json", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_type_codec(
            "jsonb", encoder=_encoder, decoder=_decoder, schema="pg_catalog"
        )
        await connection.set_builtin_type_codec(
            "hstore", codec_name="pg_contrib.hstore"
        )

        def _uuid_encoder(value):
            if isinstance(value, uuid.UUID):
                val = value.bytes
            elif value is not None:
                val = uuid.UUID(value).bytes
            else:
                val = b""
            return val

        await connection.set_type_codec(
            "uuid",
            encoder=_uuid_encoder,
            decoder=lambda u: pgproto.UUID(u),  # pylint: disable=I1101,W0108
            schema="pg_catalog",
            format="binary",
        )
        if self._init_func is not None and callable(self._init_func):
            try:
                await self._init_func(connection)  # pylint: disable=E1102
            except (ValueError, RuntimeError) as err:
                self._logger.warning(
                    f"Error on Init Connection: {err}"
                )

## init async db initialization

    # Create a database connection pool
    async def connect(self):
        """
        Creates a Pool Connection.
        """
        self._logger.debug(
            f"AsyncPg (Pool): Connecting to {self._dsn}"
        )
        try:
            # TODO: pass a setup class for set_builtin_type_codec and a setup for add listener
            server_settings = {
                "application_name": self.application_name,
                "idle_in_transaction_session_timeout": "3600",
                "tcp_keepalives_idle": "3600",
                "max_parallel_workers": "48",
                "jit": "off"
            }
            server_settings = {**server_settings, **self._server_settings}
            if self.ssl:
                _ssl = {
                    "ssl": self.sslctx
                }
            else:
                _ssl = {}
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
                connection_class=NAVConnection,
                **_ssl
            )
            # is connected
            if self._pool:
                self._connected = True
                self._initialized_on = time.time()
            return self
        except ConnectionRefusedError as err:
            raise UninitializedError(
                f"Unable to connect to database, connection Refused: {err}"
            ) from err
        except ConnectionError as ex:
            self._logger.error(
                f"Connection Error: {ex}"
            )
            raise UninitializedError(
                f"Connection Error: {ex}"
            ) from ex
        except TooManyConnectionsError as err:
            self._logger.error(
                f"Too Many Connections Error: {err}"
            )
            raise UninitializedError(
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

    async def acquire(self):
        """
        Takes a connection from the pool.
        """
        db = None
        self._connection = None
        # Take a connection from the pool.
        try:
            self._connection = await self._pool.acquire()
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
        except ConnectionRefusedError as err:
            raise UninitializedError(
                f"Unable to connect to database, connection Refused: {err}"
            ) from err
        except ConnectionDoesNotExistError as err:
            raise ProviderError(
                f"Connection Error: {err}"
            ) from err
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
        except Exception as err:  # pylint: disable=W0703
            self._logger.error(
                f"Unknown Error on Acquire: {err}"
            )
        if self._connection:
            db = pg(pool=self)
            db.set_connection(self._connection)
        return db

    async def release(self, connection=None, timeout=5):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        else:
            conn = connection
        if isinstance(conn, pg):
            conn = connection.engine()
        if not conn:
            return True
        try:
            await self._pool.release(conn, timeout=timeout)
            return True
        except InterfaceError as err:
            raise ProviderError(
                message=f"Release Interface Error: {err}"
            ) from err
        except InternalClientError as err:
            self._logger.debug(
                f"Connection already released, \
                called on a free connection holder: {err}"
            )
            return False
        except Exception as err:
            raise ProviderError(
                message=f"Release Error: {err}"
            ) from err

    async def wait_close(self, gracefully=True, timeout=5):
        """
        close
            Close Pool Connection
        """
        if self._pool:
            # try to closing main connection
            try:
                if self._connection:
                    await self._pool.release(self._connection, timeout=timeout)
                    self._connection = None
            except (InternalClientError, InterfaceError) as err:
                raise ProviderError(
                    f"Release Interface Error: {err}"
                ) from err
            except Exception as err:
                raise ProviderError(
                    f"Release Error: {err}"
                ) from err
            try:
                if gracefully:
                    await asyncio.wait_for(
                        self._pool.expire_connections(),
                        timeout=timeout
                    )
                    await asyncio.wait_for(
                        self._pool.close(),
                        timeout=timeout
                    )
                # # until end, close the pool correctly:
                self._pool.terminate()
            except Exception as err:
                error = f"Pool Exception: {err.__class__.__name__}: {err}"
                print(f"Pool Error: {error}")
                raise ProviderError(
                    f"Pool Error: {error}"
                ) from err
            finally:
                self._connected = False

    async def close(self, **kwargs):
        """
        Close Pool
        """
        try:
            if self._connection:
                await self._pool.release(self._connection, timeout=1)
                self._connection = None
        except InterfaceError as err:
            raise ProviderError(
                f"Release Interface Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Release Error: {err}"
            ) from err
        try:
            await self._pool.expire_connections()
            await self._pool.close()
        except Exception as err:
            error = f"Pool Closing Error: {err.__class__.__name__}: {err}"
            raise Exception(error) from err
        finally:
            self._pool.terminate()
            self._connected = False

    disconnect = close

    async def execute(self, sentence, *args):
        """
        Execute a connection into the Pool
        """
        try:
            return await self._pool.execute(sentence, *args)
        except InterfaceError as err:
            raise ProviderError(
                f"Execute Interface Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Execute Error: {err}"
            ) from err


class pgCursor(SQLCursor):
    _connection: asyncpg.Connection = None


class pg(SQLDriver, DBCursorBackend, ModelBackend):
    _provider = "pg"
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
        self._prepared = None
        self._cursor = None
        self._transaction = None
        self._server_settings = {}
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        if "pool" in kwargs:
            self._pool = kwargs['pool']
            self._loop = self._pool.get_loop()
        if "server_settings" in kwargs:
            self._server_settings = kwargs["server_settings"]
        if "application_name" in self._server_settings:
            self.application_name = self._server_settings["application_name"]
        if "numeric_as_float" in kwargs:
            self._numeric_as_float = kwargs['numeric_as_float']
        else:
            self._numeric_as_float = False
        # set the JSON encoder:
        self._encoder = DefaultEncoder()
        ### SSL Support:
        self.ssl: bool = False
        if params and 'ssl' in params:
            ssloptions = params['ssl']
        elif 'ssl' in kwargs:
            ssloptions = kwargs['ssl']
        else:
            ssloptions = None
        if ssloptions:
            self.ssl: bool = True
            try:
                check_hostname = ssloptions['check_hostname']
            except KeyError:
                check_hostname = False
            ### certificate Support:
            try:
                ca_file = ssloptions['cafile']
            except KeyError:
                ca_file = None
            args = {
                "cafile": ca_file
            }
            self.sslctx = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                **args
            )
            # Certificate Chain:
            try:
                certs = {
                    "certfile": ssloptions['certfile'],
                    "keyfile": ssloptions['keyfile']
                }
            except KeyError:
                certs = {
                    "certfile": None,
                    "keyfile": None
                }
            if certs['certfile']:
                self.sslctx.load_cert_chain(
                    **certs
                )
            self.sslctx.check_hostname = check_hostname

    async def close(self, timeout=5):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                if not self._connection.is_closed():
                    self._logger.debug(
                        f"Closing Connection, id: {self._connection.get_server_pid()}"
                    )
                    try:
                        if self._pool:
                            await self._pool.pool().release(self._connection)
                        else:
                            await self._connection.close(timeout=timeout)
                    except InterfaceError as err:
                        raise ProviderError(
                            f"AsyncPg: Closing Error: {err}"
                        ) from err
                    except Exception as err:
                        await self._connection.terminate()
                        self._connection = None
                        raise ProviderError(
                            f"Connection Error, Terminated: {err}"
                        ) from err
        except Exception as err:
            raise ProviderError(
                f"Close Error: {err}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    disconnect = close

    def terminate(self):
        self._loop.run_until_complete(self.close())

    async def is_in_transaction(self):
        return self._connection.is_in_transaction()

    def is_connected(self):
        try:
            if self._connection:
                return not (self._connection.is_closed())
        except (AttributeError, InterfaceError):
            pass
        return self._connected

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
            return self._encoder.dumps(value)  # pylint: disable=E1120

        def _decoder(value):
            return self._encoder.loads(value)  # pylint: disable=E1120

        server_settings = {
            "application_name": self.application_name,
            "idle_in_transaction_session_timeout": "3600",
            "tcp_keepalives_idle": "3600",
            "max_parallel_workers": "24",
            "jit": "off"
        }
        server_settings = {**server_settings, **self._server_settings}
        if self.ssl:
            _ssl = {
                "ssl": self.sslctx
            }
        else:
            _ssl = {}
        try:
            if self._pool and not self._connection:
                self._connection = await self._pool.pool().acquire()
            else:
                self._connection = await asyncpg.connect(
                    dsn=self._dsn,
                    command_timeout=self._timeout,
                    timeout=self._timeout,
                    max_cached_statement_lifetime=max_cached_statement_lifetime,
                    max_cacheable_statement_size=max_cacheable_statement_size,
                    server_settings=server_settings,
                    connection_class=NAVConnection,
                    loop=self._loop,
                    **_ssl
                )
                await self._connection.set_type_codec(
                    "json",
                    encoder=_encoder,
                    decoder=_decoder,
                    schema="pg_catalog"
                )
                await self._connection.set_type_codec(
                    "jsonb",
                    encoder=_encoder,
                    decoder=_decoder,
                    schema="pg_catalog"
                )
                await self._connection.set_builtin_type_codec(
                    "hstore", codec_name="pg_contrib.hstore"
                )

                def _uuid_encoder(value):
                    if isinstance(value, uuid.UUID):
                        val = value.bytes
                    elif value is not None:
                        val = uuid.UUID(bytes=value)
                    else:
                        val = b""
                    return val

                def _uuid_decoder(value):
                    if value is None:
                        return b""
                    else:
                        return uuid.UUID(bytes=value)

                await self._connection.set_type_codec(
                    "uuid",
                    encoder=_uuid_encoder,
                    decoder=_uuid_decoder,
                    schema="pg_catalog",
                    format="binary",
                )
            if self._connection:
                self._connected = True
                if self._init_func is not None and callable(self._init_func):
                    try:
                        await self._init_func(self._connection)  # pylint: disable=E1102
                    except (ValueError, RuntimeError) as err:
                        self._logger.warning(
                            f"Error on Init Connection: {err}"
                        )
                self._initialized_on = time.time()
                self._logger.debug(f'Initialized on: {self._initialized_on}')
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

    async def release(self):
        try:
            if not await self._connection.is_closed():
                if self._pool:
                    if isinstance(self._connection, pg):
                        connection = self._connection.engine()
                    else:
                        connection = self._connection
                    release = asyncio.create_task(
                        self._pool.release(connection, timeout=10)
                    )
                    asyncio.ensure_future(release, loop=self._loop)
                    return await release
                else:
                    await self._connection.close(timeout=5)
        except (InterfaceError, RuntimeError) as err:
            raise ProviderError(
                f"Release Interface Error: {err}"
            ) from err
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
        else:
            return False

    async def prepare(self, sentence: str):
        error = None
        await self.valid_operation(sentence)
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
            error = f"Fatal Runtime Error: {err}"
        except (InvalidSQLStatementNameError, PostgresSyntaxError) as err:
            error = f"Sentence Syntax Error: {err}"
        except PostgresError as err:
            error = f"PostgreSQL Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Prepare Unknown Error: {err}"
        finally:
            return [self._prepared, error]  # pylint: disable=W0150

    async def query(self, sentence: Union[str, Any], *args, **kwargs):
        self._result = None
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            self._result = await self._connection.fetch(sentence, *args, **kwargs)
            if not self._result:
                return [None, "Data was not found"]
        except RuntimeError as err:
            error = f"Query Error: {err}"
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            error = f"Sentence Error: {err}"
        except PostgresError as err:
            error = f"Postgres Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            self.generated_at()
            if error:
                return [None, error]
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def queryrow(self, sentence: str, *args):
        self._result = None
        error = None
        started = self.start_timing()
        await self.valid_operation(sentence)
        try:
            stmt = await self._connection.prepare(sentence)
            self._attributes = stmt.get_attributes()
            self._columns = [a.name for a in self._attributes]
            self._result = await stmt.fetchrow(*args)
            if not self._result:
                return [None, "Data was not found"]
        except RuntimeError as err:
            error = f"Query Error: {err}"
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            error = f"Sentence Error: {err}"
        except PostgresError as err:
            error = f"Postgres Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query Row: {err}"
        finally:
            self.generated_at(started)
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        self.start_timing()
        await self.valid_operation(sentence)
        try:
            self._result = await self._connection.execute(sentence, *args, **kwargs)
        except (
            InvalidSQLStatementNameError,
            PostgresSyntaxError,
            UndefinedColumnError,
            UndefinedTableError
        ) as err:
            error = f"Sentence Error: {err}"
        except DuplicateTableError as err:
            error = f"Duplicated table: {err}"
        except PostgresError as err:
            error = f"Postgres Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def execute_many(self, sentence: str, *args):
        error = None
        self.start_timing()
        await self.valid_operation(sentence)
        try:
            self._result = await self._connection.executemany(sentence, *args)
        except InterfaceWarning as err:
            error = f"Interface Warning: {err}"
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            error = f"Sentence Error: {err}"
        except DuplicateTableError as err:
            error = f"Duplicated table: {err}"
        except PostgresError as err:
            error = f"Postgres Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    executemany = execute_many

    async def fetch_all(self, sentence: str, *args, **kwargs):
        result = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            stmt = await self._connection.prepare(sentence)
            self._attributes = stmt.get_attributes()
            self._columns = [a.name for a in self._attributes]
            result = await stmt.fetch(*args)
            if not result:
                return None
            return result
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            raise StatementError(f"Statement Error: {err}") from err
        except (RuntimeError, PostgresError) as err:
            raise ProviderError(
                f"Postgres Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Fetch: {err}"
            ) from err
        finally:
            self.generated_at()

    async def fetch_one(self, sentence: str, *args, **kwargs):
        result = None
        self.start_timing()
        await self.valid_operation(sentence)
        try:
            result = await self._connection.fetchrow(sentence, *args, **kwargs)
            return result
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            raise StatementError(f"Statement Error: {err}") from err
        except (RuntimeError, PostgresError) as err:
            raise ProviderError(
                f"Postgres Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Fetch: {err}"
            ) from err
        finally:
            self.generated_at()

    async def fetchval(self, sentence: str, *args, column: int = 0, **kwargs):
        result = None
        await self.valid_operation(sentence)
        self.start_timing()
        try:
            result = await self._connection.fetchval(sentence, column=column, *args, **kwargs)
            return result
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError,
                UndefinedTableError
        ) as err:
            raise StatementError(f"Statement Error: {err}") from err
        except (RuntimeError, PostgresError) as err:
            raise ProviderError(
                f"Postgres Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Fetch: {err}"
            ) from err
        finally:
            self.generated_at()

## Transaction Context
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

    async def cursor(self, sentence: Union[str, any], params: Iterable[Any] = None, **kwargs):  # pylint: disable=W0236
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
            error = f"Error forward Cursor: {err}"
            raise Exception(error) from err

    async def fetch(self, number=1):
        try:
            return await self._cursor.fetch(number)
        except Exception as err:
            error = f"Error Fetch Cursor: {err}"
            raise Exception(error) from err

    async def fetchrow(self):
        try:
            return await self._cursor.fetchrow()
        except Exception as err:
            error = f"Error Fetchrow Cursor: {err}"
            raise Exception(error) from err

## Cursor Iterator Context
    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

## COPY Functions
## type: [ text, csv, binary ]
    async def copy_from_table(
        self, table="", schema="public", output=None, file_type="csv", columns=None
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
                format=file_type,
                output=output,
            )
            return result
        except UndefinedTableError as ex:
            raise StatementError(
                f"Error on Copy, Table {table }doesn't exists: {ex}"
            ) from ex
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError
        ) as ex:
            raise StatementError(
                f"Error on Copy, Invalid Statement Error: {ex}"
            ) from ex
        except Exception as ex:
            raise ProviderError(
                f"Error on Table Copy: {ex}"
            ) from ex

    async def copy_to_table(
        self, table="", schema="public", source=None, file_type="csv", columns=None
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
                format=file_type,
                source=source,
            )
            return result
        except UndefinedTableError as ex:
            raise StatementError(
                f"Error on Copy to Table {table }doesn't exists: {ex}"
            ) from ex
        except (
                InvalidSQLStatementNameError,
                PostgresSyntaxError,
                UndefinedColumnError
        ) as ex:
            raise StatementError(
                f"Error on Copy, Invalid Statement Error: {ex}"
            ) from ex
        except Exception as ex:
            raise Exception(
                f"Error on Copy to Table {ex}"
            ) from ex

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
        except UndefinedTableError as ex:
            raise StatementError(
                f"Error on Copy to Table {table }doesn't exists: {ex}"
            ) from ex
        except (
            InvalidSQLStatementNameError,
            PostgresSyntaxError,
            UndefinedColumnError
        ) as ex:
            raise StatementError(
                f"Error on Copy, Invalid Statement Error: {ex}"
            ) from ex
        except UniqueViolationError as ex:
            raise StatementError(
                f"Error on Copy, Constraint Violated: {ex}"
            ) from ex
        except InterfaceError as ex:
            raise ProviderError(
                f"Error on Copy into Table Function: {ex}"
            ) from ex
        except (RuntimeError, PostgresError) as ex:
            raise ProviderError(
                f"Postgres Error on Copy into Table: {ex}"
            ) from ex
        except Exception as ex:
            raise Exception(
                f"Error on Copy into Table: {ex}"
            ) from ex

## Model Logic:
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
        except Exception as err:  # pylint: disable=W0703
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
            columns = ", ".join(["{name} {type}".format(**e) for e in fields])  # pylint: disable=C0209
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
            raise RuntimeError(f'SQLite: invalid Object type {object!s}')

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    async def use(self, database: str):
        raise NotImplementedError(
            'AsyncPg Error: There is no Database in SQLite'
        )  # pragma: no cover

    async def _insert_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        insert a row from model.
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        cols = []
        columns = []
        source = []
        _filter = {}
        n = 1
        fields = _model.columns()
        for name, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            columns.append(column)
            # validating required field
            try:
                required = field.required()
            except AttributeError:
                required = False
            pk = self._get_attribute(field, value, attr='primary_key')
            if pk is True and value is None:
                if 'db_default' in field.metadata:
                    continue
            if required is False and value is None or value == "None":
                if 'db_default' in field.metadata:
                    continue
                else:
                    # get default value
                    default = field.default
                    if callable(default):
                        value = default()
                    else:
                        continue
            elif required is True and value is None or value == "None":
                if 'db_default' in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ValueError(
                        f"Field {name} is required and value is null over {_model.Meta.name}"
                    )
            source.append(value)
            cols.append(column)
            n += 1
            if pk := self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            cols = ",".join(cols)
            values = ",".join(["${}".format(a) for a in range(1, n)])  # pylint: disable=C0209
            columns = ','.join(columns)
            primary = f"RETURNING {columns}"
            insert = f"INSERT INTO {table}({cols}) VALUES({values}) {primary}"
            self._logger.debug(f"INSERT: {insert}")
            stmt = await self._connection.prepare(insert)
            result = await stmt.fetchrow(*source, timeout=2)
            self._logger.debug(stmt.get_statusmsg())
            if result:
                for f, val in result.items():
                    setattr(_model, f, val)
                return _model
        except UniqueViolationError as err:
            raise StatementError(
                message=f"Constraint Error: {err!r}",
            ) from err
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _delete_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        delete a row from model.
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        source = []
        _filter = {}
        n = 1
        fields = _model.columns()
        for _, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            source.append(
                value
            )
            n += 1
            if pk := self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            condition = self._where(fields, **_filter)
            _delete = f"DELETE FROM {table} {condition};"
            self._logger.debug(f'DELETE: {_delete}')
            result = await self._connection.execute(_delete)
            return f'DELETE {result}: {_filter!s}'
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _update_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        Updating a row in a Model.
        TODO: How to update when if primary key changed.
        Alternatives: Saving *dirty* status and previous value on dict
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        cols = []
        source = []
        _filter = {}
        n = 1
        fields = _model.columns()
        for name, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            # validating required field
            try:
                required = field.required()
            except AttributeError:
                required = False
            if required is False and value is None or value == "None":
                default = field.default
                if callable(default):
                    value = default()
                else:
                    continue
            elif required is True and value is None or value == "None":
                if 'db_default' in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ValueError(
                        f"Field {name} is required and value is null over {_model.Meta.name}"
                    )
            cols.append("{} = {}".format(name, "${}".format(n)))  # pylint: disable=C0209
            source.append(value)
            n += 1
            if pk := self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f'UPDATE: {_update}')

            stmt = await self._connection.prepare(_update)
            result = await stmt.fetchrow(*source, timeout=2)
            self._logger.debug(stmt.get_statusmsg())

            condition = self._where(fields, **_filter)
            get = f"SELECT * FROM {table} {condition}"

            result = await self._connection.fetchrow(get)
            if result:
                for f, val in result.items():
                    setattr(_model, f, val)
                return _model
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _save_(self, model: Model, *args, **kwargs):
        """
        Save a row in a Model, using Insert-or-Update methodology.
        """

    async def _fetch_(self, _model: Model, *args, **kwargs):
        """
        Returns one Row using Model.
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns()
        _filter = {}
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT * FROM {table} {condition}"
        try:
            result = await self._connection.fetchrow(_get)
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model Fetch over {table}: {e}"
            ) from e

    async def _filter_(self, _model: Model, *args, **kwargs):
        """
        Filter a Model using Fields.
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ','.join(args)
        else:
            columns = '*'
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            result = await self._connection.fetch(_get)
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model GET over {table}: {e}"
            ) from e

    async def _select_(self, *args, **kwargs):
        """
        Get a query from Model.
        """
        try:
            model = kwargs['_model']
        except KeyError as e:
            raise ProviderError(
                f'Missing Model for SELECT {kwargs!s}'
            ) from e
        try:
            schema = ''
            sc = model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        if args:
            condition = '{}'.join(args)
        else:
            condition = None
        if 'fields' in kwargs:
            columns = ','.join(kwargs['fields'])
        else:
            columns = '*'
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            result = await self._connection.fetch(_get)
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model SELECT over {table}: {e}"
            ) from e

    async def _get_(self, _model: Model, *args, **kwargs):
        """
        Get one row from model.
        """
        try:
            schema = ''
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ','.join(args)
        else:
            columns = ','.join(fields)  # getting only selected fields
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            result = await self._connection.fetchrow(_get)
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model GET over {table}: {e}"
            ) from e

    async def _all_(self, _model: Model, *args, **kwargs):  # pylint: disable=W0613
        """
        Get all rows on a Model.
        """
        try:
            schema = ''
            # sc = _model.Meta.schema
            if (sc := _model.Meta.schema):
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        if 'fields' in kwargs:
            columns = ','.join(kwargs['fields'])
        else:
            columns = '*'
        _all = f"SELECT {columns} FROM {table}"
        try:
            result = await self._connection.fetch(_all)
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model All over {table}: {e}"
            ) from e

    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """
        try:
            schema = ''
            if (sc := _model.Meta.schema):
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns(_model)
        _filter = {}
        for name, field in fields.items():
            datatype = field.type
            if name in kwargs:
                val = kwargs[name]
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _delete = f"DELETE FROM {table} {condition}"
        try:
            self._logger.debug(f'DELETE: {_delete}')
            result = await self._connection.execute(_delete)
            return f'DELETE {result}: {_filter!s}'
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _updating_(self, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
        """
        try:
            model = kwargs['_model']
        except KeyError as e:
            raise ProviderError(
                f'Missing Model for SELECT {kwargs!s}'
            ) from e
        try:
            schema = ''
            sc = model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        fields = model.columns(model)
        if _filter is None:
            if args:
                _filter = args[0]
        cols = []
        source = []
        new_cond = {}
        n = 1
        for name, field in fields.items():
            try:
                val = kwargs[name]
            except (KeyError, AttributeError):
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            source.append(value)
            if name in _filter:
                new_cond[name] = value
            cols.append("{} = {}".format(name, "${}".format(n)))  # pylint: disable=C0209
            n += 1
        try:
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f'UPDATE: {_update}')
            stmt = await self._connection.prepare(_update)
            result = await stmt.fetchrow(*source, timeout=2)
            self._logger.debug(stmt.get_statusmsg())
            print(f'UPDATE {result}: {_filter!s}')

            new_conditions = {**_filter, **new_cond}
            condition = self._where(fields, **new_conditions)

            _all = f"SELECT * FROM {table} {condition}"
            result = await self._connection.fetch(_all)
            if result:
                return [model(**dict(r)) for r in result]
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {model.Meta.name}: {err!s}"
            ) from err
