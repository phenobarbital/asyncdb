#!/usr/bin/env python3

import asyncio
import time
from typing import Optional, Union, Any
from collections.abc import Callable, Iterable
import ssl
from concurrent.futures import ThreadPoolExecutor
import MySQLdb
from MySQLdb.cursors import DictCursor
from asyncdb.exceptions import (
    ConnectionTimeout,
    NoDataFound,
    DriverError,
)
from ..interfaces.cursors import DBCursorBackend
from .base import BasePool
from .sql import SQLCursor, SQLDriver


class mysqlCursor(SQLCursor):
    _connection: Any = None


class mysqlclientPool(BasePool):
    _setup_func: Optional[Callable] = None
    _init_func: Optional[Callable] = None

    def __init__(
        self, dsn: str = None, loop: asyncio.AbstractEventLoop = None, params: Optional[dict] = None, **kwargs
    ):
        self._test_query = "SELECT 1"
        self._max_clients = 30
        self._min_size = 10
        self._dsn = "mysql://{user}:{password}@{host}:{port}/{database}"
        self._init_command = kwargs.pop("init_command", None)
        self._sql_modes = kwargs.pop("sql_modes", None)
        self._executor = ThreadPoolExecutor(max_workers=self._min_size)
        self._queue = asyncio.Queue(maxsize=self._max_clients)
        self._current_size: int = 0
        super(mysqlclientPool, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    async def _connection_(self):
        params = {}
        if self._init_command:
            params["init_command"] = self._init_command
        if self._sql_modes:
            params["sql_mode"] = self._sql_modes
        connection = await self._thread_func(
            MySQLdb.connect,
            host=self._params["host"],
            port=int(self._params["port"]),
            user=self._params["user"],
            password=self._params["password"],
            database=self._params["database"],
            connect_timeout=self._timeout,
            **params,
            executor=self._executor,
        )
        return connection

    async def connect(self):
        """
        Create a database connection pool.
        """
        self._logger.debug("MySQL Client: Connecting to {}".format(self._params))
        try:
            # First connection of Pool
            self._pool = await self._connection_()
        except MySQLdb.OperationalError as err:
            raise ConnectionTimeout(f"MySQL: Unable to connect to database: {err}")
        except Exception as err:
            raise DriverError(f"Unknown Error: {err}")
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def acquire(self):
        """
        Acquire a connection from the pool, creating a new one if needed.
        """
        if self._queue.empty() and self._current_size < self._max_clients:
            try:
                self._connection = await self._connection_()
                self._current_size += 1
            except Exception as err:
                raise DriverError(f"MySQL: Unable to acquire a connection from the pool: {err}")
        if self._connection:
            db = mysqlclient(pool=self)
            db.set_connection(self._connection)
        return db

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        elif isinstance(connection, mysqlclient):
            conn = connection.get_connection()
        else:
            conn = connection
        try:
            if self._queue.full():
                conn.close()
                self._current_size -= 1
            else:
                await self._queue.put(conn)
        except Exception as err:
            raise DriverError(f"MySQL: Unable to release a connection from the pool: {err}")

    async def wait_close(self, gracefully=True):
        """
        close
            Close Pool Connection
        """
        raise NotImplementedError

    async def close(self):
        """
        Close Pool.
        """
        while not self._queue.empty():
            conn = await self._queue.get()
            try:
                conn.close()
            except MySQLdb.OperationalError as err:
                self._logger.warning(f"MySQL: Unable to close connection: {err}")
            self._current_size -= 1
        # Close connection from the pool:
        await self._thread_func(self._pool.close)
        self._connected = False
        self._logger.debug("MySQL Connection Closed.")

    disconnect = close

    def terminate(self):
        self._pool.terminate()

    def _execute(self, conn, sentence: str, *args):
        """
        Execute a connection into the Pool
        """
        try:
            with conn.cursor() as cursor:
                result = cursor.execute(sentence, *args)
            return result
        except MySQLdb.Error as e:
            raise DriverError(f"Error executing query: {e}")
        except Exception as err:
            raise DriverError(f"MySQL: Unable to Execute: {err}")

    async def execute(self, sentence, *args):
        """
        Execute a connection into the Pool
        """
        error = None
        result = None
        try:
            loop = asyncio.get_running_loop()
            conn = await self._connection_()
            result = await loop.run_in_executor(self._executor, self._execute, conn, sentence, *args)
        except Exception as err:
            error = f"MySQL: Unable to Execute: {err}"
        finally:
            conn.close()
            return [result, error]

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
        except DriverError as err:
            error = err
        finally:
            return [result, error]  # pylint: disable=W0150


class mysqlclient(SQLDriver, DBCursorBackend):
    _provider = "mysql"
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        self._dsn = "mysql://{user}:{password}@{host}:{port}/{database}"
        self._prepared = None
        self._cursor = None
        self._transaction = None
        self._init_command = kwargs.pop("init_command", None)
        self._sql_modes = kwargs.pop("sql_modes", None)
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        if "pool" in kwargs:
            self._pool = kwargs["pool"]
            self._loop = self._pool.get_loop()
        ### SSL Support:
        self.ssl: bool = False
        if params and "ssl" in params:
            ssloptions = params["ssl"]
        elif "ssl" in kwargs:
            ssloptions = kwargs["ssl"]
        else:
            ssloptions = None
        if ssloptions:
            self.ssl: bool = True
            try:
                check_hostname = ssloptions["check_hostname"]
            except KeyError:
                check_hostname = False
            ### certificate Support:
            try:
                ca_file = ssloptions["cafile"]
            except KeyError:
                ca_file = None
            args = {"cafile": ca_file}
            self.sslctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, **args)
            # Certificate Chain:
            try:
                certs = {"certfile": ssloptions["certfile"], "keyfile": ssloptions["keyfile"]}
            except KeyError:
                certs = {"certfile": None, "keyfile": None}
            if certs["certfile"]:
                self.sslctx.load_cert_chain(**certs)
            self.sslctx.check_hostname = check_hostname

    async def close(self, timeout: int = 10) -> None:
        try:
            if self._connection:
                close = self._thread_func(self._connection.close)
                await asyncio.wait_for(close, timeout)
        except Exception as e:
            print(e)
            self._logger.exception(e, stack_info=True)
            raise DriverError(f"MySQL Closing Error: {e!s}") from e
        finally:
            self._connected = False
            self._connection = None

    def terminate(self):
        self.terminate()

    async def _connection_(self):
        params = {}
        if self._init_command:
            params["init_command"] = self._init_command
        if self._sql_modes:
            params["sql_mode"] = self._sql_modes
        try:
            connection = await self._thread_func(
                MySQLdb.connect,
                host=self._params["host"],
                port=int(self._params["port"]),
                user=self._params["user"],
                password=self._params["password"],
                database=self._params["database"],
                connect_timeout=self._timeout,
                **params,
                executor=self._executor,
            )
        except Exception as exc:
            raise DriverError(f"MySQL: Unable to connect to database: {exc}")
        return connection

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            if not self._pool:
                self._connection = await self._connection_()
            else:
                self._connection = await self._pool.acquire()
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except DriverError:
            raise
        except Exception as err:
            self._connection = None
            raise DriverError(f"Connection Error: {err}")
        finally:
            return self

    async def release(self):
        """
        Release a Connection
        """
        try:
            if self._pool:
                release = asyncio.create_task(self._pool.release(self._connection, timeout=10))
                asyncio.ensure_future(release, loop=self._loop)
                return await release
            else:
                self._connection.close()
        except Exception as err:
            raise DriverError(f"Release Error: {err}")
        finally:
            self._connected = False
            self._connection = None

    def prepared_statement(self):
        return self._prepared

    @property
    def connected(self):
        return self._connected

    async def prepare(self, sentence: str):
        """
        Preparing a sentence
        """
        raise NotImplementedError

    def _query_(self, conn, sentence: str, *args, returns: str = "all", size: int = None):
        """
        Execute a connection into the Pool
        """
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(sentence, *args)
                if returns == "all":
                    result = cursor.fetchall()
                elif returns == "one":
                    result = cursor.fetchone()
                elif returns == "many":
                    result = cursor.fetchmany(size)
                else:
                    result = cursor.fetchall()
            return result
        except MySQLdb.Error as e:
            raise DriverError(f"Error executing query: {e}")
        except Exception as err:
            raise DriverError(f"MySQL: Unable to Execute: {err}")

    async def query(self, sentence: str, *args, size: int = None):
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            if size is not None:
                returns = "many"
            else:
                returns = "all"
            self._result = await self._thread_func(
                self._query_, self._connection, sentence, *args, returns=returns, size=size
            )
            if not self._result:
                raise NoDataFound("MySQL: No Data was Found")
        except NoDataFound:
            error = "Mysql: No Data was Found"
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
        finally:
            self.generated_at()
            if error:
                return [None, error]
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def queryrow(self, sentence: str, *args):
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            self._result = await self._thread_func(self._query_, self._connection, sentence, *args, returns="one")
            if not self._result:
                raise NoDataFound("MySQL: No Data was Found")
        except NoDataFound:
            error = "Mysql: No Data was Found"
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
        finally:
            self.generated_at()
            if error:
                return [None, error]
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def fetch_one(self, sentence: str, *args):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            result = await self._thread_func(self._query_, self._connection, sentence, *args, returns="one")
            if not result:
                raise NoDataFound("MySQL: No Data was Found")
            return result
        except NoDataFound:
            raise
        except RuntimeError as err:
            raise DriverError(f"MySQL Runtime Error: {err}")
        except Exception as err:
            raise DriverError(f"MySQL: Error on Query: {err}")
        finally:
            self.generated_at()

    async def fetch_all(self, sentence: str, *args):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            result = await self._thread_func(self._query_, self._connection, sentence, *args, returns="all")
            if not result:
                raise NoDataFound("MySQL: No Data was Found")
            return result
        except NoDataFound:
            raise
        except RuntimeError as err:
            raise DriverError(f"MySQL Runtime Error: {err}")
        except Exception as err:
            raise DriverError(f"MySQL: Error on Query: {err}")
        finally:
            self.generated_at()

    async def fetch_many(self, sentence: str, *args, size: int = 1000):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            result = await self._thread_func(self._query_, self._connection, sentence, *args, returns="many", size=size)
            if not result:
                raise NoDataFound("MySQL: No Data was Found")
            return result
        except NoDataFound:
            raise
        except RuntimeError as err:
            raise DriverError(f"MySQL Runtime Error: {err}")
        except Exception as err:
            raise DriverError(f"MySQL: Error on Query: {err}")
        finally:
            self.generated_at()

    def _execute_(self, conn, sentence: str, *args):
        """
        Execute a connection into the Pool
        """
        try:
            with conn.cursor(DictCursor) as cursor:
                result = cursor.execute(sentence, *args)
            return result
        except MySQLdb.Error as e:
            raise DriverError(f"Error executing query: {e}")
        except Exception as err:
            raise DriverError(f"MySQL: Unable to Execute: {err}")

    async def execute(self, sentence: str, *args):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            result = await self._thread_func(self._execute_, self._connection, sentence, *args)
            return [result, None]
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
        finally:
            self.generated_at()
            return [result, error]

    def _executemany_(self, conn, sentence: str, args):
        """
        Execute a connection into the Pool
        """
        try:
            with conn.cursor(DictCursor) as cursor:
                result = cursor.executemany(sentence, args)
            return result
        except MySQLdb.Error as e:
            raise DriverError(f"Error executing query: {e}")
        except Exception as err:
            raise DriverError(f"MySQL: Unable to Execute: {err}")

    async def executemany(self, sentence: str, args: Union[tuple, list]):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            result = await self._thread_func(self._executemany_, self._connection, sentence, args)
            return result
        except Exception as err:
            raise DriverError(f"MySQL: Error on Execute: {err}")
        finally:
            self.generated_at()

    execute_many = executemany

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    async def use(self, database: str):
        raise NotImplementedError  # pragma: no cover

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
