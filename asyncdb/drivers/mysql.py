#!/usr/bin/env python3

import asyncio
import time
from typing import Optional, Union, Any
from collections.abc import Callable, Iterable
import ssl
import asyncmy
from asyncmy.cursors import DictCursor
from ..exceptions import (
    ConnectionTimeout,
    NoDataFound,
    DriverError,
    StatementError,
)
from ..interfaces.cursors import DBCursorBackend
from .base import BasePool
from .sql import SQLCursor, SQLDriver


class mysqlCursor(SQLCursor):
    _connection: Any = None


class mysqlPool(BasePool):
    _setup_func: Optional[Callable] = None
    _init_func: Optional[Callable] = None

    def __init__(
        self, dsn: str = None, loop: asyncio.AbstractEventLoop = None, params: Optional[dict] = None, **kwargs
    ):
        self._test_query = "SELECT 1"
        self._max_clients = 300
        self._min_size = 10
        self._dsn = "mysql://{user}:{password}@{host}:{port}/{database}"
        self._init_command = kwargs.pop("init_command", None)
        self._sql_modes = kwargs.pop("sql_modes", None)
        super(mysqlPool, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    async def connect(self):
        """
        Create a database connection pool.
        """
        try:
            # TODO: pass a setup class for set_builtin_type_codec and a setup for add listener
            params = {}
            if self._init_command:
                params["init_command"] = self._init_command
            if self._sql_modes:
                params["sql_mode"] = self._sql_modes
            self._pool = await asyncmy.create_pool(
                host=self._params["host"],
                port=int(self._params["port"]),
                user=self._params["user"],
                password=self._params["password"],
                database=self._params["database"],
                connect_timeout=self._timeout,
                **params,
            )
        except TimeoutError as err:
            raise ConnectionTimeout(f"MySQL: Unable to connect to database: {err}")
        except ConnectionRefusedError as err:
            raise DriverError(f"MySQL: Unable to connect to database, connection Refused: {err}")
        except Exception as err:
            raise DriverError(f"Unknown Error: {err}")
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def acquire(self):
        """
        Take a connection from the pool.
        """
        try:
            self._connection = await self._pool.acquire()
        except Exception as err:
            raise DriverError(f"MySQL: Unable to acquire a connection from the pool: {err}")
        if self._connection:
            db = mysql(pool=self)
            db.set_connection(self._connection)
        return db

    async def release(self, connection=None):
        """
        Release a connection from the pool
        """
        if not connection:
            conn = self._connection
        elif isinstance(connection, mysql):
            conn = connection.get_connection()
        else:
            conn = connection
        try:
            await self._pool.release(conn)
        except Exception as err:
            raise DriverError(f"MySQL: Unable to release a connection from the pool: {err}")

    async def wait_close(self, gracefully=True):
        """
        close
            Close Pool Connection
        """
        if self._pool:
            # try to closing main connection
            try:
                if self._connection:
                    await self._pool.release(self._connection)
            except Exception as err:
                raise DriverError(f"MySQL: Unable to release a connection from the pool: {err}")
            # at now, try to closing pool
            try:
                self._pool.close()
            except Exception as err:
                raise DriverError(f"Closing Error: {err}")
            finally:
                self._pool.terminate()
                self._pool = None

    async def close(self):
        """
        Close Pool.
        """
        try:
            await self._pool.clear()
            self._pool.close()
        except Exception as err:
            print(f"MySQL: Unable to close the pool: {err}")
            self._pool.terminate()

    disconnect = close

    def terminate(self):
        self._pool.terminate()

    async def execute(self, sentence, *args):
        """
        Execute a connection into the Pool
        """
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cursor:
                    result = await cursor.execute(sentence, *args)
            return result
        except Exception as err:
            raise DriverError(f"MySQL: Unable to Execute: {err}")

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


class mysql(SQLDriver, DBCursorBackend):
    _provider = "mysql"
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        self._dsn = "mysql://{user}:{password}@{host}:{port}/{database}"
        self._prepared = None
        self._cursor = None
        self._transaction = None
        self._server_settings = {}
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

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                if self._pool:
                    await self._pool.release(self._connection)
                else:
                    self._connection.close()
        except Exception as err:
            raise DriverError(f"Error on Close Connection: {err}")
        finally:
            self._connection = None
            self._connected = False

    def terminate(self):
        self.terminate()

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        self._cursor = None
        try:
            if not self._pool:
                params = {}
                self._connection = await asyncmy.connect(
                    host=self._params["host"],
                    port=int(self._params["port"]),
                    user=self._params["user"],
                    password=self._params["password"],
                    database=self._params["database"],
                    connect_timeout=self._timeout,
                    **params,
                )
            else:
                self._connection = await self._pool.acquire()
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            self._connection = None
            self._cursor = None
            raise DriverError(f"Connection Error: {err}")
        finally:
            return self

    async def release(self):
        """
        Release a Connection
        """
        try:
            if not await self._connection._closed:
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
        if self._pool:
            return not self._pool._closed
        elif self._connection:
            return not self._connection._closed
        else:
            return False

    async def prepare(self, sentence: str):
        """
        Preparing a sentence
        """
        raise NotImplementedError

    async def query(self, sentence: str, size: int = None):
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sentence)
                if not size:
                    self._result = await cursor.fetchall()
                else:
                    self._result = await cursor.fetchmany(size)
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

    async def queryrow(self, sentence: str):
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sentence)
                self._result = await cursor.fetchone()
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

    async def fetch_one(self, sentence: str):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sentence)
                result = await cursor.fetchone()
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

    async def fetch_all(self, sentence: str):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sentence)
                result = await cursor.fetchall()
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

    async def fetch_many(self, sentence: str, size: int = 1000):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sentence)
                result = await cursor.fetchmany(size)
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

    async def execute(self, sentence: str):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor() as cursor:
                result = await cursor.execute(sentence)
            return [result, None]
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
        finally:
            self.generated_at()
            return [result, error]

    async def executemany(self, sentence: str, args: Union[tuple, list]):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            async with self._connection.cursor(cursor=DictCursor) as cursor:
                result = await cursor.executemany(sentence, args)
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
