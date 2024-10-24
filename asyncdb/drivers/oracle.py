"""Oracle Driver.
"""

import os
import asyncio
from typing import Union, Any, Optional
from collections.abc import Iterable
import time
from datetime import datetime
import oracledb
from ..utils.encoders import DefaultEncoder
from ..exceptions import DriverError
from .sql import SQLDriver


class oracle(SQLDriver):
    """
    oracle Driver class for interacting with Oracle asynchronously using oracledb.

    Attributes:
    -----------
    _provider : str
        Name of the database provider ('oracle').
    _syntax : sql
    _dsn : str
        Data Source Name (DSN) for connecting to the database, if provided.
    _connection : OracleDB connections.
        Holds the active connection to the database.
    _connected : bool
        Indicates if the driver is currently connected to the database.
    """

    _provider = "oracle"
    _syntax = "sql"

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        """
        Initializes the Oracle driver with the given DSN,
        event loop, and optional parameters.

        Parameters:
        -----------
        dsn : str, optional
            The Data Source Name for the database connection. Defaults to an empty string.
        loop : asyncio.AbstractEventLoop, optional
            The event loop to use for asynchronous operations. Defaults to None.
        params : dict, optional
            Additional connection parameters as a dictionary. Defaults to None.
        kwargs : dict
            Additional keyword arguments to pass to the base SQLDriver.
        """
        self._test_query = "SELECT 1 FROM dual"
        _starttime = datetime.now()
        self._dsn = "{host}:{port}/{database}"
        self._database = None
        self.application_name = os.getenv("APP_NAME", "ASYNCDB")
        if params:
            self._database = params.get("database", kwargs.get("database", None))
        try:
            self._lib_dir = params["oracle_client"]
        except (KeyError, TypeError):
            self._lib_dir = kwargs.get("oracle_client", None)
        try:
            super().__init__(dsn=dsn, loop=loop, params=params, **kwargs)
            _generated = datetime.now() - _starttime
            print(f"Oracle Started in: {_generated}")
        except Exception as err:
            raise DriverError(f"Oracle Error: {err}") from err
        # set the JSON encoder:
        self._encoder = DefaultEncoder()
        # Initialize executor and connection
        self._executor = None
        self._connection = None

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError

    async def connection(self):
        user = self._params.get("user", None)
        password = self._params.get("password", None)
        if self._lib_dir is not None:
            oracledb.init_oracle_client(lib_dir=self._lib_dir, driver_name=f"{self.application_name} : 1.0")
        self._executor = self.get_executor(executor="thread", max_workers=10)
        try:
            self._connection = await self._thread_func(
                oracledb.connect, dsn=self._dsn, user=user, password=password, executor=self._executor
            )
            print("Connection: ", self._connection)
            self._connected = True
            self._initialized_on = time.time()
            if self._init_func is not None and callable(self._init_func):
                await self._init_func(self._connection)  # pylint: disable=E1102
            return self
        except Exception as ex:
            self._logger.exception("Error connecting to Oracle", exc_info=True)
            raise DriverError(f"Oracle Connection Error: {ex!s}") from ex

    async def close(self, timeout: int = 10) -> None:
        """
        Closes the active connection to the Oracle database asynchronously.

        Parameters:
        -----------
        timeout : int, optional
            The time in seconds to wait before forcefully closing the connection.
            Defaults to 10 seconds.
        """
        try:
            if self._connection:
                close = self._thread_func(self._connection.close, executor=self._executor)
                await asyncio.wait_for(close, timeout)
                print(f"{self._provider}: Closed connection.")
        except Exception as e:
            print(e)
            self._logger.exception(e, stack_info=True)
            raise DriverError(f"Oracle Closing Error: {e!s}") from e
        finally:
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None
            self._connected = False
            self._connection = None

    async def get_columns(self):
        raise NotImplementedError

    async def use(self, database: str) -> bool:
        """
        Changes the current schema (database) to the specified one.

        Parameters:
        -----------
        database : str
            The name of the schema to switch to.

        Returns:
        --------
        bool
            Returns True if the schema change was successful.

        Raises:
        -------
        DriverError
            If not connected to the database or an error
            occurs during the operation.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        try:
            # Build the ALTER SESSION statement
            sql = f"ALTER SESSION SET CURRENT_SCHEMA = {database}"
            # Execute the statement
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.execute, sql, executor=self._executor)
            await self._thread_func(cursor.close, executor=self._executor)
            self._database = database
            self._logger.info(f"Changed current schema to {database}")
            return True
        except Exception as e:
            self._logger.exception("Error changing schema", exc_info=True)
            raise DriverError(f"Error changing schema: {e}") from e

    async def execute(self, sentence: str, *args, **kwargs) -> bool:
        """
        Executes a non-returning SQL statement (like INSERT, UPDATE) asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL statement to execute.
        args : tuple
            Additional positional arguments for the SQL statement.
        kwargs : dict
            Additional keyword arguments for the SQL statement.

        Returns:
        --------
        bool
            Returns True if the execution was successful.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        try:
            # Get a cursor
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            # Execute the statement
            await self._thread_func(cursor.execute, sentence, *args, executor=self._executor)
            # Commit the transaction
            await self._thread_func(self._connection.commit, executor=self._executor)
            # Close the cursor
            await self._thread_func(cursor.close, executor=self._executor)
            self._logger.info(f"Executed statement: {sentence}")
            return True
        except Exception as e:
            self._logger.exception("Error executing statement", exc_info=True)
            raise DriverError(f"Error executing statement: {e}") from e

    async def execute_many(self, sentence: str, params: list) -> bool:
        """
        Executes multiple non-returning SQL statements asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL statement to execute.
        params : list
            A list of parameter tuples to execute with the statement.

        Returns:
        --------
        bool
            Returns True if all executions were successful.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        try:
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.executemany, sentence, params, executor=self._executor)
            await self._thread_func(self._connection.commit, executor=self._executor)
            await self._thread_func(cursor.close, executor=self._executor)
            self._logger.info(f"Executed multiple statements: {sentence}")
            return True
        except Exception as e:
            self._logger.exception("Error executing multiple statements", exc_info=True)
            raise DriverError(f"Error executing multiple statements: {e}") from e

    execute_many = execute_many

    async def query(self, sentence: str = "", **kwargs) -> Iterable[Any]:
        """
        Executes a query and retrieves all rows asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL query to execute.
        kwargs : dict
            Additional keyword arguments for the query.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the rows returned by the query.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        error = None
        try:
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.execute, sentence, executor=self._executor)
            result = await self._thread_func(cursor.fetchall, executor=self._executor)
            # Get column names
            columns = [col[0] for col in cursor.description]
            # Build list of dicts
            data = [dict(zip(columns, row)) for row in result]
            await self._thread_func(cursor.close, executor=self._executor)
            return await self._serializer(data, error)
        except Exception as e:
            self._logger.exception("Error executing query", exc_info=True)
            error = e
            return await self._serializer([], error)

    async def queryrow(self, sentence: str = "", **kwargs) -> Optional[dict]:
        """
        Executes a query and retrieves a single row asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL query to execute.
        kwargs : dict
            Additional keyword arguments for the query.

        Returns:
        --------
        Optional[dict]
            A dictionary representing the single row returned by the query, or None if no rows are returned.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        error = None
        try:
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.execute, sentence, executor=self._executor)
            row = await self._thread_func(cursor.fetchone, executor=self._executor)
            if row is not None:
                columns = [col[0] for col in cursor.description]
                data = dict(zip(columns, row))
            else:
                data = None
            await self._thread_func(cursor.close, executor=self._executor)
            return await self._serializer(data, error)
        except Exception as e:
            self._logger.exception("Error executing queryrow", exc_info=True)
            error = e
            return await self._serializer(None, error)

    async def fetch(self, sentence: str = "", **kwargs) -> Iterable[Any]:
        """
        Executes a query and retrieves all rows asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL query to execute.
        kwargs : dict
            Additional keyword arguments for the query.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the rows returned by the query.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        error = None
        try:
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.execute, sentence, executor=self._executor)
            result = await self._thread_func(cursor.fetchall, executor=self._executor)
            # Get column names
            columns = [col[0] for col in cursor.description]
            # Build list of dicts
            data = [dict(zip(columns, row)) for row in result]
            await self._thread_func(cursor.close, executor=self._executor)
            return data
        except Exception as e:
            self._logger.exception("Error executing query", exc_info=True)
            raise DriverError(f"Fetch One error: {e}") from e

    fetch_all = fetch

    async def fetch_one(self, sentence: str = "", **kwargs) -> Optional[dict]:
        """
        Executes a query and retrieves a single row asynchronously.

        Parameters:
        -----------
        sentence : str
            The SQL query to execute.
        kwargs : dict
            Additional keyword arguments for the query.

        Returns:
        --------
        Optional[dict]
            A dictionary representing the single row returned by the query,
            or None if no rows are returned.

        Raises:
        -------
        DriverError
            If not connected to the database or an error occurs during execution.
        """
        if not self._connected:
            raise DriverError("Not connected to database")
        try:
            cursor = await self._thread_func(self._connection.cursor, executor=self._executor)
            await self._thread_func(cursor.execute, sentence, executor=self._executor)
            row = await self._thread_func(cursor.fetchone, executor=self._executor)
            if row is not None:
                columns = [col[0] for col in cursor.description]
                data = dict(zip(columns, row))
            else:
                data = None
            await self._thread_func(cursor.close, executor=self._executor)
            return data
        except Exception as e:
            self._logger.exception("Error executing queryrow", exc_info=True)
            raise DriverError(f"Fetch One error: {e}") from e

    fetchone = fetch_one
