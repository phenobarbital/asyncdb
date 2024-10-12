import asyncio
from typing import Union, Any, Optional
from collections.abc import Iterable, Sequence, Awaitable
from pathlib import Path
from aiochclient import ChClient
from aiohttp import ClientSession
from .sql import SQLDriver
from ..exceptions import DriverError


class aioch(SQLDriver):
    """
    async version if clickhouse driver for Connecting to a Clickhouse Cluster.
    This class provides a consistent interface using aiochclient.

    Attributes:
    -----------
    _provider : str
        Name of the database provider (e.g., 'clickhouse').
    _syntax : str
        SQL syntax specific to the database provider (e.g., 'sql').
    _dsn : str
        Data Source Name (DSN) template for connecting to the database, if required.
    _connection : Any
        Holds the active connection to the database.
    _connected : bool
        Indicates if the driver is currently connected to the database.
    """

    _provider: str = "clickhouse"
    _syntax: str = "sql"
    _dsn: str = "{database}"
    _test_query: str = "SELECT version()"

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        """
        Initializes the clickhouse with the given DSN,
        event loop, and optional parameters.

        Parameters:
        -----------
        dsn : str, optional
            The Data Source Name for the database connection. Defaults to an empty string.
        loop : asyncio.AbstractEventLoop, optional
            The event loop to use for asynchronous operations. Defaults to None, which uses the current event loop.
        params : dict, optional
            Additional connection parameters as a dictionary. Defaults to None.
        kwargs : dict
            Additional keyword arguments to pass to the base SQLDriver.
        """
        self._session: Awaitable = None
        SQLDriver.__init__(self, dsn=dsn, loop=loop, params=params, **kwargs)

    async def connection(self, **kwargs):
        """
        Establishes a connection to the database asynchronously.

        This method should be overridden by subclasses to implement the logic
        for establishing a connection to the specific database.

        Parameters:
        -----------
        kwargs : dict
            Additional arguments to be used when establishing the connection.

        Returns:
        --------
        self : clickhouse
            Returns the instance of the driver itself after the connection is established.
        """
        self._connection = None
        self._connected = False
        if not self._session:
            self._session = ClientSession()
        try:
            self._connection = ChClient(self._session, **self.params)
            print(self._connection, await self._connection.is_alive())
            if await self._connection.is_alive():
                self._connected = True
            return self
        except Exception as exc:
            raise DriverError(f"clickhouse Error: {exc}") from exc

    connect = connection

    async def close(self, timeout: int = 5) -> None:
        """
        Closes the active connection to the database asynchronously.

        Parameters:
        -----------
        timeout : int, optional
            The time in seconds to wait before forcefully closing the connection. Defaults to 5 seconds.

        Returns:
        --------
        None
        """
        try:
            if self._session:
                await self._session.close()
        finally:
            self._connection = None
            self._connected = False
            self._session = None

    async def __aenter__(self) -> Any:
        """
        Asynchronous context manager entry.
        Establishes a connection when entering the context.

        Returns:
        --------
        self : clickhouse
            Returns the instance of the driver itself.

        Raises:
        -------
        DriverError
            If an error occurs during connection establishment.
        """
        try:
            if not self._session:
                self._session = ClientSession()
            if not self._connection:
                await self.connection()
        except Exception as err:
            error = f"Error on Cursor Fetch: {err}"
            raise DriverError(message=error) from err
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def query(self, sentence: Any, *args, **kwargs) -> Iterable[Any]:
        """
        Executes a query to retrieve data from the database asynchronously.

        Parameters:
        -----------
        sentence : Any
            The SQL query or command to execute.
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the rows returned by the query.
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            result = await self._connection.fetch(sentence)
            if result:
                self._result = result
        except Exception as exc:
            error = exc
        return await self._serializer(self._result, error)

    async def queryrow(self, sentence: Any = None) -> Iterable[Any]:
        """
        Executes a query to retrieve a single row of data from the database asynchronously.

        Parameters:
        -----------
        sentence : Any, optional
            The SQL query or command to execute. Defaults to None.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the single row returned by the query.
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        return await self._serializer(self._result, error)

    async def fetch_all(self, sentence: str, *args, **kwargs) -> Sequence:
        """
        Executes a query to fetch all rows of data without returning errors.

        This method is an alias for `query` but does not return any error information.

        Parameters:
        -----------
        sentence : str
            The SQL query or command to execute.
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Sequence
            A sequence of rows returned by the query.
        """
        cursor = None
        await self.valid_operation(sentence)

    # alias to be compatible with aiosqlite methods.
    fetchall = fetch_all

    async def fetch_many(self, sentence: str, size: int = None):
        """
        Executes a query to fetch a specified number of rows without returning errors.

        This method is an alias for `query`, but without returning any error information.

        Parameters:
        -----------
        sentence : str
            The SQL query or command to execute.
        size : int, optional
            The number of rows to fetch. Defaults to None, which fetches all rows.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the specified number of rows returned by the query.
        """
        await self.valid_operation(sentence)

    fetchmany = fetch_many

    async def fetch_one(self, sentence: str, *args, **kwargs) -> Optional[dict]:
        """
        Executes a query to fetch a single row of data without returning errors.

        This method is an alias for `queryrow`, but without returning any error information.

        Parameters:
        -----------
        sentence : str
            The SQL query or command to execute.
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Optional[dict]
            A dictionary representing the single row returned by the query, or None if no rows are returned.
        """
        await self.valid_operation(sentence)

    fetchone = fetch_one
    fetchrow = fetch_one

    async def execute(self, sentence: Any, **kwargs) -> Optional[Any]:
        """
        Executes a transaction or command that does not necessarily return a result asynchronously.

        Parameters:
        -----------
        sentence : Any
            The SQL command or transaction to execute.
        kwargs : dict
            Additional keyword arguments to be passed to the execution.

        Returns:
        --------
        Optional[Any]
            The result of the execution, if any.
        """
        error = None
        result = None
        return (result, error)

    async def execute_many(self, sentence: Union[str, list], args: list) -> Optional[Any]:
        """
        Executes multiple transactions or commands asynchronously.

        This method is similar to `execute`, but accepts multiple commands to be executed.

        Parameters:
        -----------
        sentence : Union[str, list]
            A single SQL command or a list of commands to execute.
        args : list
            A list of arguments to pass to each command.

        Returns:
        --------
        Optional[Any]
            The result of the executions, if any.
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        return (result, error)

    executemany = execute_many

    async def copy_to(self, sentence: Union[str, Path], destination: str, **kwargs) -> bool:
        """
        Copies the result of a query to a file asynchronously.

        Parameters:
        -----------
        sentence : Union[str, Path]
            The SQL query or the path to a file containing the data to be copied.
        destination : str
            The destination path where the data will be saved.
        kwargs : dict
            Additional keyword arguments to customize the copying process.

        Returns:
        --------
        bool
            Returns True if the copy operation is successful, otherwise False.
        """
        pass

    async def write(
        self,
        data,
        table_id: str = None,
        dataset_id: str = None,
        use_streams: bool = False,
        use_pandas: bool = True,
        if_exists: str = "append",
        **kwargs,
    ) -> bool:
        """
        Writes data to a table asynchronously, optionally using streams or pandas DataFrame.

        Parameters:
        -----------
        data : Any
            The data to be written, which can be a CSV file, stream, or pandas DataFrame.
        table_id : str, optional
            The ID of the table where the data will be written. Defaults to None.
        dataset_id : str, optional
            The ID of the dataset where the table resides. Defaults to None.
        use_streams : bool, optional
            If True, uses streaming to write the data. Defaults to False.
        use_pandas : bool, optional
            If True, uses pandas DataFrame to write the data. Defaults to True.
        if_exists : str, optional
            Specifies what to do if the table already exists. Defaults to "append".
        kwargs : dict
            Additional keyword arguments to customize the writing process.

        Returns:
        --------
        bool
            Returns True if the write operation is successful, otherwise False.
        """
        pass

    async def prepare(self, sentence: Union[str, list]) -> Any:
        """
        Prepares a SQL sentence for execution.

        Currently not implemented for ClickHouse and raises NotImplementedError.

        Parameters:
        -----------
        sentence : Union[str, list]
            The SQL command(s) to prepare.

        Returns:
        --------
        Any
            Typically, this would return a prepared statement object, but this implementation raises NotImplementedError.

        Raises:
        -------
        NotImplementedError
            Raised when called, as ClickHouse does not support prepared statements in this implementation.
        """
        raise NotImplementedError()  # pragma: no cover

    def tables(self, schema: str = "") -> Iterable[Any]:
        """
        Retrieves a list of tables in the specified schema.

        Currently not implemented and raises NotImplementedError.

        Parameters:
        -----------
        schema : str, optional
            The name of the schema to query. Defaults to an empty string.

        Returns:
        --------
        Iterable[Any]
            An iterable of table names.

        Raises:
        -------
        NotImplementedError
            Raised when called, as this implementation does not support table listing.
        """
        raise NotImplementedError()  # pragma: no cover

    def table(self, tablename: str = "") -> Iterable[Any]:
        """
        Retrieves information about a specific table.

        Currently not implemented and raises NotImplementedError.

        Parameters:
        -----------
        tablename : str, optional
            The name of the table to query. Defaults to an empty string.

        Returns:
        --------
        Iterable[Any]
            An iterable of table information.

        Raises:
        -------
        NotImplementedError
            Raised when called, as this implementation does not support detailed table information.
        """
        raise NotImplementedError()  # pragma: no cover

    async def use(self, database: str):
        """
        Switches the default database to the specified one.

        Currently not implemented for ClickHouse and raises NotImplementedError.

        Parameters:
        -----------
        database : str
            The name of the database to switch to.

        Raises:
        -------
        NotImplementedError
            Raised when called, as ClickHouse does not support switching databases.
        """
        raise NotImplementedError("ClickHouse Error: There is no Database in ClickHouse")
