from typing import Optional, Any
from collections.abc import Iterable, Sequence
import asyncio
import time
import motor.motor_asyncio
import pymongo
import pandas as pd
from ..exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    DriverError,
    StatementError,
    TooManyConnections,
)
from .base import BaseDriver


class mongo(BaseDriver):
    """
    mongo Driver class for interacting with MongoDB asynchronously using motor.

    Attributes:
    -----------
    _provider : str
        Name of the database provider ('mongodb').
    _syntax : None
        Not applicable for MongoDB.
    _dsn : str
        Data Source Name (DSN) for connecting to the database, if provided.
    _connection : motor.motor_asyncio.AsyncIOMotorClient
        Holds the active connection to the database.
    _connected : bool
        Indicates if the driver is currently connected to the database.
    """

    _provider = "mongodb"
    _dsn = "'mongodb://{host}:{port}"
    _syntax = "mongo"
    _parameters = ()
    _initialized_on = None
    _timeout: int = 5

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        """
        Initializes the MongoDBDriver with the given DSN,
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
        if "username" in params:
            self._dsn = "mongodb://{username}:{password}@{host}:{port}"
        if "database" in params:
            self._dsn = self._dsn + "/{database}"
            self._database_name = params.get("database", kwargs.get("database", None))
        super().__init__(dsn, loop, params, **kwargs)
        self._connection = None
        self._database = None
        self._databases: list = []

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            if self._dsn:
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(self._dsn)
            else:
                params = {"host": self._params.get("host", "localhost"), "port": self._params.get("port", 27017)}
                if "username" in self._params:
                    params["username"] = self._params["username"]
                    params["password"] = self._params["password"]
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(**params)
            try:
                self._databases = await self._connection.list_database_names()
            except Exception as err:
                raise DriverError(f"Error Connecting to Mongo: {err}") from err
            if len(self._databases) > 0:
                self._connected = True
                self._initialized_on = time.time()
            return self
        except Exception as err:
            self._connection = None
            self._cursor = None
            print(err)
            raise DriverError(f"connection Error, Terminated: {err}") from err

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                try:
                    self._connection.close()
                except Exception as err:
                    self._connection = None
                    raise DriverError(f"Connection Error, Terminated: {err}")
        except Exception as err:
            raise DriverError(f"Close Error: {err}")
        finally:
            self._connection = None
            self._connected = False

    async def test_connection(self):
        """
        Getting information about Server.

        Returns:
        --------
        [result, error] : list
            A list containing the server information and any error that occurred.
        """
        error = None
        result = None
        if self._connection:
            try:
                result = await self._connection.server_info()
            except Exception as err:
                error = err
            finally:
                return [result, error]
        else:
            error = DriverError("Not connected to MongoDB")
            return [None, error]

    async def use(self, database: str):
        """
        Switches the current database to the specified one.

        Parameters:
        -----------
        database : str
            The name of the database to switch to.

        Returns:
        --------
        None
        """
        if self._connection:
            self._database = self._connection[database]
            return self._database
        else:
            raise DriverError(f"Not connected to MongoDB on DB {database}")

    async def execute(self, collection_name: str, operation: str, *args, **kwargs) -> Optional[Any]:
        """
        Executes an operation (insert, update, delete) on a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to operate on.
        operation : str
            The operation to perform
            ('insert_one', 'insert_many', 'update_one', 'update_many',
            'delete_one', 'delete_many', etc.).
        args : tuple
            Additional positional arguments to be passed to the operation.
        kwargs : dict
            Additional keyword arguments to be passed to the operation.

        Returns:
        --------
        Optional[Any]
            The result of the operation, if any.
        """
        error = None
        result = None
        if not self._database:
            raise DriverError("No database selected. Use 'use' method to select a database.")

        collection = self._database[collection_name]
        try:
            method = getattr(collection, operation)
            result = await method(*args, **kwargs)
        except Exception as err:
            error = err
        return (result, error)

    async def execute_many(self, collection_name: str, operation: str, documents: list) -> Optional[Any]:
        """
        Executes a bulk operation on a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to operate on.
        operation : str
            The bulk operation to perform
            ('insert_many', 'update_many', 'delete_many', etc.).
        documents : list
            The list of documents or operations to perform.

        Returns:
        --------
        Optional[Any]
            The result of the bulk operation, if any.
        """
        error = None
        result = None
        if not self._database:
            raise DriverError("No database selected. Use 'use' method to select a database.")

        collection = self._database[collection_name]
        try:
            method = getattr(collection, operation)
            result = await method(documents)
        except Exception as err:
            error = err
        return (result, error)

    executemany = execute_many

    async def __aenter__(self) -> Any:
        """
        Asynchronous context manager entry.
        Establishes a connection when entering the context.

        Returns:
        --------
        self : MongoDBDriver
            Returns the instance of the driver itself.

        Raises:
        -------
        DriverError
            If an error occurs during connection establishment.
        """
        try:
            await self.connection()
        except Exception as err:
            error = f"Error on Connection: {err}"
            raise DriverError(message=error) from err
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Asynchronous context manager exit.
        Closes the connection when exiting the context.

        Parameters:
        -----------
        exc_type : type
            Exception type.
        exc : Exception
            Exception instance.
        tb : traceback
            Traceback object.

        Returns:
        --------
        None
        """
        await self.close()

    async def query(self, collection_name: str, filter: dict = None, *args, **kwargs) -> Iterable[Any]:
        """
        Executes a query to retrieve documents from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        filter : dict, optional
            The filter criteria for the query. Defaults to None (no filter).
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the documents returned by the query.
        """
        error = None
        result = None
        if not self._database:
            self._database = self.use(database=self._database_name)
            if not self._database:
                raise DriverError("No database selected. Use 'use' method to select it.")

        collection = self._database[collection_name]
        cursor = collection.find(filter or {}, *args, **kwargs)
        result = []
        try:
            async for document in cursor:
                result.append(document)
        except Exception as err:
            error = err
        return await self._serializer(result, error)

    async def queryrow(self, collection_name: str, filter: dict = None, *args, **kwargs) -> Optional[dict]:
        """
        Executes a query to retrieve a single document from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        filter : dict, optional
            The filter criteria for the query. Defaults to None (no filter).
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Optional[dict]
            The document returned by the query, or None if no document matches.
        """
        error = None
        result = None
        if not self._database:
            self._database = self.use(database=self._database_name)
            if not self._database:
                raise DriverError("No database selected. Use 'use' method to select it.")

        collection = self._database[collection_name]
        try:
            result = await collection.find_one(filter or {}, *args, **kwargs)
        except Exception as err:
            error = err
        return await self._serializer(result, error)

    async def fetch(self, collection_name: str, filter: dict = None, *args, **kwargs) -> Iterable[Any]:
        """
        Executes a query to retrieve documents from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        filter : dict, optional
            The filter criteria for the query. Defaults to None (no filter).
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Iterable[Any]
            An iterable containing the documents returned by the query.
        """
        result = None
        if not self._database:
            self._database = self.use(database=self._database_name)
            if not self._database:
                raise DriverError("No database selected. Use 'use' method to select it.")

        collection = self._database[collection_name]
        cursor = collection.find(filter or {}, *args, **kwargs)
        result = []
        try:
            async for document in cursor:
                result.append(document)
        except Exception as err:
            raise DriverError(f"Error Getting Data from Mongo {err}")
        return result

    fetch_all = fetch

    async def fetch_one(self, collection_name: str, filter: dict = None, *args, **kwargs) -> Optional[dict]:
        """
        Executes a query to retrieve a single document from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        filter : dict, optional
            The filter criteria for the query. Defaults to None (no filter).
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Optional[dict]
            The document returned by the query, or None if no document matches.
        """
        result = None
        if not self._database:
            self._database = self.use(database=self._database_name)
            if not self._database:
                raise DriverError("No database selected. Use 'use' method to select it.")

        collection = self._database[collection_name]
        try:
            result = await collection.find_one(filter or {}, *args, **kwargs)
        except Exception as err:
            raise DriverError(f"No row to be returned {err}")
        return result

    fetchrow = fetch_one
    fetchone = fetch_one

    async def write(
        self,
        data,
        table: str = None,
        database: str = None,
        use_pandas: bool = True,
        if_exists: str = "replace",
        **kwargs,
    ) -> bool:
        """
        Writes data to a collection asynchronously,
        with upsert functionality.

        Parameters:
        -----------
        data : Iterable or pandas DataFrame
            The data to be written, which can be any iterable of documents or pandas DataFrame.
        table : str, optional
            The name of the collection where the data will be written.
        database : str, optional
            The name of the database where the collection resides.
        use_pandas : bool, optional
            If True, uses pandas DataFrame to process the data. Defaults to True.
        if_exists : str, optional
            Specifies what to do if the document already exists ('replace').
            Defaults to 'replace'.
        kwargs : dict
            Additional keyword arguments, e.g., key_field for upsert identification.

        Returns:
        --------
        bool
            Returns True if the write operation is successful, otherwise False.
        """
        # Ensure database is selected
        if database:
            await self.use(database)
        if not self._database:
            raise DriverError("No database selected. Use 'use' method to select a database.")

        if not table:
            raise ValueError("No collection (table) specified.")

        collection = self._database[table]

        # Process data
        if use_pandas and isinstance(data, pd.DataFrame):
            # Assume data is a pandas DataFrame
            documents = data.to_dict("records")
        elif isinstance(data, Iterable):
            # Assume data is an iterable of documents
            documents = list(data)
        else:
            raise ValueError("Mongo: Data must be an iterable or a pandas DataFrame.")

        # Get key_field from kwargs or default to '_id'
        key_field = kwargs.get("key_field", "_id")

        # Build bulk operations
        operations = []
        if if_exists == "replace":
            for doc in documents:
                if key_field not in doc:
                    # If key_field is not in document, generate a unique identifier
                    doc[key_field] = pymongo.ObjectId()
                filter = {key_field: doc[key_field]}
                operations.append(pymongo.UpdateOne(filter, {"$set": doc}, upsert=True))
        elif if_exists == "append":
            # Insert new documents without checking for existing ones
            operations = [pymongo.InsertOne(doc) for doc in documents]
        else:
            raise ValueError("Invalid value for if_exists: choose 'replace' or 'append'")

        # Execute bulk write
        try:
            result = await collection.bulk_write(operations, ordered=False)
            return result
        except Exception as e:
            # Handle exception
            raise DriverError(f"Error during write operation: {e}")
