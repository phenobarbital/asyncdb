from typing import Optional, Any, Union, Iterable, List
from collections.abc import Sequence
import asyncio
import time
import ssl
from urllib.parse import urlencode
from bson import ObjectId
import motor.motor_asyncio
import pymongo
import pandas as pd
import pyarrow as pa
from dataclasses import is_dataclass, asdict
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
    MongoDB Driver class for interacting with MongoDB asynchronously using Motor.

    Attributes:
    -----------
    _provider : str
        Name of the database provider ('mongodb').
    _syntax : str
        Syntax type, set to 'mongo'.
    _dsn : str
        Data Source Name (DSN) for connecting to the database, if provided.
    _connection : motor.motor_asyncio.AsyncIOMotorClient
        Holds the active connection to the database.
    _database : motor.motor_asyncio.AsyncIOMotorDatabase
        Reference to the selected database.
    _connected : bool
        Indicates if the driver is currently connected to the database.
    _database_name : str
        Name of the default database to use.
    _databases : list
        List of available databases.
    _timeout : int
        Connection timeout in seconds.
    """

    _provider = "mongodb"
    _dsn_template = "mongodb://{username}:{password}@{host}:{port}/{database}"
    _syntax = "mongo"
    _parameters = ()
    _initialized_on = None
    _timeout: int = 5


    def __init__(
        self,
        dsn: str = "",
        loop: asyncio.AbstractEventLoop = None,
        params: dict = None,
        **kwargs
    ) -> None:
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
            Additional keyword arguments to pass to the base Driver.
        """
        self._connection = None
        self._database = None
        self._databases: List[str] = []
        self._database_name = params.get(
            "database",
            kwargs.get("database", None)
        )
        self._dbtype: str = params.get("dbtype", kwargs.get("dbtype", "mongodb"))
        super(mongo, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        self._dsn = self._construct_dsn(params)
        self._use_ssl: bool = kwargs.get("ssl", False)

    def _construct_dsn(self, params) -> str:
        """Construct DSN based on provided parameters."""
        if not self._params:
            return ""
        username = params.get("username")
        password = params.get("password")
        host = params.get("host", "localhost")
        port = params.get("port", 27017)
        database = self._database_name or ""
        authsource = params.get("authsource", database) or 'admin'
        if username and password:
            base_dsn = self._dsn_template.format(
                username=username,
                password=password,
                host=host,
                port=port,
                database=database,
            )
        else:
            base_dsn = f"mongodb://{host}:{port}/{database}"
        if self._dbtype == 'mongodb':
            return base_dsn + f"?authSource={authsource}"
        elif self._dbtype == 'atlas':
            return f"{base_dsn}?retryWrites=true&w=majority"
        elif self._dbtype == 'documentdb':
            more_params = params.get('connection_params', {})
            self._use_ssl = params.get('ssl', False)
            query_params = {
                "replicaSet": params.get("replicaSet", "rs0"),
                "readPreference": params.get("readPreference", "secondaryPreferred"),
                "retryWrites": params.get("retryWrites", "false"),
                **more_params
            }
            query_string = urlencode(query_params)
            _url = f"{base_dsn}?{query_string}"
            if self._use_ssl:
                cert = str(params.get("tlsCAFile", "global-bundle.pem"))
                _url = _url + "&tls=true"
                _url = _url + f"&tlsCAFile={cert}"
            return _url
        return base_dsn

    async def _select_database(self) -> motor.motor_asyncio.AsyncIOMotorDatabase:
        """
        Internal method to select the database.

        Returns:
        --------
        motor.motor_asyncio.AsyncIOMotorDatabase
            The selected database instance.

        Raises:
        -------
        DriverError
            If the database cannot be selected.
        """
        if self._database is None:
            if self._database_name:
                self._database = self._connection[self._database_name]
            else:
                raise DriverError(
                    "No database selected. Use 'use' method to select a database."
                )
        return self._database

    async def connection(self) -> "mongo":
        """
        Establishes a connection to the MongoDB server.

        Returns:
        --------
        mongo
            Returns the instance of the driver itself.

        Raises:
        -------
        DriverError
            If there is an issue establishing the connection.
        """
        self._connection = None
        self._connected = False
        ssl = {}
        if self._use_ssl:
            ssl = {
                "ssl": True,
                "ssl_cert_reqs": ssl.CERT_NONE,
                "tlsAllowInvalidCertificates": True,
                "ssl_ca_certs": str(self._params.get("tlsCAFile", "global-bundle.pem")),
            }
        try:
            if self._dsn:
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(
                    self._dsn,
                    serverSelectionTimeoutMS=self._timeout * 1000,
                    **ssl
                )
            else:
                params = {
                    "host": self._params.get("host", "localhost"),
                    "port": self._params.get("port", 27017),
                    "serverSelectionTimeoutMS": self._timeout * 1000,
                    **ssl
                }
                if "username" in self._params and "password" in self._params:
                    params["username"] = self._params["username"]
                    params["password"] = self._params["password"]
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(
                    **params
                )
            # Attempt to fetch server info to verify connection
            ad = await self._connection.admin.command('ping')
            if 'ok' not in ad:
                raise DriverError("Ping Connection Error")
            self._connected = True
            self._initialized_on = time.time()
            return self
        except Exception as err:
            self._connection = None
            self._database = None
            raise DriverError(
                f"Connection Error, Terminated: {err}"
            ) from err

    async def close(self) -> None:
        """
        Closes the connection to the MongoDB server.

        Raises:
        -------
        DriverError
            If there is an issue closing the connection.
        """
        try:
            if self._connection:
                self._connection.close()
        except Exception as err:
            raise DriverError(f"Close Error: {err}") from err
        finally:
            self._connection = None
            self._database = None
            self._connected = False

    def is_connected(self):
        return self._connected

    async def test_connection(self, use_ping: bool = False) -> list:
        """
        Tests the connection by retrieving server information.

        Returns:
        --------
        list
            A list containing the server information and any error that occurred.
        """
        error = None
        result = None
        if self._connection:
            if use_ping:
                try:
                    result = await self._connection.admin.command("ping")
                    self._connected = True
                    return [result, error]
                except Exception as err:
                    error = err
            try:
                result = await self._connection.server_info()
                self._connected = True
            except Exception as err:
                error = err
            finally:
                return [result, error]
        else:
            error = DriverError("Not connected to MongoDB")
            return [None, error]

    async def prepare(self, *args, **kwargs) -> None:
        """
        Prepares a statement. MongoDB does not support prepared statements.

        Raises:
        -------
        DriverError
            Indicating that prepared statements are not supported.
        """
        raise DriverError("MongoDB does not support prepared statements.")

    async def use(self, database: str) -> motor.motor_asyncio.AsyncIOMotorDatabase:
        """
        Switches the current database to the specified one.

        Parameters:
        -----------
        database : str
            The name of the database to switch to.

        Returns:
        --------
        motor.motor_asyncio.AsyncIOMotorDatabase
            The selected database instance.

        Raises:
        -------
        DriverError
            If the connection is not established.
        """
        if not self._connection:
            raise DriverError(
                f"Not connected to MongoDB. Cannot switch to database '{database}'."
            )
        self._database = self._connection[database]
        self._database_name = database
        return self._database


    async def execute(
        self,
        collection_name: str,
        operation: str,
        *args,
        **kwargs
    ) -> Optional[Any]:
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
        try:
            db = await self._select_database()
            collection = db[collection_name]
            method = getattr(collection, operation)
            result = await method(*args, **kwargs)
        except Exception as err:
            error = err
        return (result, error)

    async def execute_many(
        self,
        collection_name: str,
        operation: str,
        documents: list
    ) -> Optional[Any]:
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
        return await self.execute(collection_name, operation, documents)

    executemany = execute_many

    async def update_many(
        self,
        collection_name,
        conditions: dict,
        operation: str = "$set",
        query: Optional[dict] = None
    ):
        if not query:
            query = {}
        db = await self._select_database()
        collection = db[collection_name]
        try:
            if self._dbtype == 'documentdb':
                # using Update simple:
                fields = {key: 1 for key in conditions}
                cursor = collection.find(query, fields)
                docs = await cursor.to_list(length=None)
                results = []
                for doc in docs:
                    fields = {}
                    for key, value in conditions.items():
                        if value == "$toDate":
                            fields[key] = pd.to_datetime(doc[key])
                    # Update the document to store actual datetime objects
                    result = await collection.update_one(
                        {"_id": doc["_id"]},
                        {
                            operation: fields
                        }
                    )
                    results.append(result)
                return results
            else:
                # Use update_many with an aggregation pipeline:
                fields = {key: {value: f"${key}"} for key, value in conditions.items()}
                print('FIELDS > ', fields)
                return await collection.update_many(
                    query,
                    [
                        {
                            operation: fields
                        }
                    ]
                )
        except Exception as err:
            raise DriverError(
                f"Error counting documents in '{collection_name}': {err}"
            ) from err

    async def __aenter__(self) -> "mongo":
        """
        Asynchronous context manager entry.
        Establishes a connection when entering the context.

        Returns:
        --------
        mongo
            Returns the instance of the driver itself.

        Raises:
        -------
        DriverError
            If an error occurs during connection establishment.
        """
        await self.connection()
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

    async def count_documents(self, collection_name: str, query: Optional[dict] = None):
        """
        Counts the number of documents in a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to count documents from.
        query : dict, optional
            The filter criteria for the count operation. Defaults to None.

        Returns:
        --------
        int
            The number of documents that match the query.
        """
        try:
            db = await self._select_database()
            collection = db[collection_name]
            return await collection.count_documents(query or {})
        except Exception as err:
            raise DriverError(
                f"Error counting documents in '{collection_name}': {err}"
            ) from err

    async def query(
        self,
        collection_name: str,
        query: Optional[dict] = None,
        *args,
        limit: Optional[int] = None,
        **kwargs
    ) -> Iterable[Any]:
        """
        Executes a query to retrieve documents from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        query : dict, optional
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
        try:
            db = await self._select_database()
            collection = db[collection_name]
            cursor = collection.find(query or {}, *args, **kwargs)
            if limit:
                cursor = cursor.limit(limit)
            result = await cursor.to_list(length=None)
            return await self._serializer(result, None)
        except Exception as err:
            return await self._serializer(None, err)

    async def queryrow(
        self,
        collection_name: str,
        query: Optional[dict] = None,
        *args,
        **kwargs
    ) -> Optional[dict]:
        """
        Executes a query to retrieve a single document from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        query : dict, optional
            The query criteria for the query. Defaults to None (no query).
        args : tuple
            Additional positional arguments to be passed to the query.
        kwargs : dict
            Additional keyword arguments to be passed to the query.

        Returns:
        --------
        Optional[dict]
            The document returned by the query, or None if no document matches.
        """
        try:
            db = await self._select_database()
            collection = db[collection_name]
            result = await collection.find_one(query or {}, *args, **kwargs)
            return await self._serializer(result, None)
        except Exception as err:
            return await self._serializer(None, err)

    async def fetch(
        self,
        collection_name: str,
        query: Optional[dict] = None,
        *args,
        limit: Optional[int] = None,
        **kwargs
    ) -> Iterable[Any]:
        """
        Executes a query to retrieve documents from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        query : dict, optional
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
        # return await self.query(collection_name, filter, *args, **kwargs)
        error = None
        result = []
        try:
            db = await self._select_database()
            collection = db[collection_name]
            cursor = collection.find(query or {}, *args, **kwargs)
            if limit:
                cursor = cursor.limit(limit)
            async for document in cursor:
                result.append(document)
            return (result, None)
        except Exception as err:
            return (None, err)

    fetch_all = fetch

    async def fetch_one(
        self,
        collection_name: str,
        query: Optional[dict] = None,
        *args,
        **kwargs
    ) -> Optional[dict]:
        """
        Executes a query to retrieve a single document from a collection asynchronously.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to query.
        query : dict, optional
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
        return await self.queryrow(collection_name, query, *args, **kwargs)

    fetchrow = fetch_one
    fetchone = fetch_one

    async def write(
        self,
        data: Union[Iterable[dict], pd.DataFrame, pa.Table, Any],
        collection: str = None,
        database: Optional[str] = None,
        use_pandas: bool = True,
        use_bulk: bool = False,
        if_exists: str = "append",
        **kwargs,
    ) -> bool:
        """
        Writes data to a collection asynchronously,
        supporting 'append' (insert) and 'replace' (upsert) operations.

        Parameters:
        -----------
        data : Iterable[dict] | pd.DataFrame | pa.Table | Any
            The data to be written, which can be any iterable of documents,
            pandas DataFrame, Arrow Table, or dataclass instances.
        collection : str, optional
            The name of the collection where the data will be written.
        database : str, optional
            The name of the database where the collection resides.
        use_pandas : bool, optional
            If True, uses pandas DataFrame or Arrow Table to process the data. Defaults to True.
        if_exists : str, optional
            Specifies what to do if the document already exists ('replace' or 'append').
            Defaults to 'append'.
        kwargs : dict
            Additional keyword arguments, e.g., `key_field` for upsert identification.

        Returns:
        --------
        bool
            Returns True if the write operation is successful.

        Raises:
        -------
        ValueError
            If invalid parameters are provided.
        DriverError
            If an error occurs during the write operation.
        """
        # Ensure database is selected
        if database:
            await self.use(database)
        try:
            db = await self._select_database()
        except DriverError as e:
            raise e

        if not collection:
            raise ValueError("No collection specified for write operation.")

        coll = db[collection]
        # Get key_field from kwargs or default to '_id'
        key_field = kwargs.get("key_field", "_id")
        # Process data based on type
        try:
            if use_pandas and isinstance(data, pd.DataFrame):
                documents = data.to_dict("records")
            elif use_pandas and isinstance(data, pa.Table):
                # documents = [dict(zip(data.schema.names, row)) for row in data.to_pydict().values()]
                table_dict = data.to_pydict()
                documents = [dict(zip(data.schema.names, row)) for row in zip(*table_dict.values())]
            elif is_dataclass(data):
                documents = [asdict(item) for item in data] if isinstance(data, Sequence) else asdict(data)
            elif isinstance(data, Iterable):
                documents = list(data)
            else:
                raise ValueError(
                    "Mongo: Data must be an iterable of dicts, pandas DataFrame, Arrow Table, or dataclass instances."
                )
        except Exception as e:
            raise DataError(f"Error processing input data: {e}") from e
        operations = []
        try:
            if if_exists == "append":
                if use_bulk:
                    return await coll.insert_many(documents)
                # Insert new documents without checking for existing ones
                operations = [pymongo.InsertOne(doc) for doc in documents]
            elif if_exists == "replace":
                if use_bulk:
                    # extract the keys from dataset
                    list_of_keys = (
                        [
                            row[field]
                            for row in documents
                            if field in row
                            for field in key_field
                        ]
                        if isinstance(key_field, list)
                        else [
                            row[key_field] for row in documents if key_field in row
                        ]
                    )
                    # Delete all documents from List of Keys:
                    await coll.delete_many({key_field: {"$in": list_of_keys}})
                    # Then Insert all new documents at once
                    return await coll.insert_many(documents)
                for doc in documents:
                    if key_field not in doc:
                        # If key_field is not in document, generate a unique identifier
                        doc[key_field] = ObjectId()
                    filter_condition = {key_field: doc[key_field]}
                    operations.append(pymongo.UpdateOne(filter_condition, {"$set": doc}, upsert=True))
            else:
                raise ValueError("Invalid value for if_exists: choose 'replace' or 'append'")
        except Exception as e:
            raise DataError(f"Error preparing bulk operations: {e}") from e

        # Execute bulk write
        try:
            if not operations:
                raise DataError("No operations to perform during write.")
            result = await coll.bulk_write(operations, ordered=False)
            self._logger.info(
                f"Write operation successful: {result.bulk_api_result}"
            )
            return result.bulk_api_result
        except Exception as e:
            raise DriverError(
                f"Error during write operation: {e}"
            ) from e

    async def truncate_table(self, collection_name: str) -> bool:
        """
        Truncates a collection by deleting all documents within it.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to truncate.

        Returns:
        --------
        bool
            Returns True if the truncation is successful.

        Raises:
        -------
        DriverError
            If there is an issue truncating the collection.
        """
        try:
            db = await self._select_database()
            collection = db[collection_name]
            result = await collection.delete_many({})
            self._logger.info(
                f"Truncated collection '{collection_name}': Deleted {result.deleted_count} documents."
            )
            return True
        except Exception as e:
            raise DriverError(
                f"Error truncating collection '{collection_name}': {e}"
            ) from e

    async def delete(
        self,
        collection_name: str,
        filter: Optional[dict] = None,
        many: bool = False
    ) -> int:
        """
        Deletes documents from a collection based on a filter condition.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to delete from.
        filter : dict, optional
            The filter criteria for deletion. Defaults to None (delete all documents).
        many : bool, optional
            If True, deletes multiple documents matching the filter.
            If False, deletes a single document matching the filter.
            Defaults to False.

        Returns:
        --------
        int
            The number of documents deleted.

        Raises:
        -------
        DriverError
            If there is an issue during the deletion process.
        """
        try:
            db = await self._select_database()
            collection = db[collection_name]
            if many:
                result = await collection.delete_many(filter or {})
                self._logger.info(
                    f"Deleted {result.deleted_count} documents from '{collection_name}' with filter {filter}."
                )
            else:
                result = await collection.delete_one(filter or {})
                self._logger.info(
                    f"Deleted {result.deleted_count} document from '{collection_name}' with filter {filter}."
                )
            return result.deleted_count
        except Exception as e:
            raise DriverError(
                f"Error deleting documents from '{collection_name}': {e}"
            ) from e

    async def drop_collection(self, collection_name: str) -> bool:
        """
        Drops a collection from the current database.

        Parameters:
        -----------
        collection_name : str
            The name of the collection to drop.

        Returns:
        --------
        bool
            True if the collection was successfully dropped, False otherwise.

        Raises:
        -------
        DriverError
            If there is an issue dropping the collection.
        """
        try:
            db = await self._select_database()
            result = await db.drop_collection(collection_name)
            self._logger.info(f"Dropped collection '{collection_name}': {result}")
            return True
        except Exception as e:
            raise DriverError(
                f"Error dropping collection '{collection_name}': {e}"
            ) from e


    async def create_database(self, database: str) -> None:
        """
        Creates a new database in the MongoDB server.

        Parameters:
        -----------
        database : str
            The name of the database to create.
        """
        try:
            if not self._connection:
                raise DriverError("Not connected to MongoDB.")
            self._database = self._connection[database]
            self._database_name = database
            self._logger.info(f"Created database '{database}'.")
            return self._database
        except Exception as e:
            raise DriverError(
                f"Error creating database '{database}': {e}"
            ) from e

    async def drop_database(self, database: str) -> bool:
        """
        Drops a database from the MongoDB server.

        Parameters:
        -----------
        database : str
            The name of the database to drop.

        Returns:
        --------
        bool
            True if the database was successfully dropped, False otherwise.

        Raises:
        -------
        DriverError
            If there is an issue dropping the database.
        """
        try:
            if not self._connection:
                raise DriverError("Not connected to MongoDB.")
            result = await self._connection.drop_database(database)
            self._logger.info(f"Dropped database '{database}': {result}")
            return True
        except Exception as e:
            raise DriverError(
                f"Error dropping database '{database}': {e}"
            ) from e

    async def create_collection(
        self,
        database: str,
        collection: str,
        unique_index: bool = False,
        pk: str = None
    ):
        """Create a new collection in the database and optionally set a unique index."""
        try:
            db = self._connection[database]
            try:
                await db.create_collection("my_new_collection")
            except Exception as e:
                self._logger.error(f"Error creating collection '{collection}': {e}")
                return False
            if unique_index:
                coll = db[collection]
                # await coll.create_index(pk, unique=True)
                await coll.create_index([(pk, 1)], unique=True)
            self._logger.info(
                f"Created collection '{collection}' in database '{database}'."
            )
            return True
        except Exception as e:
            raise DriverError(
                f"Error creating collection '{collection}' in database '{database}': {e}"
            ) from e
