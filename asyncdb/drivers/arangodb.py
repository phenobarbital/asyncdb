"""
ArangoDB Driver for AsyncDB
Supports native graph operations, RAG, and multi-model database features.
"""
from pathlib import Path, PurePath
from typing import Any, Optional, Union, List, Dict
import asyncio
import time
import logging
from dataclasses import is_dataclass, astuple, fields as dataclass_fields
import pandas as pd
from arango import ArangoClient
from arango.database import StandardDatabase
from arango.collection import StandardCollection
from arango.graph import Graph
from arango.cursor import Cursor
from arango.exceptions import (
    DatabaseCreateError,
    DatabaseDeleteError,
    CollectionCreateError,
    DocumentInsertError,
    DocumentDeleteError,
    DocumentUpdateError,
    AQLQueryExecuteError,
    GraphCreateError,
)
from .base import InitDriver
from ..exceptions import NoDataFound, DriverError
from ..interfaces.model import ModelBackend


logging.getLogger("arango").setLevel(logging.INFO)


class arangodb(InitDriver, ModelBackend):
    """
    ArangoDB Driver with Graph and RAG support.

    Supports:
    - Document operations
    - Graph operations (vertices and edges)
    - AQL queries
    - Graph traversals
    - Vector search for RAG
    - Pandas DataFrame integration
    """

    _provider = "arangodb"
    _syntax = "aql"  # ArangoDB Query Language
    _dsn_template: str = 'http://{host}:{port}'

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop = None,
        params: dict = None,
        **kwargs
    ):
        self._client: ArangoClient = None
        self._connection: StandardDatabase = None
        self._timeout: int = kwargs.pop("timeout", 60)
        self._test_query = "RETURN 1"
        self._default_graph = kwargs.pop("default_graph", None)
        self._batch_size = kwargs.pop("batch_size", 1000)

        super(arangodb, self).__init__(loop=loop, params=params, **kwargs)

        # Connection parameters
        try:
            self._host = self.params.get("host", "127.0.0.1")
            self._port = self.params.get("port", 8529)
            self._protocol = self.params.get("protocol", "http")
        except (KeyError, AttributeError):
            self._host = "127.0.0.1"
            self._port = 8529
            self._protocol = "http"

        # Authentication
        try:
            self._username = self.params.get("username", "root")
            self._password = self.params.get("password", "")
        except (KeyError, AttributeError):
            self._username = "root"
            self._password = ""

        # Database name
        self._database_name = self.params.get("database", "_system")

    async def connection(self, database: str = None):
        """
        Establish connection to ArangoDB.
        """
        self._connection = None
        self._connected = False

        if database:
            self._database_name = database

        try:
            # Create ArangoDB client
            url = f"{self._protocol}://{self._host}:{self._port}"
            self._client = ArangoClient(hosts=url)

            # Connect to system database first
            sys_db = self._client.db(
                '_system',
                username=self._username,
                password=self._password
            )

            # Check if database exists, create if needed
            if self._database_name not in sys_db.databases():
                self._logger.debug(f"Creating database: {self._database_name}")
                sys_db.create_database(self._database_name)

            # Connect to target database
            self._connection =self._client.db(
                self._database_name,
                username=self._username,
                password=self._password
            )
            self._connected = True
            self._initialized_on = time.time()

            self._logger.info(f"Connected to ArangoDB: {self._database_name}")
            return self

        except Exception as err:
            self._logger.exception(f"ArangoDB Connection Error: {err}")
            raise DriverError(
                message=f"ArangoDB Connection Error: {err}"
            ) from err

    connect = connection

    async def close(self):
        """
        Close ArangoDB connection.
        """
        try:
            if self._client:
                self._client.close()
        except Exception as err:
            raise DriverError(
                message=f"Error closing ArangoDB connection: {err}"
            ) from err
        finally:
            self._client = None
            self._connection =None
            self._connection = None
            self._connected = False

    async def test_connection(self):
        """
        Test the ArangoDB connection.
        """
        result = None
        error = None
        try:
            result, error = await self.query(self._test_query)
        except Exception as err:
            error = err
        finally:
            return [result, error]

    async def use(self, database: str):
        """
        Switch to a different database.
        """
        try:
            self._connection =self._client.db(
                database,
                username=self._username,
                password=self._password
            )
            self._database_name = database
            self._logger.debug(f"Switched to database: {database}")
            return self
        except Exception as err:
            self._logger.error(f"Error switching database: {err}")
            raise DriverError(f"Error switching database: {err}") from err

    # Database Operations

    async def create_database(self, database: str):
        """
        Create a new database.
        """
        try:
            sys_db = self._client.db(
                '_system',
                username=self._username,
                password=self._password
            )
            sys_db.create_database(database)
            self._logger.debug(f"Database created: {database}")
            return True
        except DatabaseCreateError as err:
            raise DriverError(f"Error creating database: {err}") from err

    async def drop_database(self, database: str):
        """
        Drop a database.
        """
        try:
            sys_db = self._client.db(
                '_system',
                username=self._username,
                password=self._password
            )
            sys_db.delete_database(database)
            self._logger.debug(f"Database dropped: {database}")
            return True
        except DatabaseDeleteError as err:
            raise DriverError(f"Error dropping database: {err}") from err

    # Collection Operations

    async def create_collection(
        self,
        name: str,
        edge: bool = False,
        **kwargs
    ):
        """
        Create a collection (document or edge).

        Args:
            name: Collection name
            edge: If True, create edge collection
            **kwargs: Additional collection properties
        """
        try:
            if self._connection.has_collection(name):
                self._logger.warning(f"Collection {name} already exists")
                return self._connection.collection(name)

            collection = self._connection.create_collection(
                name,
                edge=edge,
                **kwargs
            )
            self._logger.debug(
                f"Collection created: {name} (edge={edge})"
            )
            return collection
        except CollectionCreateError as err:
            raise DriverError(
                f"Error creating collection {name}: {err}"
            ) from err

    async def drop_collection(self, name: str):
        """
        Drop a collection.
        """
        try:
            self._connection.delete_collection(name)
            self._logger.debug(f"Collection dropped: {name}")
            return True
        except Exception as err:
            raise DriverError(
                f"Error dropping collection {name}: {err}"
            ) from err

    async def collection_exists(self, name: str) -> bool:
        """
        Check if collection exists.
        """
        return self._connection.has_collection(name)

    # Graph Operations

    async def create_graph(
        self,
        name: str,
        edge_definitions: List[Dict] = None,
        orphan_collections: List[str] = None
    ):
        """
        Create a named graph.

        Args:
            name: Graph name
            edge_definitions: List of edge definitions
                [{'edge_collection': 'edges',
                  'from_vertex_collections': ['vertices'],
                  'to_vertex_collections': ['vertices']}]
            orphan_collections: Vertex collections without edges
        """
        try:
            if self._connection.has_graph(name):
                self._logger.warning(f"Graph {name} already exists")
                return self._connection.graph(name)

            graph = self._connection.create_graph(
                name,
                edge_definitions=edge_definitions or [],
                orphan_collections=orphan_collections or []
            )
            self._logger.debug(f"Graph created: {name}")
            return graph
        except GraphCreateError as err:
            raise DriverError(f"Error creating graph {name}: {err}") from err

    async def drop_graph(self, name: str, drop_collections: bool = False):
        """
        Drop a graph.

        Args:
            name: Graph name
            drop_collections: If True, also drop associated collections
        """
        try:
            self._connection.delete_graph(
                name,
                drop_collections=drop_collections
            )
            self._logger.debug(f"Graph dropped: {name}")
            return True
        except Exception as err:
            raise DriverError(f"Error dropping graph {name}: {err}") from err

    async def graph_exists(self, name: str) -> bool:
        """
        Check if graph exists.
        """
        return self._connection.has_graph(name)

    # Query Operations

    async def query(
        self,
        sentence: str,
        bind_vars: dict = None,
        **kwargs
    ) -> Any:
        """
        Execute an AQL query.

        Args:
            sentence: AQL query string
            bind_vars: Query bind variables
            **kwargs: Additional query options

        Returns:
            Tuple of (result, error)
        """
        error = None
        self._result = None

        try:
            await self.valid_operation(sentence)
            self.start_timing()

            cursor: Cursor = self._connection.aql.execute(
                sentence,
                bind_vars=bind_vars or {},
                **kwargs
            )

            # Convert cursor to list
            self._result = list(cursor)

            if not self._result:
                raise NoDataFound("ArangoDB: No Data Found")

        except NoDataFound:
            raise
        except AQLQueryExecuteError as err:
            error = f"AQL Query Error: {err}"
        except Exception as err:
            error = f"Error on Query: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)

    async def queryrow(
        self,
        sentence: str,
        bind_vars: dict = None
    ) -> Any:
        """
        Execute AQL query and return single row.

        Returns:
            Tuple of (result, error)
        """
        error = None
        self._result = None

        try:
            await self.valid_operation(sentence)

            cursor = self._connection.aql.execute(
                sentence,
                bind_vars=bind_vars or {}
            )

            self._result = next(cursor, None)

            if not self._result:
                raise NoDataFound("ArangoDB: No Data Found")

        except NoDataFound:
            raise
        except Exception as err:
            error = f"Error on Query Row: {err}"

        return [self._result, error]

    async def fetch_all(
        self,
        sentence: str,
        bind_vars: dict = None
    ) -> List:
        """
        Fetch all results from query (native, no error handling).
        """
        await self.valid_operation(sentence)

        try:
            cursor = self._connection.aql.execute(
                sentence,
                bind_vars=bind_vars or {}
            )
            if result := list(cursor):
                return result

            raise NoDataFound("ArangoDB: No Data Found")

        except NoDataFound:
            raise
        except Exception as err:
            raise DriverError(f"Error on Fetch All: {err}") from err

    fetchall = fetch_all

    async def fetch_one(
        self,
        sentence: str,
        bind_vars: dict = None
    ) -> Optional[dict]:
        """
        Fetch one result from query (native, no error handling).
        """
        await self.valid_operation(sentence)

        try:
            cursor = self._connection.aql.execute(
                sentence,
                bind_vars=bind_vars or {}
            )
            if result := next(cursor, None):
                return result

            raise NoDataFound("ArangoDB: No Data Found")

        except NoDataFound:
            raise
        except Exception as err:
            raise DriverError(f"Error on Fetch One: {err}") from err

    fetchone = fetch_one
    fetchrow = fetch_one

    async def fetchval(
        self,
        sentence: str,
        bind_vars: dict = None,
        column: int = 0
    ) -> Any:
        """
        Fetch a single value from query result.

        Args:
            sentence: AQL query
            bind_vars: Query parameters
            column: Column index or key to extract
        """
        row = await self.fetch_one(sentence, bind_vars)

        if row is None:
            return None

        if isinstance(row, dict):
            if isinstance(column, int):
                keys = list(row.keys())
                return row[keys[column]] if column < len(keys) else None
            return row.get(column)
        elif isinstance(row, (list, tuple)):
            if column < len(row):
                return row[column]

        return None

    async def execute(
        self,
        sentence: str,
        bind_vars: dict = None
    ) -> Any:
        """
        Execute an AQL statement (INSERT, UPDATE, DELETE).

        Returns:
            Tuple of (result, error)
        """
        error = None
        result = None

        try:
            await self.valid_operation(sentence)

            cursor = self._connection.aql.execute(
                sentence,
                bind_vars=bind_vars or {}
            )

            result = list(cursor)

        except Exception as err:
            error = f"Error on Execute: {err}"

        return [result, error]

    # Document Operations

    async def insert_document(
        self,
        collection: str,
        document: dict,
        return_new: bool = True
    ) -> dict:
        """
        Insert a document into collection.

        Args:
            collection: Collection name
            document: Document to insert
            return_new: Return the inserted document
        """
        try:
            col = self._connection.collection(collection)
            result = col.insert(document, return_new=return_new)

            return result['new'] if return_new else result

        except DocumentInsertError as err:
            raise DriverError(
                f"Error inserting document: {err}"
            ) from err

    async def update_document(
        self,
        collection: str,
        document: dict,
        return_new: bool = True
    ) -> dict:
        """
        Update a document.

        Args:
            collection: Collection name
            document: Document with _key or _id
            return_new: Return updated document
        """
        try:
            col = self._connection.collection(collection)
            result = col.update(document, return_new=return_new)

            return result['new'] if return_new else result

        except DocumentUpdateError as err:
            raise DriverError(
                f"Error updating document: {err}"
            ) from err

    async def delete_document(
        self,
        collection: str,
        document_key: str
    ) -> bool:
        """
        Delete a document.

        Args:
            collection: Collection name
            document_key: Document _key or _id
        """
        try:
            col = self._connection.collection(collection)
            col.delete(document_key)
            return True

        except DocumentDeleteError as err:
            raise DriverError(
                f"Error deleting document: {err}"
            ) from err

    # Write Operations

    async def write(
        self,
        data: Union[list, dict, pd.DataFrame, Any],
        collection: str,
        **kwargs
    ):
        """
        Write data into ArangoDB collection.

        Supports:
        - dict: Single document
        - list of dicts: Multiple documents
        - pandas DataFrame: Batch insert
        - Path: CSV file
        - dataclass instances
        """
        try:
            col = self._connection.collection(collection)
        except Exception:
            # Collection doesn't exist, create it
            col = await self.create_collection(collection)

        _data = None

        # Handle different input types
        if isinstance(data, PurePath):
            # Load from CSV
            if not data.exists():
                raise ValueError(f"File {data} does not exist")

            df = pd.read_csv(data)
            _data = df.to_dict('records')

        elif isinstance(data, pd.DataFrame):
            _data = data.to_dict('records')

        elif is_dataclass(data):
            _data = [astuple(data)]
            field_names = [f.name for f in dataclass_fields(data)]
            _data = [dict(zip(field_names, _data[0]))]

        elif isinstance(data, dict):
            _data = [data]

        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                _data = data
            else:
                raise ValueError("List must contain dictionaries")
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        # Batch insert
        batch_size = kwargs.get('batch_size', self._batch_size)
        inserted = 0

        for i in range(0, len(_data), batch_size):
            batch = _data[i:i + batch_size]
            col.insert_many(batch)
            inserted += len(batch)

        self._logger.debug(
            f"Inserted {inserted} documents into {collection}"
        )
        return inserted

    # Graph-specific Operations for RAG

    async def create_vertex(
        self,
        graph: str,
        collection: str,
        vertex: dict
    ) -> dict:
        """
        Create a vertex in a graph.
        """
        try:
            g = self._connection.graph(graph)
            vertex_col = g.vertex_collection(collection)
            return vertex_col.insert(vertex)
        except Exception as err:
            raise DriverError(f"Error creating vertex: {err}") from err

    async def create_edge(
        self,
        graph: str,
        collection: str,
        edge: dict
    ) -> dict:
        """
        Create an edge in a graph.

        edge must contain _from and _to fields.
        """
        try:
            g = self._connection.graph(graph)
            edge_col = g.edge_collection(collection)
            return edge_col.insert(edge)
        except Exception as err:
            raise DriverError(f"Error creating edge: {err}") from err

    async def traverse(
        self,
        start_vertex: str,
        direction: str = "outbound",
        min_depth: int = 1,
        max_depth: int = 1,
        edge_collection: str = None,
        **kwargs
    ) -> List[dict]:
        """
        Perform graph traversal for RAG.

        Args:
            start_vertex: Starting vertex ID
            direction: 'outbound', 'inbound', or 'any'
            min_depth: Minimum traversal depth
            max_depth: Maximum traversal depth
            edge_collection: Edge collection to traverse
        """
        direction_map = {
            'outbound': 'OUTBOUND',
            'inbound': 'INBOUND',
            'any': 'ANY'
        }

        dir_clause = direction_map.get(direction.lower(), 'OUTBOUND')
        edge_clause = edge_collection or ""

        query = f"""
        FOR v, e, p IN {min_depth}..{max_depth}
            {dir_clause} @start_vertex
            {edge_clause}
            RETURN {{vertex: v, edge: e, path: p}}
        """

        bind_vars = {'start_vertex': start_vertex}
        result, error = await self.query(query, bind_vars=bind_vars)

        if error:
            raise DriverError(f"Traversal error: {error}")

        return result

    async def shortest_path(
        self,
        start_vertex: str,
        end_vertex: str,
        edge_collection: str = None,
        direction: str = "outbound"
    ) -> Optional[dict]:
        """
        Find shortest path between two vertices.
        """
        direction_map = {
            'outbound': 'OUTBOUND',
            'inbound': 'INBOUND',
            'any': 'ANY'
        }

        dir_clause = direction_map.get(direction.lower(), 'ANY')
        edge_clause = edge_collection or ""

        query = f"""
        FOR v, e IN {dir_clause}
            SHORTEST_PATH @start TO @end
            {edge_clause}
            RETURN {{vertices: v, edges: e}}
        """

        bind_vars = {
            'start': start_vertex,
            'end': end_vertex
        }

        result, error = await self.queryrow(query, bind_vars=bind_vars)

        if error:
            raise DriverError(f"Shortest path error: {error}")

        return result

    # Utility methods

    async def create(
        self,
        obj: str = "collection",
        name: str = "",
        **kwargs
    ) -> bool:
        """
        Generic create method for database objects.

        Supports: collection, graph, database
        """
        if obj == "collection":
            edge = kwargs.get('edge', False)
            await self.create_collection(name, edge=edge, **kwargs)
            return True

        elif obj == "graph":
            edge_definitions = kwargs.get('edge_definitions', [])
            orphan_collections = kwargs.get('orphan_collections', [])
            await self.create_graph(
                name,
                edge_definitions=edge_definitions,
                orphan_collections=orphan_collections
            )
            return True

        elif obj == "database":
            await self.create_database(name)
            return True

        else:
            raise ValueError(f"Unknown object type: {obj}")

    async def delete(
        self,
        obj: str = "collection",
        name: str = "",
        **kwargs
    ) -> bool:
        """
        Generic delete method for database objects.
        """
        if obj == "collection":
            await self.drop_collection(name)
            return True

        elif obj == "graph":
            drop_collections = kwargs.get('drop_collections', False)
            await self.drop_graph(name, drop_collections=drop_collections)
            return True

        elif obj == "database":
            await self.drop_database(name)
            return True

        else:
            raise ValueError(f"Unknown object type: {obj}")

    async def create_vector_index(self, collection: str, field: str = 'embedding'):
        """Create ArangoSearch view for vector similarity"""
        return self._connection.create_view(
            f'{collection}_search',
            'arangosearch',
            properties={
                'links': {
                    collection: {
                        'fields': {
                            field: {'analyzers': ['identity']}
                        }
                    }
                }
            }
        )
