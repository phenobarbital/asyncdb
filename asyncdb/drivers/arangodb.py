"""
ArangoDB Driver for AsyncDB
Supports native graph operations, RAG, and multi-model database features.
"""
from pathlib import Path, PurePath
from typing import Any, Optional, Union, List, Dict, Sequence, Tuple, cast
from collections import defaultdict
import asyncio
import time
import logging
from dataclasses import is_dataclass, astuple, fields as dataclass_fields
import numpy as np
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


logging.getLogger("arango").setLevel(logging.INFO)

AQLJob = Union[str, Tuple[str, Dict[str, Any]]]


class arangodb(InitDriver):
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
            self._connection = self._client.db(
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

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError()  # pragma: no-cover

    async def use(self, database: str):
        """
        Switch to a different database.
        """
        try:
            self._connection = self._client.db(
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
        except Exception as err:
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
        except Exception as err:
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
            if not self._connected or self._connection is None:
                raise AttributeError(
                    "Not connected to database"
                )
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

        except (ValueError, AttributeError):
            raise
        except NoDataFound as e:
            error = f"{e}"
        except AQLQueryExecuteError as err:
            error = f"AQL Query Error: {err}"
        except Exception as err:
            error = f"Error on Query: {err}"

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

            results = list(cursor)
            self._result = results[0] if results else None

            if not self._result:
                raise NoDataFound("ArangoDB: No Data Found")

        except NoDataFound:
            raise
        except Exception as err:
            error = f"Error on Query Row: {err}"

        return await self._serializer(self._result, error)

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
        bind_vars: dict = None,
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
            if results := list(cursor):
                return results[0]

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

        # If row is already a scalar value (not dict/list/tuple), return it directly
        if not isinstance(row, (dict, list, tuple)):
            return row

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

    async def execute_many(
        self,
        sentences: Sequence[AQLJob],
        *,
        max_concurrency: int = 8,
        retry: int = 0,
        retry_backoff: float = 0.5,
        timeout: Optional[float] = None
    ) -> List[Tuple[Optional[List[Any]], Optional[str]]]:
        """
        Execute many AQL statements in parallel with order preserved.

        Args:
            sentences: Either a list of strings, or a list of (sentence, bind_vars) tuples.
            max_concurrency: Upper bound of parallel in-flight requests.
            retry: Number of retries on failure per sentence.
            retry_backoff: Base seconds for exponential backoff (sleep = backoff * 2**attempt).
            timeout: Optional per-task timeout in seconds.

        Returns:
            For each input, a tuple (result, error) where result is a list or None, error is str or None.
            Order matches the input order.
        """
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(idx: int, job: AQLJob):
            sentence, bind = job if isinstance(job, tuple) else (job, None)

            # Simple retry loop
            attempt = 0
            err = None
            while True:
                try:
                    async with sem:
                        if timeout:
                            return idx, await asyncio.wait_for(self.execute(sentence, bind), timeout=timeout)
                        else:
                            return idx, await self.execute(sentence, bind)
                except asyncio.TimeoutError:
                    err = f"Timeout on Execute (>{timeout}s)"
                except Exception as e:
                    err = f"Execute failed: {e}"

                if attempt >= retry:
                    return idx, [None, err]
                await asyncio.sleep(retry_backoff * (2 ** attempt))
                attempt += 1

        tasks = [asyncio.create_task(_one(i, job)) for i, job in enumerate(sentences)]
        results: List[Tuple[Optional[List[Any]], Optional[str]]] = [ (None, "not started") ] * len(sentences)
        for coro in asyncio.as_completed(tasks):
            idx, pair = await coro
            results[idx] = cast(Tuple[Optional[List[Any]], Optional[str]], tuple(pair))
        return results

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

        except Exception as err:
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
        graph_name: str = None,
        **kwargs
    ) -> List[dict]:
        """
        Perform graph traversal for RAG.

        Args:
            start_vertex: Starting vertex ID
            direction: 'outbound', 'inbound', or 'any'
            min_depth: Minimum traversal depth
            max_depth: Maximum traversal depth
            edge_collection: Edge collection to traverse (alternative to graph_name)
            graph_name: Graph name to traverse (alternative to edge_collection)
        """
        direction_map = {
            'outbound': 'OUTBOUND',
            'inbound': 'INBOUND',
            'any': 'ANY'
        }

        dir_clause = direction_map.get(direction.lower(), 'OUTBOUND')

        # Determine what to use for traversal
        if graph_name:
            traversal_clause = f"GRAPH '{graph_name}'"
        elif edge_collection:
            traversal_clause = edge_collection
        elif self._default_graph:
            traversal_clause = f"GRAPH '{self._default_graph}'"
        else:
            raise ValueError(
                "Must specify either graph_name, edge_collection, or set default_graph"
            )

        query = f"""
        FOR v, e, p IN {min_depth}..{max_depth}
            {dir_clause} @start_vertex
            {traversal_clause}
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
        direction: str = "outbound",
        graph_name: str = None
    ) -> List[dict]:
        """
        Find shortest path between two vertices.
        """
        direction_map = {
            'outbound': 'OUTBOUND',
            'inbound': 'INBOUND',
            'any': 'ANY'
        }

        dir_clause = direction_map.get(direction.lower(), 'ANY')

        # Determine what to use for traversal
        if graph_name:
            traversal_clause = f"GRAPH '{graph_name}'"
        elif edge_collection:
            traversal_clause = edge_collection
        elif self._default_graph:
            traversal_clause = f"GRAPH '{self._default_graph}'"
        else:
            raise ValueError(
                "Must specify either graph_name, edge_collection, or set default_graph"
            )

        query = f"""
        FOR v, e IN {dir_clause}
            SHORTEST_PATH @start TO @end
            {traversal_clause}
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


    async def create_knowledge_graph(
        self,
        graph_name: str,
        vertex_collections: List[str],
        edge_collection: str = "relationships"
    ):
        """
        Create a knowledge graph structure.

        Args:
            graph_name: Name of the graph
            vertex_collections: List of vertex collection names
                (e.g., ['entities', 'documents', 'concepts'])
            edge_collection: Name for edge collection
        """
        # Create vertex collections
        for collection in vertex_collections:
            if not await self.collection_exists(collection):
                await self.create_collection(collection, edge=False)

        # Create edge collection
        if not await self.collection_exists(edge_collection):
            await self.create_collection(edge_collection, edge=True)

        # Define edge definitions
        edge_definitions = [{
            'edge_collection': edge_collection,
            'from_vertex_collections': vertex_collections,
            'to_vertex_collections': vertex_collections
        }]

        # Create graph
        await self.create_graph(
            graph_name,
            edge_definitions=edge_definitions
        )

        self._logger.info(f"Knowledge graph '{graph_name}' created")
        return graph_name

    async def add_document_node(
        self,
        collection: str,
        doc_id: str,
        content: str,
        metadata: Dict = None,
        embedding: List[float] = None
    ) -> Dict:
        """
        Add a document node to the graph.

        Args:
            collection: Collection name
            doc_id: Document identifier
            content: Document content/text
            metadata: Additional metadata
            embedding: Vector embedding for semantic search
        """
        document = {
            '_key': doc_id,
            'content': content,
            'type': 'document',
            'metadata': metadata or {}
        }

        if embedding:
            document['embedding'] = embedding

        return await self.insert_document(collection, document)

    async def add_entity_node(
        self,
        collection: str,
        entity_id: str,
        entity_type: str,
        properties: Dict = None,
        embedding: List[float] = None
    ) -> Dict:
        """
        Add an entity node to the graph.

        Args:
            collection: Collection name
            entity_id: Entity identifier
            entity_type: Type of entity (person, organization, concept, etc.)
            properties: Entity properties
            embedding: Vector embedding
        """
        entity = {
            '_key': entity_id,
            'entity_type': entity_type,
            'type': 'entity',
            'properties': properties or {}
        }

        if embedding:
            entity['embedding'] = embedding

        return await self.insert_document(collection, entity)

    async def add_relationship(
        self,
        edge_collection: str,
        from_id: str,
        to_id: str,
        relation_type: str,
        properties: Dict = None,
        weight: float = 1.0
    ) -> Dict:
        """
        Add a relationship between two nodes.

        Args:
            edge_collection: Edge collection name
            from_id: Source node ID (format: collection/key)
            to_id: Target node ID (format: collection/key)
            relation_type: Type of relationship
            properties: Additional properties
            weight: Relationship weight/strength
        """
        edge = {
            '_from': from_id,
            '_to': to_id,
            'relation_type': relation_type,
            'weight': weight,
            'properties': properties or {}
        }

        return await self.insert_document(edge_collection, edge)

    async def find_related_nodes(
        self,
        node_id: str,
        relation_types: List[str] = None,
        max_depth: int = 2,
        limit: int = 10,
        graph_name: str = None
    ) -> List[Dict]:
        """
        Find nodes related to a given node.
        """
        filter_clause = ""
        if relation_types:
            types_str = ", ".join([f"'{t}'" for t in relation_types])
            filter_clause = f"FILTER e.relation_type IN [{types_str}]"

        # Use provided graph_name or default
        graph = graph_name or self._default_graph
        if not graph:
            raise ValueError("Must specify graph_name or set default_graph")

        query = f"""
        FOR v, e, p IN 1..@max_depth
            ANY @start_node
            GRAPH '{graph}'
            {filter_clause}
            LIMIT @limit
            RETURN {{
                node: v,
                relationship: e,
                depth: LENGTH(p.vertices) - 1,
                path: p.vertices[*]._key
            }}
        """

        bind_vars = {
            'start_node': node_id,
            'max_depth': max_depth,
            'limit': limit
        }

        result, error = await self.query(query, bind_vars=bind_vars)

        if error:
            self._logger.error(f"Error finding related nodes: {error}")
            return []

        return result

    async def semantic_search_with_context(
        self,
        query_embedding: List[float],
        collection: str,
        top_k: int = 5,
        include_neighbors: bool = True,
        neighbor_depth: int = 1
    ) -> List[Dict]:
        """
        Perform semantic search and include graph context.

        Args:
            query_embedding: Query vector
            collection: Collection to search
            top_k: Number of top results
            include_neighbors: Include neighboring nodes
            neighbor_depth: Depth of neighbors to include

        Note: This uses a simplified cosine similarity calculation.
        For production, consider using ArangoSearch with vector indexes.
        """
        # Note: This requires ArangoSearch or vector index
        # For now, using a simplified AQL approach

        query = f"""
FOR doc IN {collection}
    FILTER doc.embedding != null
    LET dotProduct = SUM(
        FOR i IN 0..LENGTH(doc.embedding)-1
            RETURN doc.embedding[i] * @query_vector[i]
    )
    LET docMagnitude = SQRT(SUM(
        FOR val IN doc.embedding
            RETURN val * val
    ))
    LET queryMagnitude = SQRT(SUM(
        FOR val IN @query_vector
            RETURN val * val
    ))
    LET similarity = dotProduct / (docMagnitude * queryMagnitude)
    FILTER similarity > 0
    SORT similarity DESC
    LIMIT @top_k
    RETURN {{
        document: doc,
        similarity: similarity
    }}
        """

        bind_vars = {
            'query_vector': query_embedding,
            'top_k': top_k
        }

        results, error = await self.query(query, bind_vars=bind_vars)

        if error:
            self._logger.error(f"Semantic search error: {error}")
            return []

        # Add graph context if requested
        if include_neighbors and results:
            enriched_results = []
            for result in results:
                doc_id = result['document']['_id']
                neighbors = await self.find_related_nodes(
                    doc_id,
                    max_depth=neighbor_depth
                )
                result['graph_context'] = neighbors
                enriched_results.append(result)
            return enriched_results

        return results

    async def get_subgraph(
        self,
        node_ids: List[str],
        max_depth: int = 2,
        edge_types: List[str] = None,
        graph_name: str = None
    ) -> Dict[str, Any]:
        """
        Extract a subgraph around given nodes.
        """
        graph = graph_name or self._default_graph
        if not graph:
            raise ValueError("Must specify graph_name or set default_graph")

        filter_clause = ""
        if edge_types:
            types_str = ", ".join([f"'{t}'" for t in edge_types])
            filter_clause = f"FILTER e.relation_type IN [{types_str}]"

        # Collect all results in a single query
        query = f"""
        LET allResults = (
            FOR node_id IN @node_ids
                FOR v, e IN 0..@max_depth
                    ANY node_id
                    GRAPH '{graph}'
                    {filter_clause}
                    RETURN {{vertex: v, edge: e}}
        )
        FOR item IN allResults
            RETURN DISTINCT item
        """

        bind_vars = {
            'node_ids': node_ids,
            'max_depth': max_depth
        }

        results, error = await self.query(query, bind_vars=bind_vars)

        if error:
            self._logger.error(f"Subgraph extraction error: {error}")
            return {'nodes': [], 'edges': []}

        # Separate nodes and edges
        nodes = []
        edges = []

        for item in results:
            if item.get('vertex'):
                nodes.append(item['vertex'])
            if item.get('edge') and item['edge']:  # Check edge is not None
                edges.append(item['edge'])

        return {
            'nodes': nodes,
            'edges': edges
        }

    async def community_detection(
        self,
        collection: str,
        algorithm: str = "label_propagation",
        **kwargs
    ) -> Dict[str, List[str]]:
        """
        Detect communities in the graph.

        Args:
            collection: Vertex collection
            algorithm: Algorithm to use (label_propagation, louvain, etc.)
            **kwargs: Algorithm-specific parameters

        Returns:
            Dict mapping community_id to list of node_ids
        """
        # Simplified label propagation
        # For production, consider using ArangoDB's graph algorithms

        query = f"""
        FOR doc IN {collection}
            RETURN doc._id
        """

        nodes, error = await self.query(query)

        if error:
            self._logger.error(f"Community detection error: {error}")
            return {}

        # Placeholder - implement actual algorithm
        # You might want to use NetworkX or igraph for complex algorithms
        communities = defaultdict(list)

        # Simple example: group by a property
        for node_id in nodes:
            # Get node data
            node_data = await self.fetch_one(
                f"FOR doc IN {collection} FILTER doc._id == @id RETURN doc",
                bind_vars={'id': node_id}
            )

            # Group by some property (customize as needed)
            community_key = node_data.get('entity_type', 'unknown')
            communities[community_key].append(node_id)

        return dict(communities)

    async def centrality_analysis(
        self,
        collection: str,
        metric: str = "degree",
        graph_name: str = None
    ) -> List[Tuple[str, float]]:
        """
        Calculate node centrality metrics.
        """
        if metric != "degree":
            raise NotImplementedError(
                f"Centrality metric '{metric}' not implemented"
            )

        graph = graph_name or self._default_graph
        if not graph:
            raise ValueError("Must specify graph_name or set default_graph")

        query = f"""
        FOR doc IN {collection}
            LET inCount = LENGTH(
                FOR v IN 1..1 INBOUND doc._id
                GRAPH '{graph}'
                RETURN v
            )
            LET outCount = LENGTH(
                FOR v IN 1..1 OUTBOUND doc._id
                GRAPH '{graph}'
                RETURN v
            )
            RETURN {{
                node_id: doc._id,
                degree: inCount + outCount
            }}
        """

        results, error = await self.query(query)

        if error:
            self._logger.error(f"Centrality analysis error: {error}")
            return []

        return [(r['node_id'], r['degree']) for r in results]

    async def merge_duplicate_nodes(
        self,
        collection: str,
        similarity_threshold: float = 0.9,
        merge_strategy: str = "keep_first"
    ) -> int:
        """
        Merge duplicate nodes based on similarity.

        Args:
            collection: Collection to process
            similarity_threshold: Threshold for considering nodes duplicates
            merge_strategy: How to merge (keep_first, merge_properties, etc.)

        Returns:
            Number of nodes merged
        """
        # Get all nodes with embeddings
        query = f"""
        FOR doc IN {collection}
            FILTER doc.embedding != null
            RETURN doc
        """

        nodes, error = await self.query(query)

        if error:
            self._logger.error(f"Error getting nodes for merge: {error}")
            return 0

        merged_count = 0
        processed = set()

        # Compare all pairs
        for i, node1 in enumerate(nodes):
            if node1['_id'] in processed:
                continue

            for node2 in nodes[i+1:]:
                if node2['_id'] in processed:
                    continue

                # Calculate cosine similarity
                emb1 = np.array(node1['embedding'])
                emb2 = np.array(node2['embedding'])

                similarity = np.dot(emb1, emb2) / (
                    np.linalg.norm(emb1) * np.linalg.norm(emb2)
                )

                if similarity >= similarity_threshold:
                    # Merge nodes
                    if merge_strategy == "keep_first":
                        # Delete second node and redirect edges
                        await self._redirect_edges(
                            node2['_id'],
                            node1['_id']
                        )
                        await self.delete_document(
                            collection,
                            node2['_key']
                        )
                        processed.add(node2['_id'])
                        merged_count += 1

        self._logger.info(f"Merged {merged_count} duplicate nodes")
        return merged_count

    async def _redirect_edges(
        self,
        old_node_id: str,
        new_node_id: str
    ):
        """
        Redirect all edges from old node to new node.
        """
        # Update outbound edges
        query_out = """
        FOR edge IN @@edge_collection
            FILTER edge._from == @old_id
            UPDATE edge WITH {_from: @new_id} IN @@edge_collection
        """

        # Update inbound edges
        query_in = """
        FOR edge IN @@edge_collection
            FILTER edge._to == @old_id
            UPDATE edge WITH {_to: @new_id} IN @@edge_collection
        """

        # Get all edge collections (simplified)
        # In production, track edge collections properly
        edge_collections = ['relationships']  # Update as needed

        for edge_col in edge_collections:
            bind_vars = {
                '@edge_collection': edge_col,
                'old_id': old_node_id,
                'new_id': new_node_id
            }

            await self.execute(query_out, bind_vars=bind_vars)
            await self.execute(query_in, bind_vars=bind_vars)

    async def export_graph_to_networkx(
        self,
        graph_name: str = None
    ):
        """
        Export ArangoDB graph to NetworkX format.

        Requires networkx library.
        """
        try:
            import networkx as nx
        except ImportError:
            raise ImportError(
                "NetworkX is required for graph export. "
                "Install with: pip install networkx"
            )

        G = nx.DiGraph()

        # Get all vertices
        vertex_query = """
        FOR v IN @@graph_name
            RETURN v
        """

        # Get all edges
        edge_query = """
        FOR e IN @@edge_collection
            RETURN e
        """

        # Add nodes and edges to NetworkX graph
        # Implementation depends on graph structure

        return G

    async def build_graph_from_documents(
        self,
        documents: List[Dict],
        graph_name: str,
        entity_extractor: callable = None,
        relationship_extractor: callable = None
    ):
        """
        Build a knowledge graph from a list of documents.

        Args:
            documents: List of document dicts with 'id' and 'content'
            graph_name: Name for the knowledge graph
            entity_extractor: Function to extract entities from text
            relationship_extractor: Function to extract relationships

        Example:
            async def extract_entities(text):
                # Use NLP to extract entities
                return [{'id': 'ent1', 'type': 'person', 'text': 'John'}]

            async def extract_relationships(text, entities):
                # Extract relationships between entities
                return [{'from': 'ent1', 'to': 'ent2', 'type': 'knows'}]
        """
        # Create graph structure
        await self.create_knowledge_graph(
            graph_name,
            vertex_collections=['documents', 'entities'],
            edge_collection='relationships'
        )

        all_entities = {}

        for doc in documents:
            # Add document node
            await self.add_document_node(
                'documents',
                doc['id'],
                doc['content'],
                metadata=doc.get('metadata')
            )

            # Extract entities if extractor provided
            if entity_extractor:
                entities = await entity_extractor(doc['content'])

                for entity in entities:
                    entity_id = entity['id']

                    # Add entity node
                    if entity_id not in all_entities:
                        await self.add_entity_node(
                            'entities',
                            entity_id,
                            entity['type'],
                            properties=entity
                        )
                        all_entities[entity_id] = entity

                    # Link document to entity
                    await self.add_relationship(
                        'relationships',
                        f"documents/{doc['id']}",
                        f"entities/{entity_id}",
                        'mentions',
                        weight=1.0
                    )

            # Extract relationships if extractor provided
            if relationship_extractor and entity_extractor:
                entities = await entity_extractor(doc['content'])
                relationships = await relationship_extractor(
                    doc['content'],
                    entities
                )

                for rel in relationships:
                    await self.add_relationship(
                        'relationships',
                        f"entities/{rel['from']}",
                        f"entities/{rel['to']}",
                        rel['type'],
                        weight=rel.get('weight', 1.0)
                    )

        self._logger.info(
            f"Built knowledge graph with {len(documents)} documents "
            f"and {len(all_entities)} entities"
        )

        return graph_name
