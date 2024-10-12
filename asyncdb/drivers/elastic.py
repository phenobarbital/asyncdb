""" ElasticSearch async Provider.
Notes on Elastic Provider
--------------------
This provider implements a few subset of funcionalities from elasticsearch.
TODO:
 - use jsonpath to query json-objects
 - implements lists and hash datatypes
"""

import asyncio
import time
from typing import Any, Union
from dataclasses import dataclass, is_dataclass
from elasticsearch import AsyncElasticsearch
from ..exceptions import ConnectionTimeout, DriverError
from .base import BaseDriver
from ..utils.types import SafeDict


@dataclass
class ElasticConfig:
    host: str
    port: int = 9200
    user: str
    password: str
    db: str
    protocol: str = "http"

    def get_dsn(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}/"


class elastic(BaseDriver):
    _provider = "elasticsearch"
    _syntax = "json"

    def __init__(self, dsn: str = None, loop=None, params: Union[dict, ElasticConfig] = None, **kwargs):
        # self._dsn = "{protocol}://{user}:{password}@{host}:{port}/{database}"
        if isinstance(params, ElasticConfig):
            self._database = params.database
        else:
            self._database = params.pop("db", "default")
        self._dsn = "{protocol}://{host}:{port}/"
        super(elastic, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    def create_dsn(self, params: Union[dict, dataclass]):
        if is_dataclass(params):
            self._dsn = params.get_dsn()
        else:
            try:
                return self._dsn.format_map(SafeDict(**params)) if params else None
            except TypeError as err:
                self._logger.error(err)
                raise DriverError(f"Error creating DSN connection: {err}") from err

    async def connection(self, timeout: int = 10, **kwargs):
        """
        Asynchronously establish a connection to Elasticsearch
        with a connection timeout.

        Args:
            timeout (int): The maximum time in seconds to wait for the connection
            to be established. Defaults to 10 seconds.
            **kwargs: Additional keyword arguments to pass to the Elasticsearch connection.

        Returns:
            self: The current instance of the elastic class after establishing the connection.

        Raises:
            ConnectionTimeout: If the connection attempt exceeds the specified timeout.
            DriverError: If any other error occurs while attempting to connect to Elasticsearch.
        """
        args = {"timeout": self._timeout, **self.kwargs}
        try:
            # Use asyncio.wait_for to apply a timeout to the connection attempt
            self._connection = await asyncio.wait_for(AsyncElasticsearch(hosts=self._dsn, **args), timeout=timeout)
            self._connected = True
            return self
        except asyncio.TimeoutError:
            raise ConnectionTimeout(f"Elasticsearch connection timed out after {timeout} seconds")
        except Exception as exc:
            raise DriverError(f"Elasticsearch Connection Error: {exc}") from exc

    def is_closed(self) -> bool:
        return self._connection is None

    async def ping(self, msg: str = None) -> bool:
        try:
            return await self._connection.ping()
        except Exception as exc:
            self._logger.error(f"Ping failed: {exc}")
            return False

    async def close(self, timeout: int = 10):
        try:
            # Close the Elasticsearch connection
            await asyncio.wait_for(self._connection.close(), timeout=timeout)
        except Exception as e:
            self._logger.warning(f"Elasticsearch closing connection: {e}")

    async def test_connection(self, key: str = "test-index", id: int = 1) -> bool:
        try:
            # Perform a simple operation to check the connection
            await self._connection.index(index=key, id=id, document={"test_field": "test_value"})
            await self._connection.delete(index=key, id=id)
            return True
        except Exception as exc:
            self._logger.error(f"Test connection failed: {exc}")
            return False

    async def use(self, database: int):
        self._database = database

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError()  # pragma: no-cover

    async def get(self, key: str):
        """
        Get a document by its ID.
        """
        try:
            response = await self._connection.get(index=self._database, id=key)
            return response["_source"]
        except Exception as exc:
            self._logger.error(f"Error getting document with ID {key}: {exc}")
            raise DriverError(f"Error getting document with ID {key}: {exc}") from exc

    async def set(self, key: str, value: dict, **kwargs):
        """
        Index or update a document in Elasticsearch.
        """
        try:
            await self._connection.index(index=self._database, id=key, document=value, **kwargs)
        except Exception as exc:
            self._logger.error(f"Error setting document with ID {key}: {exc}")
            raise DriverError(f"Error setting document with ID {key}: {exc}") from exc

    async def exists(self, key: str, *keys) -> bool:
        """
        Check if a document exists by its ID.
        """
        try:
            return await self._connection.exists(index=self._database, id=key)
        except Exception as exc:
            self._logger.error(f"Error checking existence of document with ID {key}: {exc}")
            raise DriverError(f"Error checking existence of document with ID {key}: {exc}") from exc

    async def delete(self, key: str, *keys):
        """
        Delete a document by its ID.
        """
        try:
            await self._connection.delete(index=self._database, id=key)
        except Exception as exc:
            self._logger.error(f"Error deleting document with ID {key}: {exc}")
            raise DriverError(f"Error deleting document with ID {key}: {exc}") from exc

    async def query(self, sentence: str, *args, **kwargs) -> Any:
        """
        Execute a search query on the Elasticsearch index.

        Args:
            sentence (str): The query body to be executed on the Elasticsearch index,
                            typically written in Elasticsearch Query DSL format.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments to pass to the search method.

        Returns:
            List[dict]: A list of documents (hits) matching the query. Each document is a dictionary
                        containing the document's source and metadata.

        Raises:
            DriverError: If an error occurs while executing the query.

        Example:
            response = await elastic_instance.query('{"query": {"match_all": {}}}')
            # Example response:
            # [
            #     {
            #         "_index": "my-index",
            #         "_type": "_doc",
            #         "_id": "1",
            #         "_score": 1.0,
            #         "_source": {
            #             "field1": "value1",
            #             "field2": "value2",
            #             ...
            #         }
            #     },
            #     ...
            # ]
        """
        result = None
        error = None
        try:
            response = await self._connection.search(index=self._database, body=sentence, **kwargs)
            result = response["hits"]["hits"]
        except Exception as exc:
            error = exc
        finally:
            return await self._serializer(result, error)

    async def queryrow(self, sentence: str, *args, **kwargs) -> Any:
        """
        Execute a search query on the Elasticsearch index and return a single document.

        Args:
            sentence (str): The query body to be executed on the Elasticsearch index,
                            typically written in Elasticsearch Query DSL format.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments to pass to the search method.

        Returns:
            dict or None: A dictionary containing the first document (hit) matching the query,
                          including the document's source and metadata. Returns None if no documents match.
                          The result is processed through the custom serializer.

        Raises:
            DriverError: If an error occurs while executing the query.

        Example:
            response = await elastic_instance.queryrow('{"query": {"match": {"field1": "value"}}}')
            # Example response:
            # {
            #     "_index": "my-index",
            #     "_type": "_doc",
            #     "_id": "1",
            #     "_score": 1.0,
            #     "_source": {
            #         "field1": "value",
            #         "field2": "value2",
            #         ...
            #     }
            # }
        """
        result = None
        error = None
        try:
            response = await self._connection.search(index=self._database, body=sentence, size=1, **kwargs)
            hits = response["hits"]["hits"]
            return hits[0] if hits else None
        except Exception as exc:
            error = exc
        finally:
            return await self._serializer(result, error)

    async def execute(self, sentence: str, *args, **kwargs) -> None:
        """
        Execute an Elasticsearch operation that doesn't return a result.
        For example, creating an index or updating settings.
        """
        try:
            # Assuming `sentence` is an action, like creating an index
            if sentence == "create_index":
                index_name = kwargs.get("index_name")
                body = kwargs.get("body", {})
                await self._connection.indices.create(index=index_name, body=body)
            # Add other operations as needed
            else:
                self._logger.warning(f"Unsupported operation: {sentence}")
        except Exception as exc:
            self._logger.error(f"Error executing operation {sentence}: {exc}")
            raise DriverError(f"Error executing operation {sentence}: {exc}") from exc

    async def execute_many(self, sentences: list, *args, **kwargs) -> None:
        """
        Execute multiple Elasticsearch operations in bulk.
        """
        try:
            actions = []
            for sentence in sentences:
                # Assuming each sentence is a dict representing an action
                # For example: {'_op_type': 'index', '_index': 'my-index', '_id': '1', '_source': {...}}
                actions.append(sentence)
            if actions:
                await self._connection.bulk(body=actions)
        except Exception as exc:
            self._logger.error(f"Error executing bulk operations: {exc}")
            raise DriverError(f"Error executing bulk operations: {exc}") from exc

    async def fetchall(self, sentence: str, *args, **kwargs) -> Any:
        try:
            response = await self._connection.search(index=self._database, body=sentence, **kwargs)
            return response["hits"]["hits"]
        except Exception as exc:
            raise DriverError(f"Error executing query: {exc}") from exc

    fetch_all = fetchall

    async def fetchone(self, sentence: str, *args, **kwargs) -> Any:
        try:
            response = await self._connection.search(index=self._database, body=sentence, size=1, **kwargs)
            hits = response["hits"]["hits"]
            return hits[0] if hits else None
        except Exception as exc:
            raise DriverError(f"Error executing queryrow: {exc}") from exc

    fetch_one = fetchone
