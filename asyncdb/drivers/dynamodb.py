# -*- coding: utf-8 -*-
"""AWS DynamoDB Async Driver for AsyncDB.

Provides a native async DynamoDB driver using aiobotocore for the AsyncDB
framework. Supports CRUD operations, Query/Scan with auto-pagination,
PartiQL, batch operations, DDL/index management, and a pool driver.

Usage::

    from asyncdb import AsyncDB, AsyncPool

    # Single connection
    async with AsyncDB("dynamodb", params={
        "region_name": "us-east-1",
        "endpoint_url": "http://localhost:8000",
    }) as db:
        await db.set("my_table", {"pk": "key1", "data": "value"})
        item = await db.get("my_table", {"pk": "key1"})

    # Pool
    async with AsyncPool("dynamodb", params={
        "region_name": "us-east-1",
        "max_pool_connections": 10,
    }) as pool:
        conn = await pool.acquire()
        ...
        await pool.release(conn)
"""

import asyncio
import re
import time
import logging
from typing import Any, Optional, Union
from collections.abc import Sequence, Iterable

from ..exceptions import ConnectionTimeout, DriverError, EmptyStatement
from .base import InitDriver, BasePool

# Graceful import handling for aiobotocore and boto3
try:
    from aiobotocore.session import AioSession
    from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
except ImportError as _import_err:
    raise DriverError(
        "DynamoDB driver requires 'aiobotocore' and 'boto3'. "
        "Install with: pip install asyncdb[boto3]"
    ) from _import_err


class _KwargsTerminator:
    """Swallows residual kwargs to prevent TypeError from object.__init__().

    Required for InitDriver-only drivers (no DSN) to handle cooperative MRO
    when kwargs leak through the inheritance chain.
    """

    def __init__(self, **kwargs):
        super().__init__()


class dynamodb(InitDriver, _KwargsTerminator):
    """AWS DynamoDB async driver.

    A native async driver for Amazon DynamoDB using aiobotocore.
    Follows the asyncdb InitDriver pattern with Redis-style explicit
    methods (get/set/delete) alongside the abstract interface methods
    (query/execute/fetch_all).

    Attributes:
        _provider: Driver provider name for factory auto-discovery.
        _syntax: Query syntax type identifier.
    """

    _provider: str = "dynamodb"
    _syntax: str = "nosql"

    # PartiQL detection pattern
    _partiql_pattern: re.Pattern = re.compile(
        r"^\s*(SELECT|INSERT|UPDATE|DELETE)\s+",
        re.IGNORECASE,
    )

    def __init__(
        self,
        loop: Union[asyncio.AbstractEventLoop, None] = None,
        params: Union[dict, None] = None,
        **kwargs,
    ):
        """Initialize the DynamoDB driver.

        Args:
            loop: Optional asyncio event loop.
            params: Configuration parameters dict. Supported keys:
                - aws_access_key_id: AWS access key (optional, falls back to env).
                - aws_secret_access_key: AWS secret key (optional).
                - region_name: AWS region (required, e.g. "us-east-1").
                - endpoint_url: Custom endpoint for DynamoDB Local (optional).
                - aws_session_token: Temporary session token (optional).
                - profile_name: AWS profile name (optional).
                - table: Default table name (optional).
            **kwargs: Additional keyword arguments.
        """
        self._default_table: Optional[str] = None
        self._session: Optional[AioSession] = None
        self._client_ctx: Any = None
        self._serializer_type: TypeSerializer = TypeSerializer()
        self._deserializer_type: TypeDeserializer = TypeDeserializer()

        # Extract DynamoDB-specific params before passing to parent
        _params = params.copy() if params else {}
        self._default_table = _params.pop("table", None)

        # AWS client kwargs
        self._aws_access_key_id: Optional[str] = _params.get("aws_access_key_id")
        self._aws_secret_access_key: Optional[str] = _params.get("aws_secret_access_key")
        self._region_name: str = _params.get("region_name", "us-east-1")
        self._endpoint_url: Optional[str] = _params.get("endpoint_url")
        self._aws_session_token: Optional[str] = _params.get("aws_session_token")
        self._profile_name: Optional[str] = _params.get("profile_name")

        super().__init__(loop=loop, params=params, **kwargs)
        self._logger = logging.getLogger(f"DB.{self.__class__.__name__}")

    def _get_client_kwargs(self) -> dict:
        """Build kwargs dict for aiobotocore create_client call.

        Returns:
            Dictionary of client configuration arguments.
        """
        kwargs: dict[str, Any] = {
            "region_name": self._region_name,
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._aws_access_key_id:
            kwargs["aws_access_key_id"] = self._aws_access_key_id
        if self._aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self._aws_secret_access_key
        if self._aws_session_token:
            kwargs["aws_session_token"] = self._aws_session_token
        return kwargs

    # ── Connection Lifecycle ──────────────────────────────────────────

    async def connection(self, **kwargs) -> "dynamodb":
        """Create aiobotocore session and DynamoDB client.

        Creates an AioSession and opens a DynamoDB client using the
        configured AWS credentials and region.

        Returns:
            Self, with an active DynamoDB connection.

        Raises:
            ConnectionTimeout: If unable to connect to DynamoDB.
        """
        try:
            self._session = AioSession(
                profile=self._profile_name
            ) if self._profile_name else AioSession()
            client_kwargs = self._get_client_kwargs()
            self._client_ctx = self._session.create_client("dynamodb", **client_kwargs)
            self._connection = await self._client_ctx.__aenter__()
            self._connected = True
            self._logger.info(
                f"Connected to DynamoDB (region={self._region_name}, "
                f"endpoint={self._endpoint_url or 'default'})"
            )
            return self
        except Exception as err:
            self._connected = False
            raise ConnectionTimeout(
                f"Unable to connect to DynamoDB: {err}"
            ) from err

    async def close(self, timeout: int = 10) -> None:
        """Close the DynamoDB client.

        Args:
            timeout: Maximum time in seconds to wait for close.
        """
        try:
            if self._client_ctx is not None:
                await self._client_ctx.__aexit__(None, None, None)
        except Exception as err:
            self._logger.warning(f"Error closing DynamoDB client: {err}")
        finally:
            self._connection = None
            self._client_ctx = None
            self._connected = False
            self._logger.debug("DynamoDB connection closed.")

    async def __aenter__(self) -> "dynamodb":
        """Async context manager entry.

        Returns:
            Self with an active connection.
        """
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    # ── Test Connection ───────────────────────────────────────────────

    async def test_connection(self, **kwargs) -> bool:
        """Test connectivity by listing tables (limit=1).

        Returns:
            True if the connection is working.

        Raises:
            DriverError: If the connection test fails.
        """
        try:
            await self._connection.list_tables(Limit=1)
            return True
        except Exception as err:
            raise DriverError(
                f"DynamoDB connection test failed: {err}"
            ) from err

    # ── Serialization Utilities ───────────────────────────────────────

    def _serialize(self, item: dict) -> dict:
        """Serialize a Python dict to DynamoDB format.

        Args:
            item: Plain Python dictionary.

        Returns:
            Dictionary with DynamoDB type descriptors.
        """
        return {k: self._serializer_type.serialize(v) for k, v in item.items()}

    def _deserialize(self, item: dict) -> dict:
        """Deserialize a DynamoDB item to a plain Python dict.

        Args:
            item: DynamoDB item with type descriptors.

        Returns:
            Plain Python dictionary.
        """
        return {k: self._deserializer_type.deserialize(v) for k, v in item.items()}

    def _deserialize_items(self, items: list[dict]) -> list[dict]:
        """Deserialize a list of DynamoDB items.

        Args:
            items: List of DynamoDB items with type descriptors.

        Returns:
            List of plain Python dictionaries.
        """
        return [self._deserialize(item) for item in items]

    def _is_partiql(self, sentence: Any) -> bool:
        """Detect if a sentence looks like a PartiQL/SQL statement.

        Args:
            sentence: The input to check.

        Returns:
            True if the sentence appears to be a PartiQL statement.
        """
        if not isinstance(sentence, str):
            return False
        return bool(self._partiql_pattern.match(sentence))

    def _chunk_items(self, items: list, chunk_size: int = 25) -> list[list]:
        """Split items into chunks for batch operations.

        Args:
            items: List of items to chunk.
            chunk_size: Maximum items per chunk (default 25).

        Returns:
            List of item sublists.
        """
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    async def _paginate_query(self, method: str, table: str, **kwargs) -> list[dict]:
        """Auto-paginate Query or Scan operations.

        Uses LastEvaluatedKey to fetch all pages of results.

        Args:
            method: The DynamoDB API method name ("query" or "scan").
            table: Table name.
            **kwargs: Additional arguments for the API call.

        Returns:
            List of deserialized items from all pages.
        """
        items: list[dict] = []
        kwargs["TableName"] = table
        api_method = getattr(self._connection, method)
        while True:
            response = await api_method(**kwargs)
            raw_items = response.get("Items", [])
            items.extend(self._deserialize_items(raw_items))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return items

    def _resolve_table(self, table: Optional[str] = None) -> str:
        """Resolve the table name, falling back to default.

        Args:
            table: Explicit table name, or None to use default.

        Returns:
            Resolved table name.

        Raises:
            EmptyStatement: If no table name is available.
        """
        resolved = table or self._default_table
        if not resolved:
            raise EmptyStatement("No table name provided and no default table set.")
        return resolved

    # ── Abstract Method: use ──────────────────────────────────────────

    async def use(self, database: str) -> None:
        """Set the default table name for subsequent operations.

        Args:
            database: Table name to use as default.
        """
        self._default_table = database
        self._logger.debug(f"Default table set to: {database}")

    # ── CRUD Operations (TASK-016) ────────────────────────────────────

    async def get(self, table: str = None, key: dict = None, **kwargs) -> Optional[dict]:
        """Get a single item by primary key.

        Args:
            table: Table name (uses default if not provided).
            key: Primary key dictionary (e.g. {"pk": "value"}).
            **kwargs: Additional arguments for GetItem API.

        Returns:
            Deserialized item dict, or None if not found.

        Raises:
            DriverError: On DynamoDB client errors.
            EmptyStatement: If key is empty or missing.
        """
        table = self._resolve_table(table)
        if not key:
            raise EmptyStatement("Cannot get item without a key.")
        try:
            response = await self._connection.get_item(
                TableName=table,
                Key=self._serialize(key),
                **kwargs,
            )
            raw_item = response.get("Item")
            if raw_item is None:
                return None
            return self._deserialize(raw_item)
        except self._connection.exceptions.ResourceNotFoundException as err:
            raise DriverError(f"Table '{table}' not found: {err}") from err
        except Exception as err:
            raise DriverError(f"DynamoDB get error: {err}") from err

    async def set(self, table: str = None, item: dict = None, **kwargs) -> bool:
        """Put an item into a table.

        Args:
            table: Table name (uses default if not provided).
            item: Item dictionary to write.
            **kwargs: Additional arguments for PutItem API
                (e.g. ConditionExpression).

        Returns:
            True on success.

        Raises:
            DriverError: On DynamoDB client errors.
            EmptyStatement: If item is empty.
        """
        table = self._resolve_table(table)
        if not item:
            raise EmptyStatement("Cannot put an empty item.")
        try:
            await self._connection.put_item(
                TableName=table,
                Item=self._serialize(item),
                **kwargs,
            )
            return True
        except Exception as err:
            raise DriverError(f"DynamoDB set/put error: {err}") from err

    async def delete(self, table: str = None, key: dict = None, **kwargs) -> bool:
        """Delete an item by primary key.

        Args:
            table: Table name (uses default if not provided).
            key: Primary key dictionary.
            **kwargs: Additional arguments for DeleteItem API.

        Returns:
            True on success.

        Raises:
            DriverError: On DynamoDB client errors.
            EmptyStatement: If key is empty.
        """
        table = self._resolve_table(table)
        if not key:
            raise EmptyStatement("Cannot delete item without a key.")
        try:
            await self._connection.delete_item(
                TableName=table,
                Key=self._serialize(key),
                **kwargs,
            )
            return True
        except Exception as err:
            raise DriverError(f"DynamoDB delete error: {err}") from err

    async def write(self, table: str = None, item: dict = None, **kwargs) -> bool:
        """Write a single item (alias for set).

        Args:
            table: Table name (uses default if not provided).
            item: Item dictionary to write.
            **kwargs: Additional arguments for PutItem API.

        Returns:
            True on success.
        """
        return await self.set(table=table, item=item, **kwargs)

    async def update(
        self,
        table: str = None,
        key: dict = None,
        update_expression: str = None,
        expression_values: dict = None,
        **kwargs,
    ) -> Optional[dict]:
        """Update an item with an UpdateExpression.

        Args:
            table: Table name (uses default if not provided).
            key: Primary key dictionary.
            update_expression: DynamoDB UpdateExpression string.
            expression_values: ExpressionAttributeValues dictionary.
            **kwargs: Additional arguments (e.g. ReturnValues,
                ExpressionAttributeNames, ConditionExpression).

        Returns:
            Updated attributes dict (if ReturnValues is set), or None.

        Raises:
            DriverError: On DynamoDB client errors.
            EmptyStatement: If key or update_expression is missing.
        """
        table = self._resolve_table(table)
        if not key:
            raise EmptyStatement("Cannot update item without a key.")
        if not update_expression:
            raise EmptyStatement("Cannot update item without an UpdateExpression.")
        try:
            api_kwargs: dict[str, Any] = {
                "TableName": table,
                "Key": self._serialize(key),
                "UpdateExpression": update_expression,
            }
            if expression_values:
                api_kwargs["ExpressionAttributeValues"] = {
                    k: self._serializer_type.serialize(v)
                    for k, v in expression_values.items()
                }
            # Merge additional kwargs (ReturnValues, ConditionExpression, etc.)
            api_kwargs.update(kwargs)
            if "ReturnValues" not in api_kwargs:
                api_kwargs["ReturnValues"] = "ALL_NEW"
            response = await self._connection.update_item(**api_kwargs)
            attrs = response.get("Attributes")
            if attrs:
                return self._deserialize(attrs)
            return None
        except Exception as err:
            raise DriverError(f"DynamoDB update error: {err}") from err

    # ── Query & Scan Operations (TASK-017) ────────────────────────────

    async def query(self, table: str = None, **kwargs) -> Optional[list[dict]]:
        """Query by partition key with optional sort key conditions.

        Supports auto-pagination and GSI/LSI queries via IndexName kwarg.

        Args:
            table: Table name (uses default if not provided).
            **kwargs: DynamoDB Query parameters (KeyConditionExpression,
                FilterExpression, ExpressionAttributeValues,
                ExpressionAttributeNames, IndexName, Limit, etc.).

        Returns:
            List of deserialized item dicts, or None on error.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        table = self._resolve_table(table)
        try:
            return await self._paginate_query("query", table, **kwargs)
        except Exception as err:
            raise DriverError(f"DynamoDB query error: {err}") from err

    async def queryrow(self, table: str = None, **kwargs) -> Optional[dict]:
        """Query returning a single item.

        Executes a query with Limit=1 and returns the first result.

        Args:
            table: Table name (uses default if not provided).
            **kwargs: DynamoDB Query parameters.

        Returns:
            Single deserialized item dict, or None if no match.
        """
        kwargs["Limit"] = 1
        results = await self.query(table=table, **kwargs)
        if results:
            return results[0]
        return None

    async def fetch_all(self, table: str = None, **kwargs) -> list[dict]:
        """Full table scan with auto-pagination.

        Args:
            table: Table name (uses default if not provided).
            **kwargs: DynamoDB Scan parameters (FilterExpression, etc.).

        Returns:
            List of all deserialized items in the table.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        table = self._resolve_table(table)
        try:
            return await self._paginate_query("scan", table, **kwargs)
        except Exception as err:
            raise DriverError(f"DynamoDB scan error: {err}") from err

    async def fetch_one(self, table: str = None, key: dict = None, **kwargs) -> Optional[dict]:
        """Fetch a single item by key (alias for get).

        Args:
            table: Table name (uses default if not provided).
            key: Primary key dictionary.
            **kwargs: Additional arguments for GetItem API.

        Returns:
            Deserialized item dict, or None if not found.
        """
        return await self.get(table=table, key=key, **kwargs)

    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        """Smart execute: routes PartiQL strings or performs put_item.

        If the sentence is a SQL-like string, it is executed as a PartiQL
        statement. Otherwise it is treated as a put_item operation.

        Args:
            sentence: PartiQL statement string or item dict to write.
            *args: Positional arguments (PartiQL parameters list).
            **kwargs: Additional arguments (table, etc.).

        Returns:
            PartiQL results or True for write operations.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        if self._is_partiql(sentence):
            params = args[0] if args else None
            return await self.partiql(sentence, parameters=params, **kwargs)
        elif isinstance(sentence, dict):
            table = kwargs.pop("table", None)
            return await self.set(table=table, item=sentence, **kwargs)
        else:
            raise DriverError(
                f"Cannot execute: unsupported sentence type {type(sentence).__name__}"
            )

    async def execute_many(self, sentences: list, *args) -> Optional[Any]:
        """Execute multiple statements.

        If sentences are strings, executes as batch PartiQL.
        If dicts, writes them as items.

        Args:
            sentences: List of PartiQL strings or item dicts.
            *args: Additional arguments.

        Returns:
            List of results for PartiQL, or True for writes.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        if not sentences:
            return None
        # Check if PartiQL batch
        if isinstance(sentences[0], str) and self._is_partiql(sentences[0]):
            stmts = [{"Statement": s} for s in sentences]
            return await self.partiql_batch(stmts)
        elif isinstance(sentences[0], dict):
            # If dicts with "Statement" key, it's PartiQL batch format
            if "Statement" in sentences[0]:
                return await self.partiql_batch(sentences)
            # Otherwise treat as items to write
            table = None
            for item in sentences:
                await self.set(table=table, item=item)
            return True
        return None

    async def prepare(self, sentence: Union[str, list] = None) -> Any:
        """Prepared statements are not supported by DynamoDB.

        Raises:
            NotImplementedError: Always raised.
        """
        raise NotImplementedError("DynamoDB does not support prepared statements.")

    # ── PartiQL Operations (TASK-018) ─────────────────────────────────

    async def partiql(
        self,
        statement: str,
        parameters: list = None,
        **kwargs,
    ) -> Optional[list[dict]]:
        """Execute a PartiQL statement.

        Args:
            statement: PartiQL SQL-like statement string.
            parameters: List of parameter values for the statement.
            **kwargs: Additional arguments for execute_statement API.

        Returns:
            List of deserialized result dicts.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            api_kwargs: dict[str, Any] = {"Statement": statement}
            if parameters:
                api_kwargs["Parameters"] = [
                    self._serializer_type.serialize(v) for v in parameters
                ]
            api_kwargs.update(kwargs)

            items: list[dict] = []
            while True:
                response = await self._connection.execute_statement(**api_kwargs)
                raw_items = response.get("Items", [])
                items.extend(self._deserialize_items(raw_items))
                next_token = response.get("NextToken")
                if not next_token:
                    break
                api_kwargs["NextToken"] = next_token
            return items
        except Exception as err:
            raise DriverError(f"DynamoDB PartiQL error: {err}") from err

    async def partiql_batch(
        self,
        statements: list[dict],
        **kwargs,
    ) -> Optional[list[dict]]:
        """Execute multiple PartiQL statements via batch_execute_statement.

        Args:
            statements: List of statement dicts, each with "Statement"
                and optionally "Parameters" keys.
            **kwargs: Additional arguments.

        Returns:
            List of result dicts from batch execution.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            results: list[dict] = []
            # Chunk into groups of 25 (DynamoDB limit)
            for chunk in self._chunk_items(statements, chunk_size=25):
                # Serialize parameters if present
                serialized_stmts = []
                for stmt in chunk:
                    s: dict[str, Any] = {"Statement": stmt["Statement"]}
                    if "Parameters" in stmt:
                        s["Parameters"] = [
                            self._serializer_type.serialize(v)
                            for v in stmt["Parameters"]
                        ]
                    serialized_stmts.append(s)
                response = await self._connection.batch_execute_statement(
                    Statements=serialized_stmts,
                    **kwargs,
                )
                for resp in response.get("Responses", []):
                    if "Error" in resp:
                        self._logger.warning(
                            f"PartiQL batch error: {resp['Error']}"
                        )
                    elif "Item" in resp:
                        results.append(self._deserialize(resp["Item"]))
            return results
        except Exception as err:
            raise DriverError(f"DynamoDB PartiQL batch error: {err}") from err

    # ── Batch Operations (TASK-019) ───────────────────────────────────

    async def write_batch(
        self,
        table: str = None,
        items: list[dict] = None,
        **kwargs,
    ) -> bool:
        """Batch write items with auto-chunking and retry.

        Auto-chunks into groups of 25 (DynamoDB BatchWriteItem limit)
        and retries UnprocessedItems with exponential backoff.

        Args:
            table: Table name (uses default if not provided).
            items: List of item dicts to write.
            **kwargs: Additional arguments.

        Returns:
            True when all items are written.

        Raises:
            DriverError: On DynamoDB client errors or max retries exceeded.
        """
        table = self._resolve_table(table)
        if not items:
            return True
        max_retries = 3
        try:
            for chunk in self._chunk_items(items, chunk_size=25):
                request_items = {
                    table: [
                        {"PutRequest": {"Item": self._serialize(item)}}
                        for item in chunk
                    ]
                }
                for attempt in range(max_retries):
                    response = await self._connection.batch_write_item(
                        RequestItems=request_items
                    )
                    unprocessed = response.get("UnprocessedItems", {})
                    if not unprocessed:
                        break
                    if attempt == max_retries - 1:
                        raise DriverError(
                            f"Failed to write all items after {max_retries} retries. "
                            f"Unprocessed: {len(unprocessed.get(table, []))} items"
                        )
                    await asyncio.sleep(2 ** attempt * 0.1)
                    request_items = unprocessed
            return True
        except DriverError:
            raise
        except Exception as err:
            raise DriverError(f"DynamoDB batch write error: {err}") from err

    async def get_batch(
        self,
        table: str = None,
        keys: list[dict] = None,
        **kwargs,
    ) -> list[dict]:
        """Batch get items by keys with auto-chunking and retry.

        Auto-chunks into groups of 100 (DynamoDB BatchGetItem limit)
        and retries UnprocessedKeys.

        Args:
            table: Table name (uses default if not provided).
            keys: List of primary key dicts.
            **kwargs: Additional arguments.

        Returns:
            List of deserialized item dicts.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        table = self._resolve_table(table)
        if not keys:
            return []
        max_retries = 3
        all_items: list[dict] = []
        try:
            for chunk in self._chunk_items(keys, chunk_size=100):
                request_items = {
                    table: {
                        "Keys": [self._serialize(key) for key in chunk]
                    }
                }
                for attempt in range(max_retries):
                    response = await self._connection.batch_get_item(
                        RequestItems=request_items
                    )
                    raw_items = response.get("Responses", {}).get(table, [])
                    all_items.extend(self._deserialize_items(raw_items))
                    unprocessed = response.get("UnprocessedKeys", {})
                    if not unprocessed:
                        break
                    if attempt == max_retries - 1:
                        self._logger.warning(
                            f"Some keys still unprocessed after {max_retries} retries."
                        )
                        break
                    await asyncio.sleep(2 ** attempt * 0.1)
                    request_items = unprocessed
            return all_items
        except DriverError:
            raise
        except Exception as err:
            raise DriverError(f"DynamoDB batch get error: {err}") from err

    # ── DDL & Index Management (TASK-020) ─────────────────────────────

    async def create_table(
        self,
        table: str,
        key_schema: list[dict],
        attribute_definitions: list[dict],
        billing_mode: str = "PAY_PER_REQUEST",
        **kwargs,
    ) -> dict:
        """Create a DynamoDB table.

        Args:
            table: Table name.
            key_schema: Key schema list
                (e.g. [{"AttributeName": "pk", "KeyType": "HASH"}]).
            attribute_definitions: Attribute definitions list.
            billing_mode: Billing mode ("PAY_PER_REQUEST" or "PROVISIONED").
            **kwargs: Additional arguments (GlobalSecondaryIndexes,
                LocalSecondaryIndexes, ProvisionedThroughput, etc.).

        Returns:
            Table description dict from the create response.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            api_kwargs: dict[str, Any] = {
                "TableName": table,
                "KeySchema": key_schema,
                "AttributeDefinitions": attribute_definitions,
                "BillingMode": billing_mode,
            }
            api_kwargs.update(kwargs)
            response = await self._connection.create_table(**api_kwargs)
            return response.get("TableDescription", {})
        except Exception as err:
            raise DriverError(f"DynamoDB create_table error: {err}") from err

    async def delete_table(self, table: str) -> bool:
        """Delete a DynamoDB table.

        Args:
            table: Table name to delete.

        Returns:
            True on success.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            await self._connection.delete_table(TableName=table)
            return True
        except Exception as err:
            raise DriverError(f"DynamoDB delete_table error: {err}") from err

    async def describe_table(self, table: str) -> dict:
        """Describe a DynamoDB table.

        Args:
            table: Table name.

        Returns:
            Full table metadata dict.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            response = await self._connection.describe_table(TableName=table)
            return response.get("Table", {})
        except Exception as err:
            raise DriverError(f"DynamoDB describe_table error: {err}") from err

    async def tables(self, **kwargs) -> list[str]:
        """List all tables in the account/region.

        Supports auto-pagination via ExclusiveStartTableName.

        Args:
            **kwargs: Additional arguments (Limit, etc.).

        Returns:
            List of table name strings.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            table_names: list[str] = []
            while True:
                response = await self._connection.list_tables(**kwargs)
                table_names.extend(response.get("TableNames", []))
                last_table = response.get("LastEvaluatedTableName")
                if not last_table:
                    break
                kwargs["ExclusiveStartTableName"] = last_table
            return table_names
        except Exception as err:
            raise DriverError(f"DynamoDB list_tables error: {err}") from err

    async def table(self, tablename: str = "") -> dict:
        """Get table description (alias for describe_table).

        Args:
            tablename: Table name. If empty, uses default table.

        Returns:
            Table metadata dict.
        """
        tablename = self._resolve_table(tablename or None)
        return await self.describe_table(tablename)

    async def wait_for_table(
        self,
        table: str,
        status: str = "ACTIVE",
        timeout: int = 300,
    ) -> bool:
        """Poll until a table reaches the desired status.

        Args:
            table: Table name.
            status: Target table status (default "ACTIVE").
            timeout: Maximum seconds to wait (default 300).

        Returns:
            True when the table reaches the target status.

        Raises:
            DriverError: If timeout is exceeded.
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                desc = await self.describe_table(table)
                if desc.get("TableStatus") == status:
                    return True
            except DriverError:
                pass  # Table may not exist yet during creation
            await asyncio.sleep(2)
        raise DriverError(
            f"Table '{table}' did not reach status '{status}' within {timeout}s"
        )

    async def create_index(
        self,
        table: str,
        index_name: str,
        key_schema: list[dict],
        projection: dict,
        **kwargs,
    ) -> dict:
        """Create a Global Secondary Index on an existing table.

        Args:
            table: Table name.
            index_name: Name for the new GSI.
            key_schema: Key schema for the index.
            projection: Projection configuration dict.
            **kwargs: Additional arguments (ProvisionedThroughput, etc.).

        Returns:
            Update table response dict.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            create_action: dict[str, Any] = {
                "IndexName": index_name,
                "KeySchema": key_schema,
                "Projection": projection,
            }
            # Include ProvisionedThroughput if present
            if "ProvisionedThroughput" in kwargs:
                create_action["ProvisionedThroughput"] = kwargs.pop(
                    "ProvisionedThroughput"
                )
            response = await self._connection.update_table(
                TableName=table,
                GlobalSecondaryIndexUpdates=[
                    {"Create": create_action}
                ],
                **kwargs,
            )
            return response.get("TableDescription", {})
        except Exception as err:
            raise DriverError(f"DynamoDB create_index error: {err}") from err

    async def delete_index(self, table: str, index_name: str) -> dict:
        """Delete a Global Secondary Index.

        Args:
            table: Table name.
            index_name: Name of the GSI to delete.

        Returns:
            Update table response dict.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        try:
            response = await self._connection.update_table(
                TableName=table,
                GlobalSecondaryIndexUpdates=[
                    {"Delete": {"IndexName": index_name}}
                ],
            )
            return response.get("TableDescription", {})
        except Exception as err:
            raise DriverError(f"DynamoDB delete_index error: {err}") from err

    async def list_indexes(self, table: str) -> list[dict]:
        """List all GSIs and LSIs on a table.

        Args:
            table: Table name.

        Returns:
            List of index metadata dicts.

        Raises:
            DriverError: On DynamoDB client errors.
        """
        desc = await self.describe_table(table)
        indexes: list[dict] = []
        for gsi in desc.get("GlobalSecondaryIndexes", []):
            indexes.append({
                "type": "GSI",
                "IndexName": gsi.get("IndexName"),
                "KeySchema": gsi.get("KeySchema"),
                "Projection": gsi.get("Projection"),
                "IndexStatus": gsi.get("IndexStatus"),
            })
        for lsi in desc.get("LocalSecondaryIndexes", []):
            indexes.append({
                "type": "LSI",
                "IndexName": lsi.get("IndexName"),
                "KeySchema": lsi.get("KeySchema"),
                "Projection": lsi.get("Projection"),
            })
        return indexes


# ── Pool Driver (TASK-021) ────────────────────────────────────────────


class dynamodbPool(BasePool):
    """Connection pool for DynamoDB using a shared aiobotocore session.

    Manages a shared AioSession with HTTP connection pooling for
    DynamoDB. Each acquire() returns a dynamodb driver instance
    sharing the session.

    Attributes:
        _provider: Driver provider name for factory auto-discovery.
        _syntax: Query syntax type identifier.
    """

    _provider: str = "dynamodb"
    _syntax: str = "nosql"
    _dsn_template: str = ""  # DynamoDB doesn't use DSN

    def __init__(
        self,
        dsn: str = "",
        loop: asyncio.AbstractEventLoop = None,
        params: Union[dict, None] = None,
        **kwargs,
    ):
        """Initialize the DynamoDB pool driver.

        Args:
            dsn: Ignored (DynamoDB doesn't use DSN).
            loop: Optional asyncio event loop.
            params: Configuration parameters dict (same as dynamodb driver,
                plus max_pool_connections).
            **kwargs: Additional keyword arguments.
        """
        self._session: Optional[AioSession] = None
        self._params: dict = params.copy() if params else {}
        self._max_pool_connections: int = self._params.pop(
            "max_pool_connections", 10
        )
        # Extract AWS client kwargs
        self._region_name: str = self._params.get("region_name", "us-east-1")
        self._endpoint_url: Optional[str] = self._params.get("endpoint_url")
        self._aws_access_key_id: Optional[str] = self._params.get("aws_access_key_id")
        self._aws_secret_access_key: Optional[str] = self._params.get("aws_secret_access_key")
        self._aws_session_token: Optional[str] = self._params.get("aws_session_token")
        self._profile_name: Optional[str] = self._params.get("profile_name")
        self._client_kwargs: dict[str, Any] = {}
        self._acquired_connections: list = []

        super().__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        self._logger = logging.getLogger(f"DB.{self.__class__.__name__}")

    def _get_client_kwargs(self) -> dict:
        """Build kwargs dict for aiobotocore create_client.

        Returns:
            Dictionary of client configuration arguments.
        """
        kwargs: dict[str, Any] = {
            "region_name": self._region_name,
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._aws_access_key_id:
            kwargs["aws_access_key_id"] = self._aws_access_key_id
        if self._aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self._aws_secret_access_key
        if self._aws_session_token:
            kwargs["aws_session_token"] = self._aws_session_token
        return kwargs

    async def connect(self, **kwargs) -> "dynamodbPool":
        """Create shared AioSession for connection pooling.

        Returns:
            Self, with an active session.

        Raises:
            ConnectionTimeout: If unable to create the session.
        """
        try:
            self._session = AioSession(
                profile=self._profile_name
            ) if self._profile_name else AioSession()
            self._client_kwargs = self._get_client_kwargs()
            # Verify connectivity with a test client
            ctx = self._session.create_client("dynamodb", **self._client_kwargs)
            client = await ctx.__aenter__()
            try:
                await client.list_tables(Limit=1)
            finally:
                await ctx.__aexit__(None, None, None)
            self._connected = True
            self._logger.info(
                f"DynamoDB pool connected (region={self._region_name}, "
                f"max_connections={self._max_pool_connections})"
            )
            return self
        except Exception as err:
            self._connected = False
            raise ConnectionTimeout(
                f"Unable to connect DynamoDB pool: {err}"
            ) from err

    async def disconnect(self, timeout: int = 5) -> None:
        """Close all client connections and the session.

        Args:
            timeout: Maximum seconds to wait for cleanup.
        """
        # Close all acquired connections
        for conn in self._acquired_connections:
            try:
                if conn._client_ctx is not None:
                    await conn._client_ctx.__aexit__(None, None, None)
                conn._connected = False
            except Exception as err:
                self._logger.warning(f"Error closing pool connection: {err}")
        self._acquired_connections.clear()
        self._session = None
        self._connected = False
        self._logger.debug("DynamoDB pool disconnected.")

    close = disconnect

    async def acquire(self) -> "dynamodb":
        """Create a new dynamodb driver instance sharing the pool session.

        Returns:
            A dynamodb driver instance with an active connection.

        Raises:
            DriverError: If the pool is not connected.
        """
        if not self._connected or not self._session:
            raise DriverError("DynamoDB pool is not connected. Call connect() first.")
        try:
            driver = dynamodb(params=self._params)
            driver._session = self._session
            ctx = self._session.create_client("dynamodb", **self._client_kwargs)
            driver._client_ctx = ctx
            driver._connection = await ctx.__aenter__()
            driver._connected = True
            self._acquired_connections.append(driver)
            return driver
        except Exception as err:
            raise DriverError(f"DynamoDB pool acquire error: {err}") from err

    async def release(
        self,
        connection: "dynamodb" = None,
        timeout: int = 10,
    ) -> None:
        """Close an individual client returned by acquire.

        Args:
            connection: The dynamodb driver instance to release.
            timeout: Maximum seconds to wait for release.
        """
        if not connection:
            return
        try:
            if connection._client_ctx is not None:
                await connection._client_ctx.__aexit__(None, None, None)
            connection._connection = None
            connection._connected = False
            if connection in self._acquired_connections:
                self._acquired_connections.remove(connection)
        except Exception as err:
            self._logger.warning(f"Error releasing pool connection: {err}")
