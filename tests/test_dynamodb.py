"""Tests for the DynamoDB driver (FEAT-004 / TASK-022).

Tests connect to DynamoDB Local via endpoint_url="http://localhost:8000".
Start DynamoDB Local before running:
    docker run -d -p 8000:8000 amazon/dynamodb-local:latest

Tests are automatically skipped when DynamoDB Local is not available.
"""

import asyncio
import socket
import uuid
from decimal import Decimal
from typing import AsyncGenerator

import pytest

from asyncdb.drivers.dynamodb import dynamodb, dynamodbPool


# ── Connectivity check ────────────────────────────────────────────────

DYNAMODB_LOCAL_ENDPOINT = "http://localhost:8000"

DYNAMO_PARAMS = {
    "region_name": "us-east-1",
    "endpoint_url": DYNAMODB_LOCAL_ENDPOINT,
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
}


def _is_dynamodb_local_running() -> bool:
    """Check if DynamoDB Local is reachable on localhost:8000."""
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            return True
    except (OSError, ConnectionRefusedError):
        return False


DYNAMODB_LOCAL_AVAILABLE = _is_dynamodb_local_running()

pytestmark = pytest.mark.skipif(
    not DYNAMODB_LOCAL_AVAILABLE,
    reason="DynamoDB Local not running on localhost:8000",
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
async def dynamo() -> AsyncGenerator[dynamodb, None]:
    """Create a dynamodb driver connected to DynamoDB Local."""
    db = dynamodb(params=DYNAMO_PARAMS.copy())
    async with db as conn:
        yield conn


@pytest.fixture
async def test_table(dynamo: dynamodb) -> AsyncGenerator[str, None]:
    """Create a temporary test table and clean up after."""
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    await dynamo.create_table(
        table_name,
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        attribute_definitions=[{"AttributeName": "pk", "AttributeType": "S"}],
    )
    await dynamo.wait_for_table(table_name, timeout=30)
    yield table_name
    try:
        await dynamo.delete_table(table_name)
    except Exception:
        pass


@pytest.fixture
async def composite_table(dynamo: dynamodb) -> AsyncGenerator[str, None]:
    """Create a test table with composite key (pk + sk) and a GSI."""
    table_name = f"test_comp_{uuid.uuid4().hex[:8]}"
    await dynamo.create_table(
        table_name,
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi_index",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    await dynamo.wait_for_table(table_name, timeout=30)
    yield table_name
    try:
        await dynamo.delete_table(table_name)
    except Exception:
        pass


# ── Connection Lifecycle Tests ────────────────────────────────────────


class TestConnectionLifecycle:
    """Tests for connection, close, and context manager."""

    @pytest.mark.asyncio
    async def test_connection_and_close(self):
        """Test manual connection() and close()."""
        db = dynamodb(params=DYNAMO_PARAMS.copy())
        await db.connection()
        assert db.is_connected() is True
        assert db._connection is not None
        await db.close()
        assert db.is_connected() is False
        assert db._connection is None

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async with works correctly."""
        async with dynamodb(params=DYNAMO_PARAMS.copy()) as db:
            assert db.is_connected() is True
            result = await db.test_connection()
            assert result is True
        assert db.is_connected() is False

    @pytest.mark.asyncio
    async def test_test_connection(self, dynamo: dynamodb):
        """Test test_connection returns True."""
        result = await dynamo.test_connection()
        assert result is True

    @pytest.mark.asyncio
    async def test_use_sets_default_table(self, dynamo: dynamodb):
        """Test use() sets the default table."""
        await dynamo.use("my_table")
        assert dynamo._default_table == "my_table"


# ── CRUD Tests ────────────────────────────────────────────────────────


class TestCRUD:
    """Tests for get, set, delete, write, update, fetch_one."""

    @pytest.mark.asyncio
    async def test_set_and_get(self, dynamo: dynamodb, test_table: str):
        """Test writing and reading a single item."""
        item = {"pk": "key1", "data": "hello", "count": 42}
        result = await dynamo.set(test_table, item)
        assert result is True

        fetched = await dynamo.get(test_table, {"pk": "key1"})
        assert fetched is not None
        assert fetched["pk"] == "key1"
        assert fetched["data"] == "hello"
        assert fetched["count"] == Decimal("42")  # DynamoDB returns Decimal

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, dynamo: dynamodb, test_table: str):
        """Test get returns None for missing items."""
        result = await dynamo.get(test_table, {"pk": "nonexistent"})
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, dynamo: dynamodb, test_table: str):
        """Test deleting an item."""
        await dynamo.set(test_table, {"pk": "del_key", "val": "x"})
        result = await dynamo.delete(test_table, {"pk": "del_key"})
        assert result is True

        fetched = await dynamo.get(test_table, {"pk": "del_key"})
        assert fetched is None

    @pytest.mark.asyncio
    async def test_write_alias(self, dynamo: dynamodb, test_table: str):
        """Test write() is an alias for set()."""
        result = await dynamo.write(test_table, {"pk": "write_key", "v": 1})
        assert result is True
        fetched = await dynamo.get(test_table, {"pk": "write_key"})
        assert fetched is not None

    @pytest.mark.asyncio
    async def test_update(self, dynamo: dynamodb, test_table: str):
        """Test updating an item with UpdateExpression."""
        await dynamo.set(test_table, {"pk": "upd1", "count": 10, "name": "alice"})
        updated = await dynamo.update(
            test_table,
            key={"pk": "upd1"},
            update_expression="SET #n = :newname, #c = #c + :inc",
            expression_values={":newname": "bob", ":inc": 5},
            ExpressionAttributeNames={"#n": "name", "#c": "count"},
        )
        assert updated is not None
        assert updated["name"] == "bob"
        assert updated["count"] == Decimal("15")

    @pytest.mark.asyncio
    async def test_fetch_one(self, dynamo: dynamodb, test_table: str):
        """Test fetch_one as alias for get."""
        await dynamo.set(test_table, {"pk": "f1", "data": "fetch"})
        result = await dynamo.fetch_one(test_table, key={"pk": "f1"})
        assert result is not None
        assert result["data"] == "fetch"

    @pytest.mark.asyncio
    async def test_default_table(self, dynamo: dynamodb, test_table: str):
        """Test CRUD with default table."""
        await dynamo.use(test_table)
        await dynamo.set(item={"pk": "dt1", "v": "default"})
        fetched = await dynamo.get(key={"pk": "dt1"})
        assert fetched["v"] == "default"


# ── Query & Scan Tests ────────────────────────────────────────────────


class TestQueryScan:
    """Tests for query, queryrow, fetch_all."""

    @pytest.mark.asyncio
    async def test_query_with_key_condition(
        self, dynamo: dynamodb, composite_table: str
    ):
        """Test query with KeyConditionExpression."""
        # Insert items
        for i in range(5):
            await dynamo.set(composite_table, {
                "pk": "user1", "sk": f"item_{i:03d}",
                "gsi_pk": "group_a", "data": f"val_{i}",
            })

        results = await dynamo.query(
            composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user1"}},
        )
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_queryrow(self, dynamo: dynamodb, composite_table: str):
        """Test queryrow returns single item."""
        await dynamo.set(composite_table, {
            "pk": "qr_user", "sk": "001",
            "gsi_pk": "g", "data": "single",
        })
        result = await dynamo.queryrow(
            composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "qr_user"}},
        )
        assert result is not None
        assert result["pk"] == "qr_user"

    @pytest.mark.asyncio
    async def test_fetch_all_scan(self, dynamo: dynamodb, test_table: str):
        """Test full table scan."""
        for i in range(10):
            await dynamo.set(test_table, {"pk": f"scan_{i}", "idx": i})

        results = await dynamo.fetch_all(test_table)
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_query_with_gsi(self, dynamo: dynamodb, composite_table: str):
        """Test query using GSI IndexName."""
        for i in range(3):
            await dynamo.set(composite_table, {
                "pk": f"gsi_user_{i}", "sk": "001",
                "gsi_pk": "shared_group", "data": f"v{i}",
            })

        results = await dynamo.query(
            composite_table,
            IndexName="gsi_index",
            KeyConditionExpression="gsi_pk = :gpk",
            ExpressionAttributeValues={":gpk": {"S": "shared_group"}},
        )
        assert len(results) == 3


# ── Pagination Tests ──────────────────────────────────────────────────


class TestPagination:
    """Test auto-pagination for query and scan."""

    @pytest.mark.asyncio
    async def test_scan_pagination(self, dynamo: dynamodb, test_table: str):
        """Test that scan paginates through all items."""
        # Insert enough items to potentially trigger pagination
        items = [{"pk": f"page_{i:04d}", "data": f"val_{i}"} for i in range(30)]
        for item in items:
            await dynamo.set(test_table, item)

        results = await dynamo.fetch_all(test_table)
        assert len(results) == 30


# ── PartiQL Tests ─────────────────────────────────────────────────────


class TestPartiQL:
    """Tests for PartiQL support."""

    @pytest.mark.asyncio
    async def test_partiql_select(self, dynamo: dynamodb, test_table: str):
        """Test PartiQL SELECT statement."""
        await dynamo.set(test_table, {"pk": "pql1", "name": "alice"})
        await dynamo.set(test_table, {"pk": "pql2", "name": "bob"})

        results = await dynamo.partiql(
            f'SELECT * FROM "{test_table}" WHERE pk = ?',
            parameters=["pql1"],
        )
        assert len(results) == 1
        assert results[0]["name"] == "alice"

    @pytest.mark.asyncio
    async def test_partiql_insert(self, dynamo: dynamodb, test_table: str):
        """Test PartiQL INSERT statement."""
        await dynamo.partiql(
            f"INSERT INTO \"{test_table}\" VALUE {{'pk': ?, 'name': ?}}",
            parameters=["pql_ins", "charlie"],
        )
        fetched = await dynamo.get(test_table, {"pk": "pql_ins"})
        assert fetched is not None
        assert fetched["name"] == "charlie"

    @pytest.mark.asyncio
    async def test_execute_partiql_routing(self, dynamo: dynamodb, test_table: str):
        """Test that execute() routes PartiQL strings correctly."""
        await dynamo.set(test_table, {"pk": "exec_pql", "v": "test"})
        results = await dynamo.execute(
            f'SELECT * FROM "{test_table}" WHERE pk = ?',
            ["exec_pql"],
        )
        assert len(results) == 1
        assert results[0]["v"] == "test"

    @pytest.mark.asyncio
    async def test_is_partiql_detection(self, dynamo: dynamodb):
        """Test _is_partiql detection."""
        assert dynamo._is_partiql("SELECT * FROM tbl") is True
        assert dynamo._is_partiql("INSERT INTO tbl VALUE ...") is True
        assert dynamo._is_partiql("UPDATE tbl SET ...") is True
        assert dynamo._is_partiql("DELETE FROM tbl WHERE ...") is True
        assert dynamo._is_partiql("  SELECT * FROM tbl") is True
        assert dynamo._is_partiql({"pk": "val"}) is False
        assert dynamo._is_partiql("not a query") is False
        assert dynamo._is_partiql(123) is False


# ── Batch Operations Tests ────────────────────────────────────────────


class TestBatchOperations:
    """Tests for write_batch and get_batch."""

    @pytest.mark.asyncio
    async def test_write_batch(self, dynamo: dynamodb, test_table: str):
        """Test batch writing items."""
        items = [{"pk": f"batch_{i:03d}", "val": i} for i in range(10)]
        result = await dynamo.write_batch(test_table, items)
        assert result is True

        # Verify items were written
        for i in range(10):
            fetched = await dynamo.get(test_table, {"pk": f"batch_{i:03d}"})
            assert fetched is not None

    @pytest.mark.asyncio
    async def test_write_batch_chunking(self, dynamo: dynamodb, test_table: str):
        """Test batch write with >25 items (auto-chunking)."""
        items = [{"pk": f"chunk_{i:04d}", "data": f"v{i}"} for i in range(30)]
        result = await dynamo.write_batch(test_table, items)
        assert result is True

        all_items = await dynamo.fetch_all(test_table)
        assert len(all_items) == 30

    @pytest.mark.asyncio
    async def test_get_batch(self, dynamo: dynamodb, test_table: str):
        """Test batch get items."""
        items = [{"pk": f"bg_{i:03d}", "val": i} for i in range(10)]
        await dynamo.write_batch(test_table, items)

        keys = [{"pk": f"bg_{i:03d}"} for i in range(10)]
        results = await dynamo.get_batch(test_table, keys)
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_get_batch_empty(self, dynamo: dynamodb, test_table: str):
        """Test batch get with empty keys list."""
        results = await dynamo.get_batch(test_table, [])
        assert results == []


# ── DDL Tests ─────────────────────────────────────────────────────────


class TestDDL:
    """Tests for DDL and table management."""

    @pytest.mark.asyncio
    async def test_create_and_delete_table(self, dynamo: dynamodb):
        """Test creating and deleting a table."""
        table_name = f"test_ddl_{uuid.uuid4().hex[:8]}"
        desc = await dynamo.create_table(
            table_name,
            key_schema=[{"AttributeName": "id", "KeyType": "HASH"}],
            attribute_definitions=[{"AttributeName": "id", "AttributeType": "S"}],
        )
        assert desc.get("TableName") == table_name
        await dynamo.wait_for_table(table_name, timeout=30)

        # Delete
        result = await dynamo.delete_table(table_name)
        assert result is True

    @pytest.mark.asyncio
    async def test_describe_table(self, dynamo: dynamodb, test_table: str):
        """Test describing a table."""
        desc = await dynamo.describe_table(test_table)
        assert desc.get("TableName") == test_table
        assert desc.get("TableStatus") == "ACTIVE"

    @pytest.mark.asyncio
    async def test_list_tables(self, dynamo: dynamodb, test_table: str):
        """Test listing tables."""
        table_list = await dynamo.tables()
        assert isinstance(table_list, list)
        assert test_table in table_list

    @pytest.mark.asyncio
    async def test_table_alias(self, dynamo: dynamodb, test_table: str):
        """Test table() as alias for describe_table."""
        desc = await dynamo.table(test_table)
        assert desc.get("TableName") == test_table

    @pytest.mark.asyncio
    async def test_wait_for_table(self, dynamo: dynamodb, test_table: str):
        """Test wait_for_table with ACTIVE table."""
        result = await dynamo.wait_for_table(test_table, timeout=10)
        assert result is True


# ── Index Management Tests ────────────────────────────────────────────


class TestIndexManagement:
    """Tests for GSI/LSI management."""

    @pytest.mark.asyncio
    async def test_list_indexes_with_gsi(
        self, dynamo: dynamodb, composite_table: str
    ):
        """Test list_indexes returns GSI info."""
        indexes = await dynamo.list_indexes(composite_table)
        assert len(indexes) >= 1
        gsi_names = [idx["IndexName"] for idx in indexes if idx["type"] == "GSI"]
        assert "gsi_index" in gsi_names

    @pytest.mark.asyncio
    async def test_list_indexes_empty(self, dynamo: dynamodb, test_table: str):
        """Test list_indexes on table with no indexes."""
        indexes = await dynamo.list_indexes(test_table)
        assert indexes == []


# ── Pool Driver Tests ─────────────────────────────────────────────────


class TestPool:
    """Tests for dynamodbPool."""

    @pytest.mark.asyncio
    async def test_pool_connect_disconnect(self):
        """Test pool connect and disconnect."""
        pool = dynamodbPool(params=DYNAMO_PARAMS.copy())
        await pool.connect()
        assert pool.is_connected() is True

        await pool.disconnect()
        assert pool.is_connected() is False

    @pytest.mark.asyncio
    async def test_pool_acquire_release(self):
        """Test acquiring and releasing connections."""
        pool = dynamodbPool(params=DYNAMO_PARAMS.copy())
        await pool.connect()

        conn = await pool.acquire()
        assert isinstance(conn, dynamodb)
        assert conn.is_connected() is True

        # Verify connection works
        table_list = await conn.tables()
        assert isinstance(table_list, list)

        await pool.release(conn)
        assert conn.is_connected() is False

        await pool.disconnect()

    @pytest.mark.asyncio
    async def test_pool_multiple_acquire(self):
        """Test acquiring multiple connections."""
        pool = dynamodbPool(params=DYNAMO_PARAMS.copy())
        await pool.connect()

        conn1 = await pool.acquire()
        conn2 = await pool.acquire()
        assert conn1 is not conn2
        assert conn1.is_connected() is True
        assert conn2.is_connected() is True

        await pool.release(conn1)
        await pool.release(conn2)
        await pool.disconnect()

    @pytest.mark.asyncio
    async def test_pool_context_manager(self):
        """Test pool async context manager."""
        pool = dynamodbPool(params=DYNAMO_PARAMS.copy())
        await pool.connect()

        conn = await pool.acquire()
        assert conn.is_connected() is True
        await pool.release(conn)

        await pool.disconnect()
        assert pool.is_connected() is False


# ── Error Handling Tests ──────────────────────────────────────────────


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_get_missing_table(self, dynamo: dynamodb):
        """Test error when accessing non-existent table."""
        from asyncdb.exceptions import DriverError

        with pytest.raises(DriverError):
            await dynamo.get("nonexistent_table_12345", {"pk": "x"})

    @pytest.mark.asyncio
    async def test_empty_key_raises(self, dynamo: dynamodb, test_table: str):
        """Test EmptyStatement when key is missing."""
        from asyncdb.exceptions import EmptyStatement

        with pytest.raises(EmptyStatement):
            await dynamo.get(test_table, key=None)

    @pytest.mark.asyncio
    async def test_empty_item_raises(self, dynamo: dynamodb, test_table: str):
        """Test EmptyStatement when item is empty."""
        from asyncdb.exceptions import EmptyStatement

        with pytest.raises(EmptyStatement):
            await dynamo.set(test_table, item=None)

    @pytest.mark.asyncio
    async def test_no_table_raises(self, dynamo: dynamodb):
        """Test EmptyStatement when no table provided and no default."""
        from asyncdb.exceptions import EmptyStatement

        dynamo._default_table = None
        with pytest.raises(EmptyStatement):
            await dynamo.get(key={"pk": "x"})

    @pytest.mark.asyncio
    async def test_prepare_raises(self, dynamo: dynamodb):
        """Test prepare() raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            await dynamo.prepare("SELECT 1")


# ── Type Marshalling Tests ────────────────────────────────────────────


class TestTypeMarshalling:
    """Tests for Python ↔ DynamoDB type conversion."""

    @pytest.mark.asyncio
    async def test_type_roundtrip(self, dynamo: dynamodb, test_table: str):
        """Test that Python types survive serialization round-trip."""
        item = {
            "pk": "types_test",
            "str_val": "hello",
            "int_val": 42,
            "float_val": Decimal("3.14"),  # DynamoDB uses Decimal
            "bool_val": True,
            "null_val": None,
            "list_val": [1, "two", True],
            "map_val": {"nested": "dict", "num": 99},
        }
        await dynamo.set(test_table, item)
        fetched = await dynamo.get(test_table, {"pk": "types_test"})

        assert fetched["str_val"] == "hello"
        assert fetched["int_val"] == Decimal("42")
        assert fetched["float_val"] == Decimal("3.14")
        assert fetched["bool_val"] is True
        assert fetched["null_val"] is None
        assert len(fetched["list_val"]) == 3
        assert fetched["map_val"]["nested"] == "dict"

    def test_serialize_deserialize_roundtrip(self):
        """Test _serialize and _deserialize are inverse operations."""
        db = dynamodb.__new__(dynamodb)
        from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
        db._serializer_type = TypeSerializer()
        db._deserializer_type = TypeDeserializer()

        original = {"key": "value", "num": 42, "flag": True}
        serialized = db._serialize(original)
        deserialized = db._deserialize(serialized)

        assert deserialized["key"] == "value"
        assert deserialized["num"] == Decimal("42")
        assert deserialized["flag"] is True

    def test_chunk_items(self):
        """Test _chunk_items splits correctly."""
        db = dynamodb.__new__(dynamodb)
        items = list(range(7))
        chunks = db._chunk_items(items, chunk_size=3)
        assert len(chunks) == 3
        assert chunks[0] == [0, 1, 2]
        assert chunks[1] == [3, 4, 5]
        assert chunks[2] == [6]
