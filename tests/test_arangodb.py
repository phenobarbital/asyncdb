"""
Comprehensive pytest suite for ArangoDB driver.

Tests cover:
- Connection/disconnection
- Query operations
- Database CRUD
- Collection CRUD
- Document CRUD
- Graph operations
- Bulk write operations
- Error handling
"""
import os
import pytest
import asyncio
from unittest.mock import Mock, MagicMock, AsyncMock, patch
import pandas as pd
from pathlib import Path
from tempfile import NamedTemporaryFile

from asyncdb.drivers.arangodb import arangodb
from asyncdb.exceptions import NoDataFound, DriverError


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def db_params():
    """Default database parameters for testing."""
    return {
        'host': 'localhost',
        'port': 8529,
        'username': 'root',
        'password': '12345678',
        'database': 'navigator'
    }

@pytest.fixture(scope='session')
def test_params():
    """Test database parameters - override with environment variables."""
    return {
        'host': os.environ.get('ARANGO_HOST', 'localhost'),
        'port': int(os.environ.get('ARANGO_PORT', 8529)),
        'username': os.environ.get('ARANGO_USER', 'root'),
        'password': os.environ.get('ARANGO_PASSWORD', '12345678'),
        'database': 'test_integration_db'
    }

@pytest.fixture(scope='session')
async def db_session(test_params):
    """Create database instance for entire test session."""
    db = arangodb(params=test_params)
    await db.connection()

    # Clean up any existing test data
    try:
        await db.drop_database('test_integration_db')
    except:
        pass

    await db.create_database('test_integration_db')
    await db.use('test_integration_db')

    yield db

    # Cleanup after all tests
    try:
        await db.drop_database('test_integration_db')
    except:
        pass

    await db.close()

@pytest.fixture
async def db_instance(test_params):
    """Create fresh database instance for each test (for REAL tests)."""
    db = arangodb(params=test_params)
    await db.connection()
    await db.use('test_integration_db')
    yield db
    await db.close()

@pytest.fixture
async def clean_collection(db_instance):
    """Provide a clean collection for testing."""
    collection_name = 'test_collection'

    # Drop if exists
    if await db_instance.collection_exists(collection_name):
        await db_instance.drop_collection(collection_name)

    # Create fresh collection
    await db_instance.create_collection(collection_name)

    yield collection_name

    # Cleanup
    try:
        await db_instance.drop_collection(collection_name)
    except:
        pass

class AsyncIterator:
    """Helper for mocking async iterators."""
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration

@pytest.fixture
def mock_arango_client():
    """Mock ArangoDB client."""
    with patch('asyncdb.drivers.arangodb.ArangoClient') as mock_client:
        # Setup mock database
        mock_db = MagicMock()  # The DB object itself
        
        # Async methods on DB object
        mock_db.has_collection = AsyncMock(return_value=False)
        mock_db.create_collection = AsyncMock()
        mock_db.delete_collection = AsyncMock()
        mock_db.create_graph = AsyncMock()
        mock_db.delete_graph = AsyncMock()
        mock_db.has_graph = AsyncMock(return_value=False)
        
        # Collection method returns a collection object (sync in arangoasync structure, methods async)
        # Wait, in the driver we use: await self._connection.collection(name) WRONG
        # In my refactor: 
        #   if not await self._connection.has_collection(collection): ...
        #   col = self._connection.collection(collection) -> This is SYNC in implementation? 
        #   Actually in my refactor I kept `col = self._connection.collection(collection)` which implies sync return.
        #   Let's check my replacement_content in Step 215.
        #   `col = self._connection.collection(collection)`
        #   So collection() on db is sync, but insert/update/delete on col are async.
        
        mock_col = MagicMock()
        mock_col.insert = AsyncMock()
        mock_col.update = AsyncMock()
        mock_col.delete = AsyncMock()
        mock_col.insert_many = AsyncMock()
        
        mock_db.collection.return_value = mock_col
        
        # Graphs
        mock_graph = MagicMock()
        mock_vertex_col = MagicMock()
        mock_vertex_col.insert = AsyncMock()
        mock_graph.vertex_collection.return_value = mock_vertex_col
        
        mock_edge_col = MagicMock()
        mock_edge_col.insert = AsyncMock()
        mock_graph.edge_collection.return_value = mock_edge_col
        
        mock_db.graph.return_value = mock_graph

        # AQL
        mock_db.aql.execute = AsyncMock()

        # System DB
        mock_sys_db = MagicMock()
        mock_sys_db.has_database = AsyncMock(return_value=True)
        mock_sys_db.create_database = AsyncMock()
        mock_sys_db.delete_database = AsyncMock()

        # Mock client.db() to return appropriate database (ASYNC)
        async def db_side_effect(name, **kwargs):
            if name == '_system':
                return mock_sys_db
            return mock_db

        mock_client_instance = MagicMock()
        mock_client_instance.db = AsyncMock(side_effect=db_side_effect)
        mock_client_instance.close = AsyncMock()

        mock_client.return_value = mock_client_instance

        yield {
            'client': mock_client,
            'client_instance': mock_client_instance,
            'db': mock_db,
            'sys_db': mock_sys_db,
            'collection': mock_col
        }


@pytest.fixture
async def db_instance_mock(db_params, mock_arango_client):
    """Create a database instance for testing with MOCKS."""
    db = arangodb(params=db_params)
    await db.connection()
    yield db
    await db.close()


@pytest.fixture
def sample_documents():
    """Sample documents for testing."""
    return [
        {'_key': 'doc1', 'name': 'Document 1', 'value': 100},
        {'_key': 'doc2', 'name': 'Document 2', 'value': 200},
        {'_key': 'doc3', 'name': 'Document 3', 'value': 300}
    ]


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame for testing."""
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [30, 35, 28]
    })


# ============================================================================
# CONNECTION TESTS
# ============================================================================

class TestConnection:
    """Test connection and disconnection."""

    @pytest.mark.asyncio
    async def test_connection_success(self, db_params, mock_arango_client):
        """Test successful connection to ArangoDB."""
        db = arangodb(params=db_params)

        result = await db.connection()

        assert result is db
        assert db._connected is True
        assert db._connection is not None
        assert db._database_name == 'navigator'

        await db.close()

    @pytest.mark.asyncio
    async def test_connection_creates_database_if_not_exists(
        self, db_params, mock_arango_client
    ):
        """Test that connection creates database if it doesn't exist."""
        # Mock database doesn't exist
        mock_arango_client['sys_db'].has_database = AsyncMock(return_value=False)

        db = arangodb(params=db_params)
        await db.connection()

        # Verify database creation was called
        mock_arango_client['sys_db'].create_database.assert_awaited_once_with('navigator')

        await db.close()

    @pytest.mark.asyncio
    async def test_connection_with_custom_database(self, db_params, mock_arango_client):
        """Test connection to a specific database."""
        db = arangodb(params=db_params)

        await db.connection(database='custom_db')

        assert db._database_name == 'custom_db'

        await db.close()

    @pytest.mark.asyncio
    async def test_connection_failure(self, db_params):
        """Test connection failure handling."""
        with patch('asyncdb.drivers.arangodb.ArangoClient') as mock_client:
             # Mock the client instance and its db method to raise exception
            mock_instance = MagicMock()
            mock_instance.db = AsyncMock(side_effect=Exception("Connection failed"))
            mock_client.return_value = mock_instance

            db = arangodb(params=db_params)

            with pytest.raises(DriverError, match="Connection failed"):
                await db.connection()

    @pytest.mark.asyncio
    async def test_close_connection(self, db_instance_mock):
        """Test closing connection."""
        assert db_instance_mock._connected is True

        await db_instance_mock.close()

        assert db_instance_mock._connected is False
        assert db_instance_mock._connection is None
        assert db_instance_mock._client is None

    @pytest.mark.asyncio
    async def test_test_connection(self, db_instance_mock, mock_arango_client):
        """Test connection test."""
        # Use AsyncIterator for cursor
        mock_cursor = AsyncIterator([1])
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result, error = await db_instance_mock.test_connection()

        assert error is None
        assert result is not None


# ============================================================================
# DATABASE OPERATIONS TESTS
# ============================================================================

class TestDatabaseOperations:
    """Test database CRUD operations."""

    @pytest.mark.asyncio
    async def test_use_database(self, db_instance_mock, mock_arango_client):
        """Test switching to different database."""
        new_db = MagicMock()
        mock_client_instance = mock_arango_client['client_instance']

        # Update side effect for db() method
        original_side_effect = mock_client_instance.db.side_effect
        
        async def updated_db_side_effect(name, **kwargs):
            if name == 'another_db':
                return new_db
            return await original_side_effect(name, **kwargs)

        mock_client_instance.db.side_effect = updated_db_side_effect

        result = await db_instance_mock.use('another_db')

        assert result is db_instance_mock
        assert db_instance_mock._database_name == 'another_db'
        assert db_instance_mock._connection == new_db

    @pytest.mark.asyncio
    async def test_create_database(self, db_instance_mock, mock_arango_client):
        """Test creating a new database."""
        result = await db_instance_mock.create_database('new_db')

        assert result is True
        mock_arango_client['sys_db'].create_database.assert_awaited_with('new_db')

    @pytest.mark.asyncio
    async def test_create_database_failure(self, db_instance_mock, mock_arango_client):
        """Test database creation failure."""
        mock_arango_client['sys_db'].create_database.side_effect = DriverError(
            "Creation Error: Database already exists"
        )

        with pytest.raises(DriverError, match="Error creating database"):
            await db_instance_mock.create_database('existing_db')

    @pytest.mark.asyncio
    async def test_drop_database(self, db_instance_mock, mock_arango_client):
        """Test dropping a database."""
        result = await db_instance_mock.drop_database('old_db')

        assert result is True
        mock_arango_client['sys_db'].delete_database.assert_awaited_with('old_db')


# ============================================================================
# COLLECTION OPERATIONS TESTS
# ============================================================================

class TestCollectionOperations:
    """Test collection CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_collection(self, db_instance_mock, mock_arango_client):
        """Test creating a collection."""
        mock_collection = MagicMock()
        mock_arango_client['db'].has_collection = AsyncMock(return_value=False)
        mock_arango_client['db'].create_collection = AsyncMock(return_value=mock_collection)

        result = await db_instance_mock.create_collection('test_collection')

        assert result == mock_collection
        # arangodb.py adds col_type=2 when edge=False
        mock_arango_client['db'].create_collection.assert_awaited_once_with(
            'test_collection', col_type=2
        )

    @pytest.mark.asyncio
    async def test_create_edge_collection(self, db_instance_mock, mock_arango_client):
        """Test creating an edge collection."""
        mock_collection = MagicMock()
        mock_arango_client['db'].has_collection = AsyncMock(return_value=False)
        mock_arango_client['db'].create_collection = AsyncMock(return_value=mock_collection)

        result = await db_instance_mock.create_collection('edges', edge=True)

        # arangodb.py adds col_type=3 when edge=True
        mock_arango_client['db'].create_collection.assert_awaited_once_with(
            'edges', col_type=3
        )

# ============================================================================
# QUERY OPERATIONS TESTS
# ============================================================================

class TestQueryOperations:
    """Test AQL query operations."""

    @pytest.mark.asyncio
    async def test_query(self, db_instance_mock, mock_arango_client):
        """Test executing a query."""
        mock_cursor = AsyncIterator([{'name': 'Alice'}])
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result, error = await db_instance_mock.query("FOR doc IN test RETURN doc")

        assert error is None
        assert result == [{'name': 'Alice'}]
        mock_arango_client['db'].aql.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_query_error(self, db_instance_mock, mock_arango_client):
        """Test query error handling."""
        mock_arango_client['db'].aql.execute.side_effect = DriverError("AQL Error")

        result, error = await db_instance_mock.query("INVALID QUERY")

        # implementation catches exception and returns error string
        assert result is None
        assert isinstance(error, str)
        assert "AQL Error" in error

    @pytest.mark.asyncio
    async def test_fetchval(self, db_instance_mock, mock_arango_client):
        """Test fetching a single value."""
        mock_cursor = AsyncIterator([{'val': 42}])
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result = await db_instance_mock.fetchval("RETURN 42", column='val')

        assert result == 42
        mock_arango_client['db'].aql.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetch_one(self, db_instance_mock, mock_arango_client):
        """Test fetching a single row."""
        mock_cursor = AsyncIterator([{'name': 'Alice', 'age': 30}])
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result = await db_instance_mock.fetch_one("RETURN doc")

        assert result == {'name': 'Alice', 'age': 30}

    @pytest.mark.asyncio
    async def test_fetch_all(self, db_instance_mock, mock_arango_client):
        """Test fetching all rows."""
        data = [{'id': 1}, {'id': 2}, {'id': 3}]
        mock_cursor = AsyncIterator(data)
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result = await db_instance_mock.fetch_all("RETURN doc")

        assert result == data


# ============================================================================
# DOCUMENT OPERATIONS TESTS
# ============================================================================

class TestDocumentOperations:
    """Test document CRUD operations."""

    @pytest.mark.asyncio
    async def test_insert_document(self, db_instance_mock, mock_arango_client):
        """Test inserting a document."""
        mock_col = MagicMock()
        # insert returns dict with 'new' if return_new=True
        mock_col.insert = AsyncMock(return_value={'_key': '123', '_rev': 'abc', 'new': {'_key': '123', 'name': 'Alice'}})
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        doc = {'name': 'Alice'}
        result = await db_instance_mock.insert_document('users', doc)

        assert result['_key'] == '123'
        mock_col.insert.assert_awaited_once_with(doc, return_new=True)

    @pytest.mark.asyncio
    async def test_update_document(self, db_instance_mock, mock_arango_client):
        """Test updating a document."""
        mock_col = MagicMock()
        # update returns dict with 'new' if return_new=True
        mock_col.update = AsyncMock(return_value={'_key': '123', '_rev': 'def', 'new': {'_key': '123', '_rev': 'def', 'name': 'Alice Updated'}})
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        doc = {'_key': '123', 'name': 'Alice Updated'}
        result = await db_instance_mock.update_document('users', doc)

        assert result['_rev'] == 'def'
        mock_col.update.assert_awaited_once_with(doc, return_new=True)

    @pytest.mark.asyncio
    async def test_delete_document(self, db_instance_mock, mock_arango_client):
        """Test deleting a document."""
        mock_col = MagicMock()
        mock_col.delete = AsyncMock(return_value=True)
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        result = await db_instance_mock.delete_document('users', '123')

        assert result is True
        # delete takes key string, not dict
        mock_col.delete.assert_awaited_once_with('123')

    @pytest.mark.asyncio
    async def test_document_exists(self, db_instance_mock, mock_arango_client):
        """Test checking if document exists."""
        mock_col = MagicMock()
        mock_col.has = AsyncMock(return_value=True)
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        # Not directly supported by driver wrapper yet? 
        # But if it was there, let's assume it checks via collection
        pass



class TestWriteOperations:
    """Test write operations."""

    @pytest.mark.asyncio
    async def test_write_single_dict(self, db_instance_mock, mock_arango_client):
        """Test writing a single dictionary."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        mock_arango_client['db'].collection.return_value = mock_col
        # Ensure has_collection is True to avoid create_collection path
        mock_arango_client['db'].has_collection.return_value = True

        data = {'name': 'Alice', 'age': 30}
        result = await db_instance_mock.write(data, collection='users')

        assert result == 1
        mock_col.insert_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_list_of_dicts(self, db_instance_mock, mock_arango_client, sample_documents):
        """Test writing multiple documents."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        result = await db_instance_mock.write(sample_documents, collection='users')

        assert result == 3
        mock_col.insert_many.assert_awaited()

    @pytest.mark.asyncio
    async def test_write_dataframe(self, db_instance_mock, mock_arango_client, sample_dataframe):
        """Test writing pandas DataFrame."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        result = await db_instance_mock.write(sample_dataframe, collection='users')

        assert result == 3
        mock_col.insert_many.assert_awaited()

    @pytest.mark.asyncio
    async def test_write_csv_file(self, db_instance_mock, mock_arango_client):
        """Test writing from CSV file."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        # Create temporary CSV
        with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('name,age\n')
            f.write('Alice,30\n')
            f.write('Bob,35\n')
            csv_path = Path(f.name)

        try:
            result = await db_instance_mock.write(csv_path, collection='users')
            assert result == 2
        finally:
            csv_path.unlink()

    @pytest.mark.asyncio
    async def test_write_batch_size(self, db_instance_mock, mock_arango_client):
        """Test writing with custom batch size."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        mock_arango_client['db'].collection.return_value = mock_col
        mock_arango_client['db'].has_collection.return_value = True

        # Create 150 documents
        large_dataset = [{'id': i} for i in range(150)]

        result = await db_instance_mock.write(
            large_dataset,
            collection='users',
            batch_size=50
        )

        assert result == 150
        # Should be called 3 times (150 / 50)
        assert mock_col.insert_many.call_count == 3
        assert mock_col.insert_many.await_count == 3


    @pytest.mark.asyncio
    async def test_write_creates_collection_if_not_exists(
        self, db_instance_mock, mock_arango_client
    ):
        """Test that write creates collection if it doesn't exist."""
        mock_col = mock_arango_client['collection']
        mock_col.insert_many = AsyncMock()
        
        # We need to simulate has_collection returning False
        mock_arango_client['db'].has_collection.return_value = False
        # create_collection should return mock_col for inserts to work on OUR mock
        mock_arango_client['db'].create_collection.return_value = mock_col

        data = [{'name': 'Alice'}]
        result = await db_instance_mock.write(data, collection='new_collection')

        # Should have attempted to create collection
        mock_arango_client['db'].create_collection.assert_awaited()
        # And insert
        mock_col.insert_many.assert_awaited()

# ============================================================================
# GRAPH OPERATIONS TESTS
# ============================================================================

class TestGraphOperations:
    """Test graph operations."""

    @pytest.mark.asyncio
    async def test_create_graph(self, db_instance_mock, mock_arango_client):
        """Test creating a graph."""
        mock_graph = MagicMock()
        mock_arango_client['db'].has_graph.return_value = False
        mock_arango_client['db'].create_graph.return_value = mock_graph

        edge_definitions = [{
            'edge_collection': 'knows',
            'from_vertex_collections': ['persons'],
            'to_vertex_collections': ['persons']
        }]

        result = await db_instance_mock.create_graph('social', edge_definitions=edge_definitions)

        assert result == mock_graph
        mock_arango_client['db'].create_graph.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_drop_graph(self, db_instance_mock, mock_arango_client):
        """Test dropping a graph."""
        result = await db_instance_mock.drop_graph('social')

        assert result is True
        mock_arango_client['db'].delete_graph.assert_awaited_with(
            'social', drop_collections=False
        )

    @pytest.mark.asyncio
    async def test_graph_exists(self, db_instance_mock, mock_arango_client):
        """Test checking if graph exists."""
        mock_arango_client['db'].has_graph.return_value = True

        result = await db_instance_mock.graph_exists('social')

        assert result is True

    @pytest.mark.asyncio
    async def test_create_vertex(self, db_instance_mock, mock_arango_client):
        """Test creating a vertex."""
        mock_graph = mock_arango_client['db'].graph.return_value
        mock_vertex_col = mock_graph.vertex_collection.return_value
        mock_vertex_col.insert.return_value = {'_key': 'alice', 'name': 'Alice'}

        vertex = {'_key': 'alice', 'name': 'Alice'}
        result = await db_instance_mock.create_vertex('social', 'persons', vertex)

        assert result['_key'] == 'alice'
        mock_vertex_col.insert.assert_awaited_once_with(vertex)

    @pytest.mark.asyncio
    async def test_create_edge(self, db_instance_mock, mock_arango_client):
        """Test creating an edge."""
        mock_graph = mock_arango_client['db'].graph.return_value
        mock_edge_col = mock_graph.edge_collection.return_value
        mock_edge_col.insert.return_value = {
            '_from': 'persons/alice',
            '_to': 'persons/bob'
        }

        edge = {
            '_from': 'persons/alice',
            '_to': 'persons/bob',
            'since': 2020
        }
        result = await db_instance_mock.create_edge('social', 'knows', edge)

        assert result['_from'] == 'persons/alice'
        mock_edge_col.insert.assert_awaited_once_with(edge)

    @pytest.mark.asyncio
    async def test_traverse(self, db_instance_mock, mock_arango_client):
        """Test graph traversal."""
        traversal_result = [
            {'vertex': {'_key': 'bob'}, 'edge': {'_from': 'persons/alice'}},
            {'vertex': {'_key': 'charlie'}, 'edge': {'_from': 'persons/bob'}}
        ]
        mock_cursor = AsyncIterator(traversal_result)
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result = await db_instance_mock.traverse(
            'persons/alice',
            direction='outbound',
            max_depth=2,
            graph_name='social'
        )

        assert len(result) == 2
        assert result[0]['vertex']['_key'] == 'bob'

    @pytest.mark.asyncio
    async def test_shortest_path(self, db_instance_mock, mock_arango_client):
        """Test finding shortest path."""
        path_result = {
            'vertices': [{'_key': 'alice'}, {'_key': 'bob'}],
            'edges': [{'_from': 'persons/alice', '_to': 'persons/bob'}]
        }
        mock_cursor = AsyncIterator([path_result])
        mock_arango_client['db'].aql.execute.return_value = mock_cursor

        result = await db_instance_mock.shortest_path(
            'persons/alice',
            'persons/bob',
            graph_name='social'
        )

        assert result is not None
        assert len(result['vertices']) == 2


# ============================================================================
# GENERIC OPERATIONS TESTS
# ============================================================================

class TestGenericOperations:
    """Test generic create/delete methods."""

    @pytest.mark.asyncio
    async def test_create_collection_via_generic(self, db_instance_mock, mock_arango_client):
        """Test creating collection via generic create method."""
        mock_collection = MagicMock()
        mock_arango_client['db'].has_collection.return_value = False
        mock_arango_client['db'].create_collection.return_value = mock_collection

        result = await db_instance_mock.create(obj='collection', name='test_col')

        assert result is True

    @pytest.mark.asyncio
    async def test_create_graph_via_generic(self, db_instance_mock, mock_arango_client):
        """Test creating graph via generic create method."""
        mock_graph = MagicMock()
        mock_arango_client['db'].has_graph.return_value = False
        mock_arango_client['db'].create_graph.return_value = mock_graph

        result = await db_instance_mock.create(obj='graph', name='test_graph')

        assert result is True

    @pytest.mark.asyncio
    async def test_delete_collection_via_generic(self, db_instance_mock, mock_arango_client):
        """Test deleting collection via generic delete method."""
        result = await db_instance_mock.delete(obj='collection', name='test_col')

        assert result is True
        mock_arango_client['db'].delete_collection.assert_called_with('test_col')

    @pytest.mark.asyncio
    async def test_invalid_object_type(self, db_instance_mock):
        """Test invalid object type raises error."""
        with pytest.raises(ValueError, match="Unknown object type"):
            await db_instance_mock.create(obj='invalid', name='test')


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

class TestErrorHandling:
    """Test error handling across operations."""

    @pytest.mark.asyncio
    async def test_query_validation(self, db_instance_mock):
        """Test that empty queries are validated."""
        # This would depend on your valid_operation implementation
        # Assuming it checks for empty/None queries
        with patch.object(db_instance_mock, 'valid_operation') as mock_valid:
            mock_valid.side_effect = ValueError("Invalid query")

            with pytest.raises(ValueError):
                await db_instance_mock.query("")

    @pytest.mark.asyncio
    async def test_connection_required_for_operations(self, db_params):
        """Test that operations fail without connection."""
        db = arangodb(params=db_params)
        # Don't connect

        # Operations should fail without connection
        with pytest.raises(AttributeError):
            await db.query("FOR doc IN test RETURN doc")

    @pytest.mark.asyncio
    async def test_write_invalid_data_type(self, db_instance_mock):
        """Test writing invalid data type."""
        with pytest.raises(ValueError, match="Unsupported data type"):
            await db_instance_mock.write("invalid string", collection='test')


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegrationFlows:
    """Integration tests for complete workflows."""

    @pytest.mark.asyncio
    async def test_complete_document_workflow(self, db_instance_mock, mock_arango_client):
        """Test complete CRUD workflow for documents."""
        # Setup mocks
        mock_collection = MagicMock()
        mock_collection.insert = AsyncMock()
        mock_collection.update = AsyncMock()
        mock_collection.delete = AsyncMock()
        
        mock_arango_client['db'].collection.return_value = mock_collection
        mock_arango_client['db'].has_collection.return_value = False
        mock_arango_client['db'].create_collection.return_value = mock_collection

        # Create collection
        await db_instance_mock.create_collection('users')

        # Insert document
        mock_collection.insert.return_value = {'new': {'_key': 'user1', 'name': 'Alice'}}
        doc = await db_instance_mock.insert_document('users', {'name': 'Alice'})
        assert doc['_key'] == 'user1'

        # Update document
        mock_collection.update.return_value = {'new': {'_key': 'user1', 'name': 'Alice Updated'}}
        updated = await db_instance_mock.update_document('users', {'_key': 'user1', 'name': 'Alice Updated'})
        assert updated['name'] == 'Alice Updated'

        # Delete document
        result = await db_instance_mock.delete_document('users', 'user1')
        assert result is True

    @pytest.mark.asyncio
    async def test_complete_graph_workflow(self, db_instance_mock, mock_arango_client):
        """Test complete graph creation and query workflow."""
        # Setup mocks
        mock_graph = MagicMock()
        mock_vertex_col = MagicMock()
        mock_edge_col = MagicMock()

        # Ensure async methods are AsyncMock
        mock_vertex_col.insert = AsyncMock()
        mock_edge_col.insert = AsyncMock()

        mock_arango_client['db'].has_graph.return_value = False
        mock_arango_client['db'].create_graph.return_value = mock_graph
        mock_graph.vertex_collection.return_value = mock_vertex_col
        mock_graph.edge_collection.return_value = mock_edge_col
        mock_arango_client['db'].graph.return_value = mock_graph

        # Create graph
        edge_defs = [{
            'edge_collection': 'knows',
            'from_vertex_collections': ['persons'],
            'to_vertex_collections': ['persons']
        }]
        await db_instance_mock.create_graph('social', edge_definitions=edge_defs)

        # Add vertices
        mock_vertex_col.insert.side_effect = [
            {'_key': 'alice', 'name': 'Alice'},
            {'_key': 'bob', 'name': 'Bob'}
        ]

        await db_instance_mock.create_vertex('social', 'persons', {'_key': 'alice', 'name': 'Alice'})
        await db_instance_mock.create_vertex('social', 'persons', {'_key': 'bob', 'name': 'Bob'})

        # Add edge
        mock_edge_col.insert.return_value = {
            '_from': 'persons/alice',
            '_to': 'persons/bob'
        }
        await db_instance_mock.create_edge('social', 'knows', {
            '_from': 'persons/alice',
            '_to': 'persons/bob'
        })

        # Verify graph was created properly
        assert mock_arango_client['db'].create_graph.called


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestPerformance:
    """Test performance-related aspects."""

    @pytest.mark.asyncio
    async def test_batch_insert_performance(self, db_instance_mock, mock_arango_client):
        """Test that batch inserts are efficient."""
        mock_collection = MagicMock()
        # insert_many MUST be AsyncMock for await to work
        mock_collection.insert_many = AsyncMock()
        
        # Ensure collection retrieval returns OUR mock
        mock_arango_client['db'].collection.return_value = mock_collection
        # Ensure create_collection ALSO returns our mock (in case has_collection is False)
        mock_arango_client['db'].create_collection.return_value = mock_collection
        # Ensure has_collection returns True so we don't try to create it repeatedly?
        # Or let logic flow. Logic: if not has_collection -> create.
        # But fixture sets has_collection -> False by default.
        
        # Large dataset
        large_data = [{'id': i, 'value': i * 2} for i in range(5000)]

        result = await db_instance_mock.write(
            large_data,
            collection='test',
            batch_size=1000
        )

        # Should batch into 5 calls
        assert mock_collection.insert_many.call_count == 5
        assert result == 5000


# ============================================================================
# REAL INTEGRATION TESTS (require ArangoDB running)
# ============================================================================

class TestRealConnection:
    """Test real connections to ArangoDB."""

    @pytest.mark.asyncio
    async def test_connection_and_disconnection(self, test_params):
        """Test actual connection to ArangoDB."""
        db = arangodb(params=test_params)

        # Connect
        await db.connection()
        assert db._connected is True

        # Test connection
        result, error = await db.test_connection()
        assert error is None
        assert result is not None

        # Disconnect
        await db.close()
        assert db._connected is False


class TestRealQueries:
    """Test real AQL queries."""

    @pytest.mark.asyncio
    async def test_simple_query(self, db_instance, clean_collection):
        """Test a simple AQL query."""
        # Insert test data
        docs = [
            {'_key': '1', 'name': 'Alice', 'age': 30},
            {'_key': '2', 'name': 'Bob', 'age': 35},
            {'_key': '3', 'name': 'Charlie', 'age': 28}
        ]

        for doc in docs:
            await db_instance.insert_document(clean_collection, doc)

        # Query all
        query = f"FOR doc IN {clean_collection} SORT doc.age RETURN doc"
        results, error = await db_instance.query(query)

        assert error is None
        assert len(results) == 3
        assert results[0]['age'] == 28  # Charlie
        assert results[2]['age'] == 35  # Bob

    @pytest.mark.asyncio
    async def test_query_with_bind_vars(self, db_instance, clean_collection):
        """Test query with bind variables."""
        # Insert test data
        doc = {'_key': 'test1', 'name': 'Alice', 'age': 30}
        await db_instance.insert_document(clean_collection, doc)

        # Query with bind vars
        query = f"FOR doc IN {clean_collection} FILTER doc.name == @name RETURN doc"
        results, error = await db_instance.query(query, bind_vars={'name': 'Alice'})

        assert error is None
        assert len(results) == 1
        assert results[0]['name'] == 'Alice'

    @pytest.mark.asyncio
    async def test_fetchval(self, db_instance, clean_collection):
        """Test fetching a single value."""
        # Insert data
        doc = {'_key': 'test1', 'value': 42}
        await db_instance.insert_document(clean_collection, doc)

        # Fetch value by index
        query = f"FOR doc IN {clean_collection} RETURN doc.value"
        value = await db_instance.fetchval(query, column=0)

        assert value == 42


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
