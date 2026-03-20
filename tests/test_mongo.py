import pytest
import re
from pathlib import Path
import uuid
import pandas as pd
from asyncdb import AsyncDB
from asyncdb.drivers.mongo import mongo
from asyncdb.exceptions import DriverError
import asyncio
from io import BytesIO


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


DRIVER='mongo'
MONGODB_VERSION='8.0.4'
PARAMS = {
    "host": "127.0.0.1",
    "port": "27017",
    "username": 'troc_mongodata',
    "password": '12345678',
    "database": "admin"
}


# Fixture to establish a connection to MongoDB
@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

# Fixture to create a dummy database
@pytest.fixture
async def dummy_db(event_loop):
    # Generate a unique database name using UUID
    db_name = f"test_db_{uuid.uuid4().hex}"
    db_driver = mongo(
        params={
            "host": "127.0.0.1",
            "port": 27017,
            "username": 'troc_mongodata',
            "password": '12345678',
        }
    )
    await db_driver.connection()
    assert db_driver.is_connected() is True
    await db_driver.use(db_name)
    yield db_driver
    # Teardown: Drop the dummy database after test
    await db_driver.drop_database(db_name)
    await db_driver.close()

# Fixture to create a sample collection within the dummy database
@pytest.fixture
async def sample_collection(dummy_db):
    collection_name = "test_collection"
    # No need to explicitly create the collection; it will be created upon first insert
    yield (dummy_db, collection_name)
    # Teardown: Drop the collection after test
    await dummy_db.drop_collection(collection_name)


# Fixture to create a sample pandas DataFrame with two rows
@pytest.fixture
def sample_dataframe():
    data = {
        'name': ['Alice', 'Bob'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)
    return df

pytestmark = pytest.mark.asyncio

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_pool_by_params(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    assert db.is_connected() is False

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(type(result) == dict)
    pytest.assume(result['version'] == MONGODB_VERSION)

    # Check if 'version' matches the pattern 'major.minor.patch'
    version_pattern = re.compile(r'^\d+\.\d+\.\d+$')
    assert 'version' in result, "Version key not found in result."
    assert version_pattern.match(result['version']), f"Version format is incorrect: {result['version']}"

    await db.close()
    pytest.assume(db.is_connected() is False)


@pytest.mark.asyncio
async def test_is_connected_success():
    async with mongo(
        params=PARAMS
    ) as db_driver:
        # Initially connected via context manager
        assert db_driver.is_connected() == True

@pytest.mark.asyncio
async def test_is_connected_failure():
    # Connection should fail
    try:
        async with mongo(
            params={
                "host": "localhost",
                "port": 27017,
                "database": "navigator",
                "username": "wrong_user",
                "password": "wrong_pass"
            }
        ) as db_driver:
            # Connection should fail
            await db_driver.connection()
    except DriverError as e:
        assert 'Authentication failed' in str(e)
    else:
        pytest.fail("DriverError was not raised when connecting with wrong credentials.")

@pytest.mark.asyncio
async def test_is_connected_after_close():
    db_driver = mongo(
        params=PARAMS
    )
    await db_driver.connection()
    assert db_driver.is_connected() == True
    await db_driver.close()
    assert db_driver.is_connected() == False


@pytest.mark.asyncio
async def test_write_dataframe(sample_collection, sample_dataframe):
    """
    Test writing a pandas DataFrame to a MongoDB collection.
    """
    db_driver, collection_name = sample_collection

    # Use the 'write' method to insert the DataFrame into the collection
    write_success = await db_driver.write(data=sample_dataframe, collection=collection_name)
    assert write_success is True, "Failed to write DataFrame to MongoDB collection."

    # Verify that the data was written correctly
    # Fetch all documents from the collection
    documents, error = await db_driver.fetch(collection_name)
    assert error is None, f"Error during fetch: {error}"
    assert len(documents) == 2, "Number of documents in the collection does not match the DataFrame rows."

    # Convert the fetched documents to a list of dictionaries
    fetched_data = documents  # Assuming 'documents' is already a list of dicts

    # Strip '_id' from each fetched document
    fetched_data_stripped = [{k: v for k, v in doc.items() if k != '_id'} for doc in fetched_data]

    # Convert the DataFrame to a list of dictionaries for comparison
    expected_data = sample_dataframe.to_dict('records')

    # Sort both lists for consistent ordering
    fetched_data_sorted = sorted(fetched_data_stripped, key=lambda x: x['name'])
    expected_data_sorted = sorted(expected_data, key=lambda x: x['name'])

    # Assert that the fetched data matches the expected data
    assert fetched_data_sorted == expected_data_sorted, "Data mismatch between DataFrame and MongoDB collection."

@pytest.mark.asyncio
async def test_drop_collection(sample_collection):
    """
    Test dropping a collection from the MongoDB database.
    """
    db_driver, collection_name = sample_collection

    # Ensure the collection exists by inserting a document
    sample_doc = {"name": "Charlie", "age": 28}
    write_success = await db_driver.write(data=[sample_doc], collection=collection_name)
    assert write_success is True, "Failed to write sample document to MongoDB collection."

    # Verify that the document exists
    documents, error = await db_driver.fetch(collection_name)
    assert error is None, f"Error during fetch: {error}"
    assert len(documents) == 1, "Sample document was not inserted correctly."

    # Drop the collection
    drop_success = await db_driver.drop_collection(collection_name)
    assert drop_success is True, "Failed to drop MongoDB collection."

    # Verify that the collection has been dropped
    try:
        documents, error = await db_driver.fetch(collection_name)
        if error:
            assert "NamespaceNotFound" in str(error), "Unexpected error when fetching dropped collection."
        else:
            assert len(documents) == 0, "Collection was not dropped successfully."
    except DriverError as e:
        # If the collection does not exist, ensure the correct error is raised
        assert "NamespaceNotFound" in str(e), "Unexpected error when fetching dropped collection."


@pytest.mark.asyncio
async def test_drop_database(dummy_db):
    """
    Test dropping a MongoDB database.
    """
    db_driver = dummy_db
    db_name = db_driver._database_name  # Accessing the database name from the driver

    # Ensure the database exists by creating a collection
    collection_name = "temp_collection"
    sample_doc = {"name": "Diana", "age": 22}
    write_success = await db_driver.write(data=[sample_doc], collection=collection_name)
    assert write_success is True, "Failed to write sample document to MongoDB collection."

    # Verify that the document exists
    documents, error = await db_driver.fetch(collection_name)
    assert error is None, f"Error during fetch: {error}"
    assert len(documents) == 1, "Sample document was not inserted correctly."

    # Drop the database
    drop_success = await db_driver.drop_database(db_name)
    assert drop_success is True, "Failed to drop MongoDB database."

    # Verify that the database has been dropped by listing databases
    try:
        databases = await db_driver._connection.list_database_names()
        assert db_name not in databases, "Database was not dropped successfully."
    except DriverError as e:
        # If the database does not exist, ensure the correct error is raised
        assert "DatabaseNotFound" in str(e), "Unexpected error when verifying dropped database."
