import pytest
import asyncio
from asyncdb import AsyncDB
from google.cloud import bigquery as gbq
from google.cloud.bigquery.table import RowIterator
from asyncdb.drivers.bigquery import bigquery

# create a pool with parameters
DRIVER='bigquery'
PARAMS = {
    "credentials": "~/proyectos/navigator/asyncdb/env/key.json",
    "project_id": "unique-decker-385015"
}

@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=PARAMS, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result, error = await conn.test_connection()
        pytest.assume(isinstance(result, RowIterator))
        pytest.assume(not error)
    pytest.assume(db.is_connected() is False)

@pytest.mark.asyncio
async def test_bigquery_operations(event_loop):
    bq = bigquery(loop=event_loop, params=PARAMS)
    async with await bq.connection() as conn:
        assert conn is not None, "Connection failed"
        assert bq.is_connected(), "Connection failed"
        # Test query
        query = """
            SELECT corpus AS title, COUNT(word) AS unique_words
            FROM `bigquery-public-data.samples.shakespeare`
            GROUP BY title
            ORDER BY unique_words
            DESC LIMIT 10
        """
        results, error = await bq.query(query)
        assert error is None, f"Query failed with error: {error}"
        assert results is not None, "Query returned no results"

        # Test dataset creation
        dataset = await bq.create_dataset('us_states_dataset')
        assert dataset is not None, "Dataset creation failed"

        # Test table creation
        schema = [
            gbq.SchemaField("name", "STRING", mode="REQUIRED"),
            gbq.SchemaField("post_abbr", "STRING", mode="REQUIRED"),
        ]
        table = await bq.create_table(
            dataset_id='us_states_dataset',
            table_id='us_states',
            schema=schema
        )
        assert table is not None, "Table creation failed"

        # Test table truncation
        truncated = await bq.truncate_table(
            dataset_id='us_states_dataset',
            table_id='us_states'
        )

        # Test data loading
        gcs_uri = 'gs://cloud-samples-data/bigquery/us-states/us-states.json'
        job_config = gbq.job.LoadJobConfig(
            autodetect=True,
            source_format=gbq.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job_config.schema = schema
        load_job = await bq.load_table_from_uri(
            source_uri=gcs_uri,
            table=table,
            job_config=job_config
        )
        assert load_job, "Data loading failed"

        # Test fetching data
        query = """
            SELECT name AS state, post_abbr as state_code
            FROM `unique-decker-385015.us_states_dataset.us_states`
        """
        job_config = bq.get_query_config(
            use_legacy_sql=False
        )
        results, error = await bq.query(query, job_config=job_config)
        assert error is None, f"Query failed with error: {error}"
        assert results is not None, "Query returned no results"

    # Test closing connection
    assert not bq.is_connected(), "Closing connection failed"

def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
