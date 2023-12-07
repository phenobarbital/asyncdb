import asyncio
from google.cloud import bigquery as gbq
from asyncdb.drivers.bigquery import bigquery


# create a pool with parameters
params = {
    "credentials": "~/proyectos/navigator/asyncdb/env/key.json",
    "project_id": "unique-decker-385015"
}

async def connect(loop):
    bq = bigquery(loop=loop, params=params)
    await bq.connection()
    print(
        f"Connected: {bq.is_connected()}"
    )
    query = """
        SELECT corpus AS title, COUNT(word) AS unique_words
        FROM `bigquery-public-data.samples.shakespeare`
        GROUP BY title
        ORDER BY unique_words
        DESC LIMIT 10
    """
    results, error = await bq.query(query)
    for row in results:
        title = row['title']
        unique_words = row['unique_words']
        print(f'{title:<20} | {unique_words}')

    ### Sample: create a dataset, a table and load a dataset
    dataset = await bq.create_dataset('us_states_dataset')
    print('Dataset created: ', dataset)
    schema = [
        gbq.SchemaField("name", "STRING", mode="REQUIRED"),
        gbq.SchemaField("post_abbr", "STRING", mode="REQUIRED"),
    ]
    table = await bq.create_table(
        dataset_id='us_states_dataset',
        table_id='us_states',
        schema=schema
    )
    print('Table created: ', table)
    truncated = await bq.truncate_table(
        dataset_id='us_states_dataset',
        table_id='us_states'
    )
    print('Table truncated: ', truncated)
    gcs_uri = 'gs://cloud-samples-data/bigquery/us-states/us-states.json'
    job_config = gbq.job.LoadJobConfig(
        autodetect=True,
        source_format=gbq.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job_config.schema = schema
    load_job = await bq.load_table_from_uri(
        source_uri=gcs_uri,
        table=table,
        # dataset_id='us_states_dataset',
        # table_id='us_states',
        job_config=job_config
    )
    print('Job completed: ', load_job)
    query = """
        SELECT name AS state, post_abbr as state_code
        FROM `unique-decker-385015.us_states_dataset.us_states`
    """
    job_config = bq.get_query_config(
        use_legacy_sql=False
    )
    results, error = await bq.query(query, job_config=job_config)
    if error:
        print(error)
    for row in results:
        print(row)
    df, error = await bq.fetch(query)
    print(df)
    df, error = await bq.fetch(query, use_pandas=True)
    print('Pandas BQ > ', df)
    query = """
        SELECT store_id, SUM(sale_qty) AS sales, SUM(return_qty) AS returns
        FROM `unique-decker-385015.epson.sales`
        WHERE store_id != ''
        GROUP BY store_id
        ORDER BY sales
        DESC LIMIT 100
    """
    async with await bq.connection() as conn:
        # execute a sentence
        df, error = await conn.fetch(query, use_pandas=False)
        print('TEST > ', df)
    await bq.close()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(connect(loop))
    finally:
        loop.stop()
