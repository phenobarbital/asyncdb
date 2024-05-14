import asyncio
from pathlib import Path
from asyncdb import AsyncDB
from asyncdb.drivers.bigquery import bigquery

default_dsn = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator"

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = BASE_DIR.joinpath('docs', 'calendar-table-data.csv')

async def saving_into_bigquery(pg_dsn, params):
    # pg = AsyncDB('pg', dsn=pg_dsn)
    # # First: getting data from postgres
    # async with await pg.connection() as conn:
    #     # Connectar a la DB
    #     conn.output_format('pandas')
    #     df, error = await conn.query(
    #         "SELECT * FROM epson.sales LIMIT 10000"
    #     )
    #     df.infer_objects()

    bq = AsyncDB('bigquery', params=params)
    async with await bq.connection() as conn:
        # Truncate Epson Sales Table:
        # truncated = await conn.truncate_table(
        #     dataset_id='epson',
        #     table_id='sales'
        # )
        # print('Table truncated: ', truncated)
        # Write data into table:
        # saved = await conn.write(
        #     dataset_id='epson',
        #     table_id='sales',
        #     data=df
        # )
        # print('Saved: ', saved)
        # # Test Data Saved:
        # conn.output_format('pandas')
        # query = """
        # SELECT store_id, SUM(sale_qty) AS sales, SUM(return_qty) AS returns
        # FROM `unique-decker-385015.epson.sales`
        # WHERE store_id != ''
        # GROUP BY store_id
        # ORDER BY sales
        # DESC LIMIT 100
        # """
        # result, error = await conn.query(
        #     query
        # )
        # print('PANDAS > ', result)
        # Third way: creating a GCS object from CSV:
        object_name, message = await conn.create_gcs_from_csv(
            bucket_name='sample-data-troc',
            object_name=CSV_FILE.name,
            csv_data=CSV_FILE,
            overwrite=False
        )
        if object_name:
            read_csv_from_gcs = await conn.read_csv_from_gcs(
                bucket_uri=object_name,
                # bucket_name='sample-data-troc',
                # object_name=object_name,
                table_id='calendar',
                dataset_id='epson'
            )
            print(read_csv_from_gcs)

async def duckdb_parquet():
    # Create a connection to DuckDB:
    dt = AsyncDB('duckdb')
    async with await dt.connection() as conn:
        # Create a parquet file:
        result = await conn.copy_to(CSV_FILE, BASE_DIR.joinpath('docs', 'test.parquet'))
        print('PARQUET FILE: ', result)

if __name__ == "__main__":
    # print('CONNECT TO BIGQUERY:')
    # params = {
    #     "credentials": "~/proyectos/navigator/navigator-ai/env/key.json",
    #     "project_id": "unique-decker-385015"
    # }
    # asyncio.run(
    #     saving_into_bigquery(default_dsn, params)
    # )
    asyncio.run(
        duckdb_parquet()
    )
