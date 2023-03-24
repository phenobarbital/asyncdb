import asyncio
from pathlib import Path
import time
from asyncdb.drivers.delta import delta
from asyncdb import AsyncDB

async def test_connection(evt: asyncio.AbstractEventLoop):
    params = {
        "filename": "docs/nyc.taxi/"
    }
    dt = delta(params=params, loop=evt)
    ## first: create delta-table
    path = Path(__file__).parent.parent.joinpath('docs', "nyc.taxi")
    data = Path(__file__).parent.parent.joinpath('docs', 'yellow_tripdata_2022-01.parquet')
    print('PATH > ', path)
    await dt.create(path, data, name='NYC_TAXI', mode='overwrite')
    async with await dt.connection() as conn:
        print('SCHEMA: ', conn.schema())

async def test_data(evt: asyncio.AbstractEventLoop):
    params = {
        "filename": "docs/nyc.taxi/"
    }
    dt = AsyncDB('delta', params=params, loop=evt)
    async with await dt.connection() as conn:
        # querying data:
        result, error = await conn.query(
            columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'],
            factory='pandas'
        )
        print(result, error)

async def test_epson(evt: asyncio.AbstractEventLoop):
    args = {
        "pd_args": {
            "sep": "|",
            "header": False,
            "columns": [
                "week_end_date",
                "customer_id",
                "customer_description",
                "store_number",
                "product_type",
                "model",
                "model_description",
                "sale_class",
                "sale_subclass",
                "material",
                "material_description",
                "sale_qty",
                "return_qty",
                "sell_thru_qty",
                "sell_thru_rev",
                "on_hand_qty"
            ],
        }
    }
    params = {
        "filename": "docs/epson.sales/"
    }
    dt = delta(params=params, loop=evt)
    filename = '/home/ubuntu/symbits/epson/files/sales/SELLTHRU_TROC_20200630_091802.TXT'
    parquet_dir = Path(__file__).parent.parent.joinpath('docs', "epson.sales")
    parquet = parquet_dir.joinpath("20200630_091802.parquet")
    await dt.file_to_parquet(filename, parquet, factory='datatable', **args)
    await dt.create(parquet_dir, parquet, name='EPSON_SALES', mode='overwrite')
    async with await dt.connection() as conn:
        # querying data:
        result, error = await conn.query(
            columns=['customer_id', 'week_end_date', 'store_number', 'product_type', 'model'],
            factory='pandas'
        )
        print('ERROR > ', error)
        group = result.groupby(['customer_id']).count()
        print(group)
        print(result)

if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    # loop.run_until_complete(
    #     test_connection(evt=loop)
    # )
    # loop.run_until_complete(
    #     test_data(evt=loop)
    # )
    loop.run_until_complete(
        test_epson(evt=loop)
    )
    end_time = time.time()  # Record the end time
    duration_seconds = end_time - start_time
    print(f"Duration: {duration_seconds} seconds")
