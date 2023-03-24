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

if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(
        test_connection(evt=loop)
    )
    loop.run_until_complete(
        test_data(evt=loop)
    )
    end_time = time.time()  # Record the end time
    duration_seconds = end_time - start_time
    print(f"Duration: {duration_seconds} seconds")
