import asyncio
import time

import pandas as pd
import pyarrow as pa
import polars as pl
import datatable as dt

from asyncdb.drivers.pg import pg
from asyncdb.drivers.outputs.pandas import pandas_parser
from asyncdb.drivers.outputs.arrow import arrow_parser
from asyncdb.drivers.outputs.polars import polars_parser
from asyncdb.drivers.outputs.dt import dt_parser

# Connection parameters (adjust as needed)
params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator",
    "DEBUG": True,
}

# The query to test. Adjust row LIMIT as needed for your benchmarks.
QUERY = "SELECT * FROM epson.sales LIMIT 1000000"  # smaller limit for Pandas example
CHUNKSIZE = 10000

async def test_pandas(conn):
    """
    Stream data into Pandas in chunks and measure time.
    """
    start_time = time.perf_counter()

    parts = []
    async for chunk_df in conn.stream_query(
        QUERY,
        parser=pandas_parser,   # your custom parser
        chunksize=CHUNKSIZE
    ):
        parts.append(chunk_df)

    df = pd.concat(parts, ignore_index=True)
    total_rows = len(df)

    elapsed = time.perf_counter() - start_time
    print(f"[Pandas] Loaded {total_rows} rows in {elapsed:.2f} seconds.")

async def test_arrow(conn):
    """
    Stream data into Arrow Tables in chunks and measure time.
    """
    start_time = time.perf_counter()

    tables = []
    async for tbl in conn.stream_query(
        QUERY,
        parser=arrow_parser,   # your custom parser
        chunksize=CHUNKSIZE
    ):
        tables.append(tbl)

    # Concatenate all tables into one
    final_table = pa.concat_tables(tables)
    total_rows = final_table.num_rows

    elapsed = time.perf_counter() - start_time
    print(f"[Arrow] Loaded {total_rows} rows in {elapsed:.2f} seconds.")

    # then time the conversion:
    start_conv = time.perf_counter()
    df = final_table.to_pandas()
    conv_elapsed = time.perf_counter() - start_conv
    print(f"[Conversion] Arrow Table -> Pandas took {conv_elapsed:.2f} seconds.")

async def test_polars(conn):
    """
    Stream data into Polars in chunks and measure time.
    """
    start_time = time.perf_counter()

    frames = []
    async for pl_df in conn.stream_query(
        QUERY,
        parser=polars_parser,
        chunksize=CHUNKSIZE
    ):
        frames.append(pl_df)

    final_df = pl.concat(frames, how="vertical")
    total_rows = final_df.shape[0]

    elapsed = time.perf_counter() - start_time
    print(f"[Polars] Loaded {total_rows} rows in {elapsed:.2f} seconds.")

async def test_datatable(conn):
    """
    Stream data into Python datatable in chunks and measure time.
    """
    start_time = time.perf_counter()

    dt_frames = []
    async for frame in conn.stream_query(
        QUERY,
        parser=dt_parser,
        chunksize=CHUNKSIZE
    ):
        dt_frames.append(frame)

    final_frame = dt.rbind(dt_frames)  # or dt.rbind(dt_frames, force=True)
    total_rows = final_frame.nrows

    elapsed = time.perf_counter() - start_time
    print(f"[datatable] Loaded {total_rows} rows in {elapsed:.2f} seconds.")

    # 2. Convert datatable.Frame => Pandas DataFrame
    start_conv = time.perf_counter()
    df = final_frame.to_pandas()
    conv_elapsed = time.perf_counter() - start_conv

    print(f"[Conversion] datatable.Frame -> Pandas took {conv_elapsed:.2f} seconds.")
    print(f"Final Pandas DataFrame has {len(df)} rows.")

async def main():
    # Create a pg driver instance and connect
    db = pg(params=params)
    async with await db.connection() as conn:
        print("Connection:", conn)
        result, error = await conn.test_connection()
        print("Test connection:", result, error)

        # Run each parser test in sequence
        #await test_pandas(conn)
        # await test_arrow(conn)
        await test_polars(conn)
        # await test_datatable(conn)


if __name__ == "__main__":
    asyncio.run(main())
