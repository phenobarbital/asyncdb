import asyncio
from pathlib import Path
import time
import types
import gc
import pandas as pd
from asyncdb.drivers.delta import delta
from asyncdb import AsyncDB

async def test_connection(evt: asyncio.AbstractEventLoop):
    params = {
        "path": "docs/nyc.taxi/"
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
        "path": "docs/nyc.taxi/"
    }
    dt = AsyncDB('delta', params=params, loop=evt)
    async with await dt.connection() as conn:
        # querying data:
        result, error = await conn.to_df(
            columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'],
            factory='pandas'
        )
        print(result, error)
        # Querying with DuckDB:
        result, error = await conn.query(
            sentence="passenger_count > 5",
            factory='pandas'
        )
        print(result, error)
        # Querying with DuckDB using SQL Syntax:
        result, error = await conn.query(
            sentence="SELECT * FROM NYC_TAXI WHERE passenger_count > 5 LIMIT 100",
            tablename='NYC_TAXI',
            factory='pandas'
        )
        print(result, error)

async def test_create_epson(evt: asyncio.AbstractEventLoop):
    # Convert a file to parquet:
    args = {
        "has_header": False,
        "replace_destination": True,
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
    params = {
        "path": "docs/epson.sales/"
    }
    dt = delta(params=params, loop=evt)
    source = Path('/home/ubuntu/symbits/epson/files/sales/SELLTHRU_TROC_2020.TXT')
    parquet_dir = Path(__file__).parent.parent.joinpath('docs', "epson.sales")
    destination = parquet_dir.joinpath("20200630_091802.parquet")
    parquet, meta = await dt.copy_to(
        source=source,
        destination=destination,
        separator='|',
        lazy=True,
        **args
    )
    print('RESULT > ', parquet, meta)
    await dt.create(parquet_dir, parquet, name='EPSON_SALES', mode='overwrite')
    async with await dt.connection() as conn:
        # querying data:
        result, error = await conn.query(
            sentence="customer_id == '1006534'",
            factory='pandas'
        )
        print('ERROR > ',result, error)
        group = result.groupby(['customer_id']).count()
        print(group)
        print(result)

async def test_epson(evt: asyncio.AbstractEventLoop):
    tablename = 'EPSON_SALES'
    params = {
        "path": Path(__file__).parent.parent.joinpath('docs', "epson.sales")
    }
    dt = delta(params=params, loop=evt)
    query_string = "SELECT * FROM EPSON_SALES WHERE customer_id = 1006534 LIMIT 100"
    async with await dt.connection() as conn:
        # querying data:
        result, error = await conn.query(
            sentence=query_string,
            tablename=tablename,
            factory='pandas'
        )
        print('Query > ', result, error)
        group = result.groupby(['store_number']).count()
        print('GROUP > ', group)
        result, error = await conn.queryrow(
            sentence=query_string,
            tablename=tablename,
            factory='pandas'
        )
        if error:
            print(f"Error executing queryrow: {error}")
        else:
            print(result)
    # path = Path(__file__).parent.parent.joinpath('docs', "epson.sales")
    # async with await dt.connection(path=path) as conn:
    #     result, error = await conn.queryrow(
    #         sentence=query_string,
    #         tablename=tablename,
    #         factory='pandas'
    #     )
    #     if error:
    #         print(f"Error executing queryrow: {error}")
    #     else:
    #         print(result)

async def test_write_new_api(tmp_dir: Path):
    """Demonstrates the new write() API with full delta-rs parameter support."""
    # Create a lightweight driver stub (bypasses MRO init chain)
    stub = object.__new__(delta)
    stub.storage_options = {"timeout": "120s"}
    stub._delta = None
    stub.write = types.MethodType(delta.write, stub)
    stub.create = types.MethodType(delta.create, stub)

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "region": ["us", "eu", "us"],
        "value": [10, 20, 30],
    })

    # 1. Basic write with mode="overwrite"
    await stub.write(df, "sales", tmp_dir, mode="overwrite")
    print("[1] Overwrite write complete")

    # 2. Write with schema_mode="merge" — add new columns without error
    df_extra = pd.DataFrame({
        "id": [4],
        "region": ["eu"],
        "value": [40],
        "discount": [0.1],  # new column
    })
    await stub.write(
        df_extra, "sales", tmp_dir,
        mode="append",
        schema_mode="merge",  # allows the new 'discount' column
    )
    print("[2] Schema merge write complete")

    # 3. Write with configuration metadata
    await stub.write(
        df, "configured_table", tmp_dir,
        mode="append",
        configuration={"delta.appendOnly": "true"},
    )
    print("[3] Configuration write complete")

    # 4. Targeted overwrite with predicate
    df_base = pd.DataFrame({"id": [1, 2, 3], "region": ["us", "eu", "us"], "value": [10, 20, 30]})
    await stub.write(df_base, "predicate_table", tmp_dir, mode="append")
    df_update = pd.DataFrame({"id": [99], "region": ["us"], "value": [999]})
    await stub.write(
        df_update, "predicate_table", tmp_dir,
        mode="overwrite",
        predicate="region = 'us'",  # only overwrite US rows
    )
    print("[4] Predicate overwrite complete")

    # 5. Write with name and description metadata
    await stub.write(
        df, "documented_table", tmp_dir,
        name="sales_summary",
        description="Monthly sales aggregation",
    )
    print("[5] Named table write complete")

    # 6. Backward-compatible if_exists (deprecated, use mode instead)
    await stub.write(
        df, "compat_table", tmp_dir,
        if_exists="overwrite",  # Deprecated: emits DeprecationWarning
    )
    print("[6] Backward-compatible if_exists write complete")

    # 7. Write with partition_by
    await stub.write(
        df, "partitioned_sales", tmp_dir,
        partition_by=["region"],
    )
    print("[7] Partitioned write complete")


if __name__ == '__main__':
    try:
        start_time = time.time()  # Record the start time
        loop = asyncio.get_event_loop()
        # loop.set_debug(True)
        # loop.run_until_complete(
        #     test_connection(evt=loop)
        # )
        # loop.run_until_complete(
        #     test_data(evt=loop)
        # )
        loop.run_until_complete(
            test_create_epson(evt=loop)
        )
        loop.run_until_complete(
            test_epson(evt=loop)
        )
        end_time = time.time()  # Record the end time
        duration_seconds = end_time - start_time
        print(
            f"Duration: {duration_seconds} seconds"
        )
    finally:
        loop.stop()
