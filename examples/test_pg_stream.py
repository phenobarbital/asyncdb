import asyncio
import pandas as pd
import pyarrow as pa
from asyncdb.drivers.pg import pg
from asyncdb.drivers.outputs.pandas import pandas_parser
from asyncdb.drivers.outputs.arrow import arrow_parser
from asyncdb.drivers.outputs.dt import dt_parser, dt
from asyncdb.drivers.outputs.polars import polars_parser, pl


params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator",
    "DEBUG": True,
}

async def connect():
    db = pg(params=params)
    # create a connection
    async with await db.connection() as conn:
        print('Connection: ', conn)
        result, error = await conn.test_connection()
        print(result, error)

        # create a Pandas dataframe from streaming
        parts = []
        async for chunk in conn.stream_query(
            "SELECT * FROM epson.sales LIMIT 100000",
            pandas_parser,
            chunksize=1000
        ):
            print("Received chunk of size:", len(chunk))
            parts.append(chunk)

        df = pd.concat(parts, ignore_index=True)
        print(df)

        # create a Arrow Table from streaming
        parts = []
        async for chunk in conn.stream_query(
            "SELECT * FROM epson.sales LIMIT 1000000",
            arrow_parser,
            chunksize=10000
        ):
            # print("Received chunk of size:", len(chunk))
            parts.append(chunk)

        df = pa.concat_tables(parts)
        print("Final table has", df.num_rows, "rows in total.")

        # create a Polars dataframe from streaming
        parts = []
        async for chunk in conn.stream_query(
            "SELECT * FROM epson.sales LIMIT 1000000",
            polars_parser,
            chunksize=10000
        ):
            # print("Received chunk of size:", len(chunk))
            parts.append(chunk)

        df = pl.concat(parts, how="vertical")
        print("Final table has", df.shape, "rows in total.")

        # create a Datatable from streaming
        parts = []
        async for chunk in conn.stream_query(
            "SELECT * FROM epson.sales LIMIT 1000000",
            dt_parser,
            chunksize=10000
        ):
            # print("Received chunk of size:", len(chunk))
            parts.append(chunk)

        df = dt.rbind(parts)  # or dt.rbind(frames, force=True)
        print("Final table has", df.nrows, "rows in total.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(connect())
