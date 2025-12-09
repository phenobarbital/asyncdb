import asyncio
import pytest
import polars as pl
from asyncdb import AsyncDB
from asyncdb.meta.record import Record
from asyncdb.meta.recordset import Recordset

DRIVER = "duckdb"
PARAMS = {"database": ":memory:"}

pytestmark = pytest.mark.asyncio


async def test_connect(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    try:
        pytest.assume(db.is_connected() is False)
        async with await db.connection() as conn:
            pytest.assume(conn.is_connected() is True)
            # DuckDB simple test query
            conn.row_format('native')
            result, error = await conn.query("SELECT 1 as one")
            pytest.assume(not error)
            pytest.assume(result[0][0] == 1)
            conn.row_format('native')  # return tuple
            result, error = await conn.query("SELECT 1")
            pytest.assume(result[0][0] == 1)
    finally:
        await db.close()


async def test_operations(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    try:
        async with await db.connection() as conn:
            # Create table
            result = await conn.execute(
                "CREATE TABLE tests (id INTEGER, name VARCHAR)"
            )
            pytest.assume(not result[1])

            # Insert
            many = "INSERT INTO tests VALUES(?, ?)"
            examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
            await conn.execute_many(many, examples)

            # Query
            conn.row_format('native')
            result, error = await conn.query("SELECT * FROM tests where id = 2")
            pytest.assume(not error)
            for row in result:
                # tuple access: id is 0, name is 1
                pytest.assume(row[1] == 'def')

            # Create another table
            table = """
                CREATE TABLE airports (
                iata VARCHAR PRIMARY KEY,
                city VARCHAR,
                country VARCHAR
                )
            """
            result, error = await conn.execute(table)
            pytest.assume(not error)
            data = [
                ("ORD", "Chicago", "United States"),
                ("JFK", "New York City", "United States"),
                ("CDG", "Paris", "France"),
                ("LHR", "London", "United Kingdom"),
                ("DME", "Moscow", "Russia"),
                ("SVO", "Moscow", "Russia"),
            ]
            airports = "INSERT INTO airports VALUES(?, ?, ?)"
            await conn.executemany(airports, data)
            
            # Fetch
            a_country = "France"
            a_city = "Paris"
            query = "SELECT * FROM airports WHERE country=? AND city=?"
            conn.row_format('native')
            result, error = await conn.query(query, (a_country, a_city))
            pytest.assume(not error)
            # result[0] is tuple ('CDG', 'Paris', 'France')
            pytest.assume(result[0][0] == 'CDG')

    finally:
        await db.close()

async def test_formats(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    try:
        async with await db.connection() as conn:
            # Setup data
            await conn.execute("CREATE TABLE products (id INTEGER, name VARCHAR)")
            await conn.execute("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')")

            # Default (Iterable/Dict)
            conn.row_format('native')
            result, error = await conn.query("SELECT * FROM products ORDER BY id")
            pytest.assume(type(result) == list)
            pytest.assume(result[0][1] == 'Widget')

            # Polars
            conn.output_format('polars')
            result, error = await conn.query("SELECT * FROM products")
            # pytest.assume(not error)
            # pytest.assume(type(result) == pl.DataFrame)
            # pytest.assume(result['name'][0] == 'Widget')

            # Pandas
            # conn.output_format('pandas')
            # result, error = await conn.query("SELECT * FROM products")
            # pytest.assume(not error)
            # pytest.assume(type(result) == pd.DataFrame)
            # pytest.assume(result['name'][0] == 'Widget')

            # Pandas
            # conn.output_format('pandas')
            # result, error = await conn.query("SELECT * FROM products")
            # import pandas as pd
            # pytest.assume(type(result) == pd.DataFrame)
            
    finally:
        await db.close()

def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
