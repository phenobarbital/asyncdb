import asyncio
import pytest
from pprint import pprint
from asyncdb import AsyncDB
import pytest_asyncio
from typing import Generator
import polars as pl
import datatable as dt
from asyncdb.meta import Record, Recordset

DRIVER = "sqlite"
PARAMS = {"database": ":memory:"}


pytestmark = pytest.mark.asyncio


async def test_connect(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.row_format('iterable')  # returns a dictionary
        result, error = await conn.test_connection()
        pytest.assume(not error)
        pytest.assume(result[0]['1'] == 1)
        conn.row_format('native')  # return tuple
        result, error = await conn.test_connection()
        pytest.assume(result[0][0] == 1)
    assert db.is_closed() is True


async def test_operations(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result = await conn.create(
            name='tests',
            fields=[
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "varchar"}
            ]
        )
        pytest.assume(result)
        columns = await conn.column_info(
            tablename='tests'
        )
        pytest.assume(len(columns) > 0)
        pytest.assume(columns[0]['name'] == 'id')
        many = "INSERT INTO tests VALUES(?, ?)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        await conn.execute_many(many, examples)
        conn.row_format('iterable')  # change output format to dict
        result, error = await conn.query("SELECT * FROM tests where id = 2")
        print('TEST> ', result, error)
        pytest.assume(not error)
        for row in result:
            pytest.assume(row["name"] == 'def')
        table = """
            CREATE TABLE airports (
            iata text PRIMARY KEY,
            city text,
            country text
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
        a_country = "France"
        a_city = "Paris"
        query = "SELECT * FROM airports WHERE country=? AND city=?"
        async with await conn.fetch(query, (a_country, a_city)) as result:
            async for row in result:
                print(row)
                pytest.assume(row['iata'] == "CDG")
    assert db.is_closed() is True


async def test_cursors(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(db.is_connected() is True)
        # using prepare
        table = """
            CREATE TABLE airports (
            iata text PRIMARY KEY,
            city text,
            country text
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
        await conn.execute_many(airports, data)
        print('Using Cursor Objects: ')
        b_country = 'France'
        b_city = 'London'
        query = "SELECT * FROM airports WHERE country=? OR city=?"
        async with conn.cursor(query, (b_country, b_city)) as cursor:
            pytest.assume(cursor)
            print("using iterator: ")
            async for row in cursor:
                pytest.assume(type(row) == tuple)
                pytest.assume(row[0] in ["LHR", "CDG"])
            # its an iterable
            print("Using Context Manager: ")
            async with cursor:
                values = await cursor.fetch_all()
                print(values)
                pytest.assume(type(values) == list)
                pytest.assume(
                    values=[('CDG', 'Paris', 'France'), ('LHR', 'London', 'United Kingdom')])
            # this returns a cursor based object
    assert db.is_closed() is True


async def test_execute_many(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(db.is_connected() is True)
        result = await conn.create(
            name='tests_cursors',
            fields=[
                {"name": "i", "type": "integer"},
                {"name": "k", "type": "integer"}
            ]
        )
        pytest.assume(result)
        context = "INSERT INTO tests_cursors VALUES(?, ?)"
        await conn.execute_many(context, [[i, i*2] for i in range(100)])
        async with conn.cursor("SELECT * FROM tests_cursors") as cursor:
            pytest.assume(cursor)
            async for row in cursor:
                print(row)
                pytest.assume(type(row) == tuple)


async def test_formats(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    async with await db.connection() as conn:
        pytest.assume(db.is_connected() is True)
        # using prepare
        table = """
            CREATE TABLE airports (
            iata text PRIMARY KEY,
            city text,
            country text
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
        await conn.execute_many(airports, data)
        # first-format, native:
        conn.row_format('iterable')  # change output format to dict
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == list)
        conn.output_format('json')  # change output format to json
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == str)
        conn.output_format('pandas')  # change output format to pandas
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        import pandas
        pytest.assume(type(result) == pandas.core.frame.DataFrame)
        # change output format to iter generator
        conn.output_format('iterable')
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        # pytest.assume(callable(result)) # TODO: test method for generator exp
        conn.output_format('polars')  # change output format to iter generator
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == pl.frame.DataFrame)
        # change output format to iter generator
        conn.output_format('datatable')
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        print(type(result))
        pytest.assume(type(result) == dt.Frame)
        conn.output_format('csv')  # change output format to iter generator
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == str)
        # testing Record Object
        conn.output_format('record')  # change output format to iter generator
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == list)
        for row in result:
            pytest.assume(type(row) == Record)
            pytest.assume(len(row.iata) == 3)
        # testing Recordset Object
        conn.output_format('recordset')  # change output format to ResultSet
        result, error = await conn.query("SELECT * FROM airports")
        print(result)
        pytest.assume(type(result) == Recordset)
        # working with slices:
        obj = result[0:2]
        pytest.assume(len(obj) == 2)
        for row in result:
            pytest.assume(type(row) == Record)
            pytest.assume(len(row.iata) == 3)
            print(row)
    assert db.is_closed() is True


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
