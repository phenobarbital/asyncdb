import asyncio
import pytest
from pprint import pprint
from asyncdb import AsyncDB

pytestmark = pytest.mark.asyncio

async def test_connect(event_loop):
    db = AsyncDB("sqlite", params={"database": ":memory:"}, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(db.is_connected() is True)
        result, error = await db.test_connection()
        pytest.assume(not error)
        pytest.assume(result[0][0] == 1)
    assert db.is_closed() is True

async def test_operations(event_loop):
    db = AsyncDB("sqlite", params={"database": ":memory:"}, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    async with await db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result, error = await conn.execute("create table tests(id integer, name text)")
        pytest.assume(result)
        pytest.assume(not error)
        many = "INSERT INTO tests VALUES(?, ?)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        await conn.executemany(many, examples)
        result, error = await conn.query("SELECT * FROM tests where id = 2")
        print('TEST> ', result, error)
        pytest.assume(not error)
        for row in result:
            pytest.assume(result["name"] == 'def')
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
                pytest.assume(row[0] == "CDG")
    assert db.is_closed() is True

async def test_cursors(event_loop):
    db = AsyncDB("sqlite", params={"database": ":memory:"}, loop=event_loop)
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
        await conn.executemany(airports, data)
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
                values = await cursor.fetchall()
                print(values)
                pytest.assume(type(values) == list)
                pytest.assume(values = [('CDG', 'Paris', 'France'), ('LHR', 'London', 'United Kingdom')])
            # this returns a cursor based object
    assert db.is_closed() is True
