import asyncio
import pytest
from asyncdb import AsyncDB
import pytest_asyncio
import polars as pl
import datatable as dt
from asyncdb.meta import Record, Recordset

DRIVER = "sql_alchemy"
PARAMS = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}
DSN = "postgresql://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"

pytestmark = pytest.mark.asyncio

async def test_connect(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    with db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.row_format('iterable')  # returns a dictionary
        result, error = conn.test_connection()
        pytest.assume(not error)
        pytest.assume(result['one'] == 1)
        conn.row_format('native')  # return tuple
        result, error = conn.test_connection()
        pytest.assume(result[0] == 1)
        conn.row_format('record')  # return Record Object
        result, error = conn.test_connection()
        pytest.assume(result.one == 1)
    assert db.is_closed() is True
    
async def test_connect_by_dsn(event_loop):
    db = AsyncDB(DRIVER, dsn=DSN, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    with db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        conn.row_format('iterable')  # returns a dictionary
        result, error = conn.test_connection()
        pytest.assume(not error)
        pytest.assume(result['one'] == 1)
        conn.row_format('native')  # return tuple
        result, error = conn.test_connection()
        pytest.assume(result[0] == 1)
    assert db.is_closed() is True


async def test_operations(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    with db.connection() as conn:
        pytest.assume(conn.is_connected() is True)
        result = conn.create(
            name='tests',
            fields=[
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "varchar"}
            ]
        )
        pytest.assume(result)
        columns = conn.column_info(
            tablename='tests'
        )
        pytest.assume(len(columns) > 0)
        pytest.assume(columns[0]['name'] == 'id')
        many = "INSERT INTO tests VALUES(?, ?)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        conn.execute_many(many, examples)
        conn.row_format('iterable')  # change output format to dict
        result, error = conn.query("SELECT * FROM tests where id = 2")
        print('TEST> ', result, error)
        pytest.assume(not error)
        for row in result:
            pytest.assume(row["name"] == 'def')
        conn.execute('DROP TABLE tests')
        conn.execute('DROP TABLE airports')
        table = """
            CREATE TABLE airports (
            iata text PRIMARY KEY,
            city text,
            country text
            )
        """
        result, error = conn.execute(table)
        pytest.assume(not error)
        data = [
            ("ORD", "Chicago", "United States"),
            ("JFK", "New York City", "United States"),
            ("CDG", "Paris", "France"),
            ("LHR", "London", "United Kingdom"),
            ("DME", "Moscow", "Russia"),
            ("SVO", "Moscow", "Russia"),
        ]
        airports = "INSERT INTO airports VALUES(%s, %s, %s)"
        conn.executemany(airports, data)
        a_country = "France"
        a_city = "Paris"
        query = "SELECT * FROM airports WHERE country=%s AND city=%s"
        conn.row_format('iterable')  # change output format to dict
        with conn.cursor(query, (a_country, a_city)) as result:
            for row in result:
                pytest.assume(row['iata'] == "CDG")
    assert db.is_closed() is True


async def test_execute_many(event_loop):
    db = AsyncDB(DRIVER, params=PARAMS, loop=event_loop)
    pytest.assume(db.is_connected() is False)
    with db.connection() as conn:
        pytest.assume(db.is_connected() is True)
        result = conn.create(
            name='tests_cursors',
            fields=[
                {"name": "i", "type": "integer"},
                {"name": "k", "type": "integer"}
            ]
        )
        pytest.assume(result)
        context = "INSERT INTO tests_cursors VALUES(%s, %s)"
        conn.execute_many(context, [[i, i*2] for i in range(100)])
        conn.execute('DROP TABLE tests_cursors')


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
