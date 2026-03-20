import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler

params = {
    "host": "localhost",
    "port": "1433",
    "database": 'AdventureWorks2019',
    "user": 'SA',
    "password": 'P4ssW0rd1.'
}

DRIVER = 'sqlserver'


async def connect(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        pprint(conn.is_connected())
        await conn.use('AdventureWorks2019')
        result = await conn.execute("create table tests(id INT NOT NULL, name VARCHAR(100))")
        print('Execute Result: ', result)
        many = "INSERT INTO dbo.tests VALUES(%d, %s)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        result, error = await conn.executemany(many, examples)
        print(result, error)
        result, error = await conn.query("SELECT * FROM dbo.tests")
        for row in result:
            print(row)
        print('=== getting parametrized results: ===')
        result, error = await conn.queryrow("SELECT * FROM dbo.tests WHERE name=%s", 'jkl', as_dict=True)
        print(result, 'Error: ', error)
        table = """
            DROP TABLE IF EXISTS dbo.airports;
            CREATE TABLE dbo.airports (
             iata varchar(3),
             city varchar(250),
             country varchar(250)
            );
        """
        print(": Creating Table Airport: ")
        result, error = await conn.execute(table)
        data = [
            ("ORD", "Chicago", "United States"),
            ("JFK", "New York City", "United States"),
            ("CDG", "Paris", "France"),
            ("LHR", "London", "United Kingdom"),
            ("DME", "Moscow", "Russia"),
            ("SVO", "Moscow", "Russia"),
        ]
        airports = "INSERT INTO dbo.airports VALUES(%s, %s, %s)"
        await conn.executemany(airports, data)
        query = "SELECT * FROM dbo.airports WHERE country=%s OR city=%s"
        # using Cursor Objects
        print('Using Cursor Objects: ')
        b_country = 'France'
        b_city = 'London'
        async with conn.cursor(query, (b_country, b_city)) as cursor:
            print("using iterator: ", cursor)
            async for row in cursor:
                print(row)
            # its an iterable
            print("Using Context Manager: ")
            async with cursor:
                print(await cursor.fetchall())
            # this returns a cursor based object


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(default_exception_handler)
        driver = AsyncDB(DRIVER, params=params, loop=loop)
        print(driver)
        loop.run_until_complete(connect(driver))
    finally:
        loop.stop()
