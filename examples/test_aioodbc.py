import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler

## Connection Pooling

async def connect(db):
    async with await db.connection() as conn:
        print('Getting Driver: ', conn)
        pprint(await conn.test_connection())
        await conn.execute("create table tests(id integer, name text)")
        many = "INSERT INTO tests VALUES(?, ?)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        await conn.executemany(many, examples)
        result, _ = await conn.query("SELECT * FROM tests")
        for row in result:
            print(row)
        table = """
            CREATE TABLE airports (
            iata text PRIMARY KEY,
            city text,
            country text
            )
        """
        await conn.execute(table)
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
        a_country = "United States"
        a_city = "Moscow"
        query = "SELECT * FROM airports WHERE country=? OR city=?"
        async with await conn.fetch(query, (a_country, a_city)) as result:
            async for row in result:
                print(row)
        # using Cursor Objects
        print('Using Cursor Objects: ')
        b_country = 'France'
        b_city = 'London'
        async with conn.cursor(query, (b_country, b_city)) as cursor:
            print("using iterator: ")
            async for row in cursor:
                print(row)
            # its an iterable
            print("Using Context Manager: ")
            async with cursor:
                print(await cursor.fetchall())
            # this returns a cursor based object


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(default_exception_handler)
    driver = AsyncDB("odbc", params={"driver":"SQLite3", "database": ":memory:"}, loop=loop)
    asyncio.run(connect(driver))
