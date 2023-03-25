import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler


async def connect(db):
    async with await db.connection() as conn:
        pprint(await conn.test_connection())
        await conn.create(
            name='tests',
            fields=[
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "text"}
            ]
        )
        many = "INSERT INTO tests VALUES(?, ?)"
        examples = [(2, "def"), (3, "ghi"), (4, "jkl")]
        print(": Executing Insert of many entries: ")
        await conn.execute_many(many, *examples)
        result, error = await conn.query("SELECT * FROM tests")
        if error:
            print(error)
        for row in result:
            print('>>', row)
        table = """
            CREATE TABLE airports (
             iata VARCHAR PRIMARY KEY,
             city VARCHAR,
             country VARCHAR
            )
        """
        result = await conn.execute(table)
        print('CREATED ?: ', result)
        data = [
            ("ORD", "Chicago", "United States"),
            ("JFK", "New York City", "United States"),
            ("CDG", "Paris", "France"),
            ("LHR", "London", "United Kingdom"),
            ("DME", "Moscow", "Russia"),
            ("SVO", "Moscow", "Russia"),
        ]
        airports = "INSERT INTO airports VALUES(?, ?, ?)"
        await conn.executemany(airports, *data)
        # a_country = "United States"
        # a_city = "Moscow"
        query = "SELECT * FROM airports WHERE country=? OR city=?"
        # async with await conn.fetch(query, (a_country, a_city)) as result:
        #     for row in result:
        #         print(row)
        # using prepare
        print('Using Cursor Objects: ')
        b_country = 'France'
        b_city = 'London'
        async with conn.cursor(query, (b_country, b_city)) as cursor:
            async for row in cursor:
                print(row)
            # its an iterable
            print("Also: Using Context Manager: ")
            async with cursor:
                print(await cursor.fetch_all())
            # this returns a cursor based object

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(default_exception_handler)
    driver = AsyncDB("duckdb", params={"database": ":memory:"}, loop=loop)
    loop.run_until_complete(connect(driver))
