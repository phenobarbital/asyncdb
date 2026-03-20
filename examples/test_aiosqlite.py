import asyncio
from pprint import pprint

from asyncdb import AsyncDB
from asyncdb.exceptions import default_exception_handler
from asyncdb.models import Model, Column


class Airport(Model):
    iata: str = Column(primary_key=True)
    city: str
    country: str
    class Meta:
        name: str = 'airports'

async def test_model(db):
    async with await db.connection() as conn:
        table = """
            CREATE TABLE airports (
             iata text PRIMARY KEY,
             city text,
             country text
            )
        """
        result = await conn.execute(table)
        # test insertion
        Airport.Meta.set_connection(conn) # set the connection
        data = {
            "iata": 'MAD',
            "city": 'Medrid',
            "country": 'Spain'
        }
        airport = Airport(**data)
        # airport.set_connection(conn)
        result = await airport.insert()
        print('INSERT: ', result)
        # test Update:
        result.city = 'Madrid'
        result = await result.update()
        print('UPDATE: ', result)
        # test get (returns other row replacing current)
        madrid = await result.fetch(city='Madrid')
        print('MADRID IS: ', madrid)
        # test Delete:
        result = await madrid.delete()
        print('DELETE: ', result)
        # test Upsert (insert or update)
        data = [
            ("ORD", "Chicago", "United States"),
            ("JFK", "New York City", "United States"),
            ("CDG", "Paris", "France"),
            ("LHR", "London", "United Kingdom"),
            ("DME", "Moscow", "Russia"),
            ("SVO", "Moscow", "Russia"),
        ]
        for air in data:
            airport = Airport(*air)
            result = await airport.insert()
            print('INSERT: ', result)
        # test query (all)
        print(' === ITERATE OVER ALL === ')
        airports = await Airport.all()
        for airport in airports:
            print(airport)
        # test query (get one)
        print('= Test One: = ')
        paris = await Airport.get(city='Paris')
        print('Paris: ', paris)
        # test query (get many)
        print(' == Test Many ==')
        moscow = await Airport.filter(city='Moscow')
        for airport in moscow:
            print(airport)
        # test query SELECT
        print('== Test Query SELECT =')
        us = await Airport.select("WHERE country = 'United States'")
        for airport in us:
            print(airport)
        # at end: bulk insert, bulk update and bulk remove
        data = [
            {"iata": "AEP", "city": "Buenos Aires", "country": "Argentina"},
            {"iata": "ADL", "city": "Adelaide", "country": "Australia"},
            {"iata": "BSB", "city": "Brasilia", "country": "Brasil"},
            {"iata": "GRU", "city": "Sao Paulo", "country": "Brasil"},
            {"iata": "CCS", "city": "Caracas", "country": "Venezuela"},
        ]
        new_airports = await Airport.create(data)
        for airport in new_airports:
            print(airport)
        print('== Removing Many == ')
        deleted = await Airport.remove(country='Russia')
        print(deleted)
        # Updating many
        print('== Updating Many ==')
        updated_airports = await Airport.updating(_filter={"country": 'Brasil'}, country='Brazil')
        print(updated_airports)
        for airport in updated_airports:
            print(airport)
        updated = {
            "country": 'Venezuela'
        }
        updated_airports = await Airport.updating(updated, iata='BBM')
        for airport in updated_airports:
            print(airport)

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
        await conn.execute_many(many, examples)
        result, error = await conn.query("SELECT * FROM tests")
        if error:
            print(error)
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
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(default_exception_handler)
    driver = AsyncDB("sqlite", params={"database": ":memory:"}, loop=loop)
    asyncio.run(connect(driver))
    asyncio.run(test_model(driver))
