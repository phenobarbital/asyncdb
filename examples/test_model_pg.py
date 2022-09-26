import asyncio
from pprint import pprint
from asyncdb import AsyncDB
from asyncdb.models import Model, Column


class Airport(Model):
    iata: str = Column(primary_key=True)
    airport: str
    city: str
    country: str
    class Meta:
        name: str = 'airports'


async def start_example(db):
    """
    Create the Table:
    """
    async with await db.connection() as conn:
        table = """
        CREATE TABLE IF NOT EXISTS public.airports
        (
         iata character varying(3),
         airport character varying(60),
         city character varying(20),
         country character varying(30),
         CONSTRAINT pk_airports_pkey PRIMARY KEY (iata)
        )
        WITH (
        OIDS=FALSE
        );
        """
        result = await conn.execute(table)
        print(result)


async def end_example(db):
    """
    DROP the Table:
    """
    async with await db.connection() as conn:
        drop = "DROP TABLE IF EXISTS public.airports;"
        result = await conn.execute(drop)
        print(result)


async def test_model(db):
    async with await db.connection() as conn:
        # test insertion
        Airport.Meta.set_connection(conn) # set the connection
        data = {
            "iata": 'MAD',
            "airport": 'Adolfo Suarez',
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
            ("ORD", "O'Hare International", "Chicago", "United States"),
            ("JFK", "John F. Kennedy", "New York City", "United States"),
            ("CDG", "Charles De Gaulle", "Paris", "France"),
            ("LHR", "London Heathrow", "London", "United Kingdom"),
            ("DME", "Domodedovo", "Moscow", "Russia"),
            ("SVO", "Moscow Sheremétievo", "Moscow", "Russia"),
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
        print(' == TEST Bulk Insert == ')
        data = [
            {"iata": "AEP", "airport": "Jorge Newbery", "city": "Buenos Aires", "country": "Argentina"},
            {"iata": "ADL", "airport": "Adelaide International", "city": "Adelaide", "country": "Australia"},
            {"iata": "BSB", "airport": "President Juscelino Kubitschek", "city": "Brasilia", "country": "Brasil"},
            {"iata": "GRU", "airport": "São Paulo-Guarulhos", "city": "Sao Paulo", "country": "Brasil"},
            {"iata": "CCS", "airport": "Simon Bolívar Maiquetia", "city": "Caracas", "country": "Venezuela"},
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
        updated_airports = await Airport.updating(updated, iata='BBM', airport='Jacinto Lara', city='Barquisimeto')
        for airport in updated_airports:
            print(airport)


if __name__ == "__main__":
    params = {
        "user": "troc_pgdata",
        "password": "12345678",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "navigator_dev",
        "DEBUG": True,
    }
    try:
        loop = asyncio.get_event_loop()
        driver = AsyncDB("pg", params=params, loop=loop)
        loop.run_until_complete(
            start_example(driver)
        )
        print('====')
        loop.run_until_complete(
            test_model(driver)
        )
    finally:
        print('== DELETE TABLE ==')
        loop.run_until_complete(
            end_example(driver)
        )
        loop.stop()
