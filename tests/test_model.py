import asyncio
import pytest
from asyncdb import AsyncDB
from asyncdb.models import Model, Column
from asyncdb.exceptions import NoDataFound

class Airport(Model):
    iata: str = Column(primary_key=True)
    airport: str
    city: str
    country: str
    class Meta:
        name: str = 'airports'


@pytest.fixture(scope="function")
async def db():
    params = {
        "user": "troc_pgdata",
        "password": "12345678",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "navigator_dev",
        "DEBUG": True,
    }
    driver = AsyncDB("pg", params=params)
    await start_example(driver)
    yield driver
    await end_example(driver)

async def start_example(db):
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
    async with await db.connection() as conn:
        drop = "DROP TABLE IF EXISTS public.airports;"
        result = await conn.execute(drop)
        print(result)


@pytest.mark.asyncio
@pytest.mark.usefixtures("db")
async def test_airport_model(db):
    async with await db.connection() as conn:
        # test insertion
        Airport.Meta.set_connection(conn) # set the connection
        pytest.assume(db.is_connected() is True)
        pytest.assume(Airport.Meta.connection is not None)
        data = {
            "iata": 'MAD',
            "airport": 'Adolfo Suarez',
            "city": 'Medrid',
            "country": 'Spain'
        }
        airport = Airport(**data)
        # airport.set_connection(conn)
        result = await airport.insert()
        assert result.to_dict() == data
        # test Update:
        result.city = 'Madrid'
        result = await result.update()
        pytest.assume(result.city == 'Madrid')
        # test get (returns other row replacing current)
        madrid = await result.fetch(city='Madrid')
        pytest.assume(madrid.city == 'Madrid')
        # test Delete:
        result = await madrid.delete()
        assert result is not None
        try:
            madrid = await airport.fetch(city='Madrid')
        except NoDataFound as ex:
            assert ex is not None
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
            pytest.assume(isinstance(result, Model))
            pytest.assume(hasattr(result, 'iata'))
        # test query (all)
        print(' === ITERATE OVER ALL === ')
        airports = await Airport.all()
        for airport in airports:
            pytest.assume(isinstance(airport, Model))
            pytest.assume(hasattr(airport, 'iata'))
        # test query (get one)
        print('= Test One: = ')
        paris = await Airport.get(city='Paris')
        pytest.assume(paris.iata == 'CDG')
        # test query (get many)
        print(' == Test Many ==')
        moscow = await Airport.filter(city='Moscow')
        for airport in moscow:
            pytest.assume(airport.country == 'Russia')
        # test query SELECT
        print('== Test Query SELECT =')
        us = await Airport.select("WHERE country = 'United States'")
        for airport in us:
            pytest.assume(airport.country == 'United States')
            pytest.assume(airport.iata in ('ORD', 'JFK'))
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
            pytest.assume(isinstance(airport, Model))
            pytest.assume(hasattr(airport, 'iata'))
        print('== Removing Many == ')
        deleted = await Airport.remove(country='Russia')
        pytest.assume(deleted is not None)
        # Updating many
        print('== Updating Many ==')
        updated_airports = await Airport.updating(_filter={"country": 'Brasil'}, country='Brazil')
        for airport in updated_airports:
            pytest.assume(isinstance(airport, Model))
            pytest.assume(hasattr(airport, 'iata'))
        updated = {
            "country": 'Venezuela'
        }
        updated_airports = await Airport.updating(updated, iata='BBM', airport='Jacinto Lara', city='Barquisimeto')
        for airport in updated_airports:
            pytest.assume(isinstance(airport, Model))
            pytest.assume(hasattr(airport, 'iata'))

def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
