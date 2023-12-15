import asyncio
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
        # Check Methods:
        print(airport)
        result = await airport.insert()
        print('INSERTED: ', result)
        # test Update:
        result.city = 'Madrid'
        result = await result.update()
        print('UPDATED: ', result)
        # test get (returns other row replacing current)
        madrid = await result.fetch(city='Madrid')
        print('MADRID IS: ', madrid)
        # test Delete:
        result = await madrid.delete()
        print('DELETED: ', result)
        data = {
            "iata": 'ESP',
            "airport": 'Adolfo Suarez',
            "city": 'Medrid',
            "country": 'Spain'
        }
        esp = Airport(**data)
        madrid = await esp.insert()
        print('INSERTED: ', madrid, madrid.iata, madrid.old_value('iata'))
        # Then, update but changing the PK:
        madrid.iata = 'MAD'
        result = await madrid.update()
        print('UPDATED: ', result, result.iata, result.old_value('iata'))
        # Try to insert an existing value using save:
        data = {
            "iata": 'MAD',
            "airport": 'Adolfo Suarez',
            "city": 'Madrid',
            "country": 'Spain'
        }
        mad = Airport(**data)
        await mad.save()
        print('SAVED: ', mad, mad.iata, mad.old_value('iata'))
        # test query (all)
        # at end: bulk insert, bulk update and bulk remove
        print(' == TEST Bulk Insert == ')
        data = [
            {"iata": "AEP", "airport": "Jorge Newbery", "city": "Buenos Aires", "country": "Argentina"},
            {"iata": "ADL", "airport": "Adelaide International", "city": "Adelaide", "country": "Australia"},
            {"iata": "BSB", "airport": "President Juscelino Kubitschek", "city": "Brasilia", "country": "Brasil"},
            {"iata": "GRU", "airport": "São Paulo-Guarulhos", "city": "Sao Paulo", "country": "Brasil"},
            {"iata": "CCS", "airport": "Simon Bolívar Maiquetia", "city": "Caracas", "country": "Venezuela"},
        ]
        await Airport.create(data)
        print(' === ITERATE OVER ALL === ')
        airports = await Airport.all()
        for airport in airports:
            print(airport)

if __name__ == "__main__":
    params = {
        "user": "troc_pgdata",
        "password": "12345678",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "navigator",
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
