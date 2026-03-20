import asyncio
from pathlib import Path
from asyncdb import AsyncDB
from asyncdb.models import Model, Field

DRIVER='scylladb'
ARGS = {
    "host": "127.0.0.1",
    "port": "9042",
    "username": 'navigator',
    "password": 'navigator'
}

BASE_DIR = Path(__file__).resolve().parent.parent
cql_file = BASE_DIR.joinpath('docs', 'country_flag-table-data.cql')

table_schema = """
CREATE TABLE country_flags (
    country text,
    cyclist_name text,
    flag int,
    PRIMARY KEY (country, cyclist_name, flag)
)
WITH default_time_to_live = 600;
"""

class CountryFlag(Model):
    country: str = Field(primary_key=True)
    cyclist_name: str = Field(required=True, primary_key=True)
    flag: int = Field(primary_key=True)

    class Meta:
        name = 'country_flags'
        schema = 'library'
        driver = 'scylladb'
        ttl = 600
        strict = True


async def start_test():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        await conn.create_keyspace('library')
        # creation and insertion:
        result, error = await conn.execute(
            table_schema
        )
        print('CREATE > ', result, error)
        if cql_file.exists():
            print('READING FILE: ', cql_file)
            # read cql_file into a list of sentences
            sentences = []
            with open(cql_file, 'r') as file:
                sentences = file.readlines()
            chunk_size = 5000
            chunks = [sentences[i:i+chunk_size] for i in range(0, len(sentences), chunk_size)]
            for chunk in chunks:
                result, error = await conn.execute_batch(chunk)
            # Count models:
            count, error = await conn.query('SELECT COUNT(*) FROM country_flags;')
            for c in count:
                print(c)


async def finish_test():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        await conn.use('library')  # set database to work
        await conn.execute('DROP TABLE country_flags;')
        await conn.drop_keyspace('library')

async def test_operations():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        await conn.use('library')  # set database to work
        # using row factories
        result, _ = await conn.query('SELECT * from country_flags LIMIT 10000', factory='pandas')
        print(result.result)
        result, _ = await conn.query('SELECT * from country_flags LIMIT 10000', factory='recordset')
        print(result.result)
        # output formats
        db.output_format('json')  # change output format to json
        result, _ = await conn.query('SELECT * from country_flags LIMIT 1')
        print(result)

async def test_model():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        await conn.use('library')  # set database to work
        # Set the model to use the connection
        CountryFlag.Meta.connection = conn
        filter = {
            "country": "Venezuela",
            "cyclist_name": "Marty",
        }
        result = await CountryFlag.filter(**filter)
        for res in result:
            print('RESULT > ', res)
        # Get one single record:
        katie = await CountryFlag.get(
            country="Anguilla",
            cyclist_name="Kathie",
            flag=7
        )
        print('RESULT > ', katie)
        # Insert a new record:
        new_record = CountryFlag(
            country="Venezuela",
            cyclist_name="Jesus",
            flag=233
        )
        await new_record.insert()
        print('INSERT > ', new_record)
        # Delete the record:
        result = await new_record.delete()
        print('DELETED > ', result)
        # Update the record:
        katie.flag = 233
        await katie.update()
        print('UPDATED > ', katie)
        # Batch operation:
        brazil = await CountryFlag.filter(country="Brazil")
        for b in brazil:
            print(b)
        # Delete all records:
        result = await CountryFlag.remove(country="Brazil")
        print('REMOVED > ', result)
        # get all records:
        all_records = await CountryFlag.all()
        print('ALL RECORDS > ', len(all_records))

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            start_test()
        )
        loop.run_until_complete(
            test_operations()
        )
        loop.run_until_complete(
            test_model()
        )
    except Exception as err:
        print('Error: ', err)
    finally:
        loop.run_until_complete(
            finish_test()
        )
        loop.close()
