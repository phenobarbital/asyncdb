import asyncio
import re
from pathlib import Path
from asyncdb import AsyncDB
from asyncdb.models import Model, Field

DRIVER='bigquery'
# create a pool with parameters
PARAMS = {
    "credentials": "~/proyectos/navigator/asyncdb/env/key.json",
    "project_id": "unique-decker-385015"
}

BASE_DIR = Path(__file__).resolve().parent.parent
sql_file = BASE_DIR.joinpath('docs', 'countries_flags.bigquery.sql')

table_schema = """
CREATE TABLE IF NOT EXISTS library.country_flags (
  country STRING,
  cyclist_name STRING,
  flag INT64
);
"""

def parse_inserts(statements):
    rows_to_insert = []

    for statement in statements:
        match = re.search(r"INSERT INTO \w+\.\w+ \(([\w, ]+)\) VALUES \((.+)\);", statement)
        if match:
            columns = [col.strip() for col in match.group(1).split(",")]
            values = [val.strip() for val in match.group(2).split(",")]
            # Remove quotes and convert to appropriate types
            values = [int(val) if val.isdigit() else val.strip("'") for val in values]
            row_dict = dict(zip(columns, values))
            rows_to_insert.append(row_dict)
    return rows_to_insert

class CountryFlag(Model):
    country: str = Field(primary_key=True)
    cyclist_name: str = Field(required=True, primary_key=True)
    flag: int = Field(primary_key=True)

    class Meta:
        name = 'country_flags'
        schema = 'library'
        driver = 'bigquery'
        strict = True

async def test_connection():
    bq = AsyncDB('bigquery', params=PARAMS)
    await bq.connection()
    print(
        f"Connected: {bq.is_connected()}"
    )
    print('TEST ', await bq.test_connection())
    query = """
        SELECT corpus AS title, COUNT(word) AS unique_words
        FROM `bigquery-public-data.samples.shakespeare`
        GROUP BY title
        ORDER BY unique_words
        DESC LIMIT 10
    """
    results, error = await bq.query(query)
    for row in results:
        title = row['title']
        unique_words = row['unique_words']
        print(f'{title:<20} | {unique_words}')

async def start_test():
    db = AsyncDB(DRIVER, params=PARAMS)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        ### Sample: create a dataset, a table and load a dataset
        dataset = await conn.create_keyspace('library')
        print('Dataset created: ', dataset)
        # CREATE TABLE:
        result, error = await conn.execute(
            table_schema
        )
        print('CREATE > ', result, error)
        if sql_file.exists():
            print('READING FILE: ', sql_file)
            # read cql_file into a list of sentences
            sentences = []
            with open(sql_file, 'r') as file:
                sentences = file.readlines()
            chunk_size = 1000
            chunks = [sentences[i:i+chunk_size] for i in range(0, len(sentences), chunk_size)]
            for chunk in chunks:
                # Parse the inserts
                data = parse_inserts(chunk)
                print('DATA > ', len(data), data[0], type(data[0]))
                await conn.write(
                    data,
                    table_id='country_flags',
                    dataset_id='library',
                    use_streams=False,
                    use_pandas=False
                )
                break
            # Count models:
            count, error = await conn.query('SELECT COUNT(*) FROM unique-decker-385015.library.country_flags;')
            for c in count:
                print(c)


async def finish_test():
    db = AsyncDB(DRIVER, params=PARAMS)
    async with await db.connection() as conn:
        await conn.execute('DROP TABLE library.country_flags;')
        await conn.drop_keyspace('library')

async def test_operations():
    db = AsyncDB(DRIVER, params=PARAMS)
    async with await db.connection() as conn:
        print('TEST CONNECTED: ', conn.is_connected() is True)
        # using row factories
        result, err = await conn.query(
            'SELECT * from library.country_flags LIMIT 100',
            factory='pandas'
        )
        print(result, 'Error: ', err)
        result, err = await conn.query(
            'SELECT * from library.country_flags LIMIT 100',
            factory='recordset'
        )
        print(result, len(result), 'Error: ', err)
        # output formats
        db.output_format('json')  # change output format to json
        result, err = await conn.query('SELECT * from library.country_flags LIMIT 1')
        print(result, 'Error: ', err)
        for row in result:
            print(row)

async def test_model():
    db = AsyncDB(DRIVER, params=PARAMS)
    async with await db.connection() as conn:
        # Set the model to use the connection
        CountryFlag.Meta.connection = conn

        # get all records:
        all_records = await CountryFlag.all()
        print('ALL RECORDS > ', len(all_records))

        # Get one single record:
        Kissee = await CountryFlag.get(
            country="Algeria",
            cyclist_name="Kissee",
            flag=3
        )
        print('Get Kissee > ', Kissee)

        filter = {
            "country": "Austria"
        }
        result = await CountryFlag.filter(**filter)
        for res in result:
            print('RESULT > ', res)

        # Insert a new record:
        new_record = CountryFlag(
            country="Venezuela",
            cyclist_name="Jesus",
            flag=233
        )
        await new_record.insert()
        print('INSERT > ', new_record)

        # # Delete the record:
        # result = await new_record.delete()
        # print('DELETED > ', result)
        # # Update the record:
        # katie.flag = 233
        # await katie.update()
        # print('UPDATED > ', katie)
        # # Batch operation:
        # brazil = await CountryFlag.filter(country="Brazil")
        # for b in brazil:
        #     print(b)
        # # Delete all records:
        # result = await CountryFlag.remove(country="Brazil")
        # print('REMOVED > ', result)


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            test_connection()
        )
        loop.run_until_complete(
            start_test()
        )
        # loop.run_until_complete(
        #     test_operations()
        # )
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
