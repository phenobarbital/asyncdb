import asyncio
from navconfig.logging import logging
from navconfig import BASE_DIR
import pandas as pd
from dataclasses import dataclass
from asyncdb import AsyncDB

DRIVER='scylladb'
ARGS = {
    "host": "127.0.0.1",
    "port": "9042",
    "username": 'navigator',
    "password": 'navigator'
}

@dataclass
class SampleData:
    id: int
    name: str

table_schema = """
CREATE TABLE sample_table (
    id int PRIMARY KEY,
    name text
)
"""

async def test_copy():
    db = AsyncDB(DRIVER, params=ARGS)
    async with await db.connection() as conn:
        # set database to work
        await conn.create_keyspace('navigator', use=True)
        # Ensure table exists:
        result = await conn.table_exists(
            table='sample_table',
            schema=table_schema
        )

        # Case 1: DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        await conn.write(data=df, table="sample_table")

        # Case 2: Dataclass
        data_instance = SampleData(id=4, name='David')
        await conn.write(data=data_instance, table="sample_table")

        # Case 3: Dictionary
        single_dict = {'id': 5, 'name': 'Eve'}
        await conn.write(data=single_dict, table="sample_table")

        # Case 4: List of Dictionaries
        list_of_dicts = [
            {'id': 6, 'name': 'Frank'},
            {'id': 7, 'name': 'Grace'}
        ]
        await conn.write(data=list_of_dicts, table="sample_table")

        # Case 5: Using a custom sentence
        custom_sentence = "INSERT INTO sample_table (id, name) VALUES (%s, %s)"
        custom_data = (8, 'Hannah')
        await conn.write(data=custom_data, sentence=custom_sentence)

        await conn.drop_table('sample_table')


async def test_dataframe():
    db = AsyncDB(DRIVER, params=ARGS)
    # 1. Read the CSV into a Pandas DataFrame
    file = BASE_DIR.joinpath('docs', 'calendar-table-data.csv')
    df = pd.read_csv(file)

    async with await db.connection() as conn:
        # set database to work
        await conn.create_keyspace('navigator', use=True)
        # Ensure table exists:
        await conn.create_table(
            table='races',
            schema='navigator',
            pk='race_id',
            data=df
        )
        await conn.write(data=df, table="races")

        # # or using directly a CSV file:
        await conn.write(data=file, table="races")
        await conn.drop_table('races')


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            test_copy()
        )
        loop.run_until_complete(
            test_dataframe()
        )
    except Exception as err:
        print('Error: ', err)
    finally:
        loop.close()
