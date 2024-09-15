import asyncio
from asyncdb.drivers.clickhouse import clickhouse
from pprint import pprint


table = """
CREATE TABLE iris (
    sepal_length Decimal32(2),
    sepal_width Decimal32(2),
    petal_length Decimal32(2),
    petal_width Decimal32(2),
    species String
) ENGINE = MergeTree
PARTITION BY species
ORDER BY (species)
SETTINGS index_granularity = 8192;
"""

async def connect(db):
    async with await db.connection() as conn:
        result, error = await conn.test_connection()
        print('RESULT > ', result)
        await conn.execute(
            "DROP TABLE IF EXISTS iris;"
        )
        # Create A Table:
        await conn.execute(
            table
        )
        # Insert Data:
        insert = "INSERT INTO iris (sepal_length, sepal_width, petal_length, petal_width, species) VALUES"
        data = [
            (5.1, 3.7, 1.5, 0.4, 'Iris-setosa'),
            (4.6, 3.6, 1.0, 0.2, 'Iris-setosa')
        ]
        result, error = await conn.execute(
            insert, params=data
        )
        print('INSERTED > ', result, 'Error: ',  error)
        # getting one single row:
        row, error = await conn.queryrow(
            "SELECT * FROM iris"
        )
        print(row)
        # at the end, drop the table:
        await conn.execute(
            "DROP TABLE iris;"
        )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    params = {
        "host": "localhost",
        "port": 9000,
        "user": "default",
        "password": "u69ebsZQ",
        "database": "default",
        "client_name": "ASYNCDB",
        "secure": False
    }
    driver = clickhouse(params=params, loop=loop)
    print('DRV > ', driver)
    loop.run_until_complete(connect(driver))
