import asyncio
import logging
from asyncdb.drivers.hazel import hazel, HazelPortable

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Customer(HazelPortable):
    name: str
    age: int

def on_response(sql_result_future):
    iterator = sql_result_future.result().iterator()

    def on_next_row(row_future):
        try:
            row = row_future.result()
            # Process the row.
            print(row)

            # Iterate over the next row.
            next(iterator).add_done_callback(on_next_row)
        except StopIteration:
            # Exhausted the iterator. No more rows are left.
            pass
    next(iterator).add_done_callback(on_next_row)

async def test_customer(params, event_loop):
    c1 = Customer(name="Peter", age=42)
    c2 = Customer(name="John", age=23)
    c3 = Customer(name="Joe", age=33)
    hz = hazel(params=params, loop=event_loop)
    async with await hz.connection() as conn:
        print('=============================')
        print(f"Is Connected: {conn.is_connected()}")
        # add those customers to Hazelcast:
        await conn.set(1, c1, map_name='customers')
        await conn.set(2, c2, map_name='customers')
        await conn.set(3, c3, map_name='customers')
        result, error = await conn.query(map_name='customers')
        print('Result: ', result, 'Error: ', error)
        result, error = await conn.execute('DROP MAPPING IF EXISTS my_customers')
        result, error = await conn.execute(
            """
            CREATE OR REPLACE MAPPING my_customers (
                __key INT,
                name VARCHAR,
                age INT
            )
            TYPE IMap
            OPTIONS (
             'keyFormat' = 'int',
             'valueFormat' = 'portable',
             'valuePortableFactoryId' = '1',
             'valuePortableClassId' = '1',
             'valuePortableClassVersion' = '0'
            )
            """
        )
        print('MAP: ', result, error)
        print(result.update_count())
        print('EXEC QUERY ==== ')
        result, error = await conn.execute(
            "SELECT name, age FROM my_customers WHERE age < 18", map_name='customers'
        )
        print('ERROR: ', error)
        print('RESULT ', result, result.is_row_set())
        if result.is_row_set():
            print('METADATA: ', result.get_row_metadata())
            for row in result:
                print(row)
                print('ROW ', row.get_object('name'))
        # DROPPING MAPPING:
        result, error = await conn.execute('DROP MAPPING IF EXISTS my_customers')


async def test_connect(params, event_loop):
    hz = hazel(params=params, loop=event_loop)
    async with await hz.connection() as conn:
        print(f"Is Connected: {conn.is_connected()}")
        print('CONN ', conn)
        await conn.put("Test2", "No More Sweet Music")
        value = await conn.get("Test2")
        print(value)

        await conn.set_multi('Test3', {"Test2": "Testing 2"}, {"Test3": "Testing 3"})

        values = await conn.get_multi("Test3")
        print('VALUES: ', values)

        await conn.delete("Test2")
        # delete all
        await conn.delete_multi("Test2", "Test3")

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        params = {"host": "172.17.0.2", "port": 5701, "factories": [Customer]}
        # loop.run_until_complete(test_connect(params, loop))
        loop.run_until_complete(test_customer(params, loop))
    finally:
        loop.stop()
