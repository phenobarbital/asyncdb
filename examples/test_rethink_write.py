import asyncio
import logging
import pandas
from pprint import pprint
from asyncdb import AsyncDB


logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

async def test_connect(event_loop):
    rt = AsyncDB("rethink", params=params, loop=event_loop)
    async with await rt.connection() as conn:
        # set DB:
        await conn.create_database('testing', use=True)
        # places table
        created = await conn.create_table('places', pk='place_id')
        data = [
            {
                "address": "123 Main St",
                "city": "Springfield",
                "state_code": "IL",
                "name": "John Doe",
                "place_id": "1",
                "company_id": "1001"
            },
            {
                "address": "456 Elm St",
                "city": "Hometown",
                "state_code": "TX",
                "name": "Jane Smith",
                "place_id": "2",
                "company_id": "1002"
            },
            {
                "address": "789 Oak St",
                "city": "Metropolis",
                "state_code": "NY",
                "name": "Alice Johnson",
                "place_id": "3",
                "company_id": "1003"
            },
            {
                "address": "101 Maple St",
                "city": "Gotham",
                "state_code": "NJ",
                "name": "Bob Brown",
                "place_id": "4",
                "company_id": "1004"
            },
            {
                "address": "202 Pine St",
                "city": "Smallville",
                "state_code": "KS",
                "name": "Charlie Davis",
                "place_id": "5",
                "company_id": "1005"
            },
            {
                "address": "303 Cedar St",
                "city": "Star City",
                "state_code": "CA",
                "name": "Dana White",
                "place_id": "6",
                "company_id": "1006"
            },
            {
                "address": "404 Birch St",
                "city": "Central City",
                "state_code": "CO",
                "name": "Eve Black",
                "place_id": "7",
                "company_id": "1007"
            },
            {
                "address": "505 Spruce St",
                "city": "Coast City",
                "state_code": "FL",
                "name": "Frank Green",
                "place_id": "8",
                "company_id": "1008"
            },
            {
                "address": "606 Walnut St",
                "city": "Keystone",
                "state_code": "PA",
                "name": "Grace Hill",
                "place_id": "9",
                "company_id": "1009"
            },
            {
                "address": "707 Chestnut St",
                "city": "Blüdhaven",
                "state_code": "DE",
                "name": "Hank Martin",
                "place_id": "10",
                "company_id": "1010"
            },
            {
                "address": "808 Ash St",
                "city": "Midway City",
                "state_code": "OH",
                "name": "Ivy King",
                "place_id": "11",
                "company_id": "1011"
            },
            {
                "address": "909 Beech St",
                "city": "Gateway City",
                "state_code": "OR",
                "name": "Jack Lee",
                "place_id": "12",
                "company_id": "1012"
            },
            {
                "address": "1010 Redwood St",
                "city": "Fawcett City",
                "state_code": "WI",
                "name": "Kara Moore",
                "place_id": "13",
                "company_id": "1013"
            },
            {
                "address": "1111 Willow St",
                "city": "National City",
                "state_code": "AZ",
                "name": "Leo Taylor",
                "place_id": "14",
                "company_id": "1014"
            },
            {
                "address": "1212 Poplar St",
                "city": "Opal City",
                "state_code": "MD",
                "name": "Mia Anderson",
                "place_id": "15",
                "company_id": "1015"
            },
            {
                "address": "1313 Holly St",
                "city": "Happy Harbor",
                "state_code": "RI",
                "name": "Nina Perez",
                "place_id": "16",
                "company_id": "1016"
            },
            {
                "address": "1414 Magnolia St",
                "city": "Hub City",
                "state_code": "NM",
                "name": "Oscar Brown",
                "place_id": "17",
                "company_id": "1017"
            },
            {
                "address": "1515 Cypress St",
                "city": "Jump City",
                "state_code": "VA",
                "name": "Paula Davis",
                "place_id": "18",
                "company_id": "1018"
            },
            {
                "address": "1616 Fir St",
                "city": "St. Roch",
                "state_code": "LA",
                "name": "Quinn Garcia",
                "place_id": "19",
                "company_id": "1019"
            },
            {
                "address": "1717 Hemlock St",
                "city": "Gateway",
                "state_code": "MA",
                "name": "Ray Harris",
                "place_id": "20",
                "company_id": "1020"
            }
        ]
        df = pandas.DataFrame(data)
        print(df)
        # Writing Data:
        await conn.write('places', data)
        # getting data:
        _filter = {"state_code": "TX"}
        result, error = await conn.query(
            'places',
            **_filter,
            order_by = ['company_id', 'city'],
            limit=10,
            columns=['address', 'city', 'state_code', 'name', 'place_id', 'company_id']
        )
        print('Error: ', error)
        if not error:
            for row in result:
                pprint(row)
        # getting one single row:
        site1, error = await conn.queryrow(
            'places',
            columns=['address', 'city', 'state_code', 'name', 'place_id', 'company_id'],
            place_id='3'
        )
        if not error:
            print('ROW ', site1)
        await conn.drop_database('testing')

if __name__ == '__main__':
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        loop.run_until_complete(test_connect(loop))
    finally:
        loop.stop()
