import asyncio
import logging
from pprint import pprint
import iso8601
from asyncdb import AsyncDB
from asyncdb.drivers.rethink import Point


logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

scdata = [
    {'inserted_at': iso8601.parse_date('2020-12-09 04:23:52.441312+0000'),
    'description': 'TERRE HAUTE-WM - #4235', 'company_id': 1,
    'territory_id': 2.0, 'territory_name': '2-Midwest',
    'region_name': 'William Pipkin', 'district_id': 235.0,
    'district_name': '235-IN/IL Border', 'market_id': 2355.0,
    'market_name': 'IN/IL Border-2355', 'store_id': 4235.0,
    'store_name': 'TERRE HAUTE-WM - #4235', 'num_stores': 1,
    'seasonality': 0.959102399403875,
    'postpaid_num_lines': 1.0, 'postpaid_handset_lines': 1.0,
    'handset_percent': 1.0, 'postpaid_tablet_lines': 0.0,
    'tablet_percent': 0.0, 'postpaid_wearables_lines': 0.0,
    'wearables_percent': 0.0, 'post_lines': 31.03,
    'postpaid_to_goal': 0.033601080797658577,
    'trend_apd': 1.0426415371513456, 'care_plan': 0.0, 'care_plan_attach': 0.0,
    'care_plan_to_goal': 0.0, 'weight_postpaid': 0.020160648478595146,
    'prepaid_goal': 100.0, 'prepaid_sales': 1927.4, 'prepaid_quantity': 17.0,
    'prepaid_to_goal': 0.17724906131572873,
    'weight_prepaid': 0.03544981226314575, 'accessories_sales': 10.0,
    'accessories_to_postpaid_ratio': 10.0,
    'accessories_to_goal': 5.2132076857567275, 'weight_accessory': 2.0,
    'weight_score': 2.055610460741741, 'market_ranking': 3.0,
    'district_ranking': 11.0, 'region_ranking': 43.0,
    'territory_ranking': 210.0, 'company_ranking': 373.0,
    'filterdate': iso8601.parse_date('2020-10-30 00:00:00+0000'),
    'store_tier': None,
    'launch_group': None
    }
]

async def test_connect(event_loop):
    rt = AsyncDB("rethink", params=params, loop=event_loop)
    print(rt)
    async with await rt.connection() as conn:
        print(f"Connected: {conn.is_connected()}")
        print('== TEST ==')
        result, error = await conn.test_connection()
        print('TEST: ', result, error)
        dbs = await conn.list_databases()
        print('Current databases: ', dbs)
        await conn.create_database('testing', use=True)
        print('Created database testing ===')
        created = await conn.create_table('sample_scorecard', pk='store_id')
        print(created)
        print('Check if table already exists: ')
        exists = await conn.list_tables()
        print('Exists? ', 'sample_scorecard' in exists)
        inserted = await conn.insert('sample_scorecard', scdata)
        print('INSERTED: ', inserted)
        await conn.drop_table('sample_scorecard')
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
        await conn.insert('places', data)
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


async def create_rethink_table(event_loop):
    rt = AsyncDB('rethink', params=params, loop=event_loop)
    print(rt)
    async with await rt.connection() as conn:  #pylint: disable=E1101
        await conn.create_database('navigator', use=True)
        await conn.create_table('chatbots_usage')
        exists = await conn.list_tables()
        print('Exists? ', 'chatbots_usage' in exists)
        # chatbot loaders:
        await conn.create_table('chatbots_data')
        created = await conn.create_index(
            table='chatbots_data', field=None, name='data_version_control', fields=['chatbot_id', 'source_type', 'version']
        )
        print('CREATED > ', created)
        exists = await conn.list_tables()
        print('Exists? ', 'chatbots_data' in exists)

if __name__ == '__main__':
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        loop.run_until_complete(test_connect(loop))
        loop.run_until_complete(create_rethink_table(loop))
    finally:
        loop.stop()
