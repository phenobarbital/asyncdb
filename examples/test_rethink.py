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

data = [
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
        inserted = await conn.insert('sample_scorecard', data)
        print('INSERTED: ', inserted)
        await conn.drop_table('sample_scorecard')

        await conn.db('troc')
        _filter = {"company_id": 10, "state_code": "FL"}
        result, error = await conn.query(
            'troc_populartimes',
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
            'troc_populartimes',
            columns=['address', 'city', 'state_code', 'name', 'place_id', 'company_id', 'coordinates'],
            place_id='ChIJheKbjENx54gRDpzTZVc0NHk'
        )
        if not error:
            print('ROW ', site1)
        site2, error = await conn.queryrow(
            'troc_populartimes',
            columns=['address', 'city', 'state_code', 'name', 'place_id', 'company_id', 'coordinates'],
            place_id='ChIJMYUYxbBGFIgRMbFlFTJWD5g'
        )
        if not error:
            print('ROW ', site2)
        # return distance between site1 and 2:
        p1 = Point(*site1['coordinates'].values())
        p2 = Point(*site2['coordinates'].values())
        distance = await conn.distance(p1, p2, unit='km')
        print(f'::: Distance between {p1} and {p2}: {distance} km.')
        await conn.drop_database('testing')


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_debug(True)
        loop.run_until_complete(test_connect(loop))
    finally:
        loop.stop()
