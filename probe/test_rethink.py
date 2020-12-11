import asyncio
import logging

loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)
import iso8601
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


from asyncdb import AsyncDB, AsyncPool
from asyncdb.exceptions import NoDataFound, ProviderError
from asyncdb.providers.rethink import rethink


params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

rt = AsyncDB("rethink", params=params, loop=loop)


data = [{'inserted_at': iso8601.parse_date('2020-12-09 04:23:52.441312+0000'),
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
    }]

print(data)
async def connect(db):
    async with await db.connection() as conn:
        print("Connected: {}".format(conn.is_connected))
        print(await conn.test_connection())
        conn.use('troc')
        db = await conn.list_databases()
        print(db)
        filter = {"company_id": 10, "state_code": "FL"}
        result, error = await conn.query('troc_populartimes', filter=filter)
        if not error:
            for row in result:
                #print(row)
                pass
        # insert data:
        conn.use('walmart')
        inserted = await conn.insert('walmart_scorecard', data)
        print(inserted)
try:
    loop.run_until_complete(connect(rt))
finally:
    loop.run_until_complete(rt.close())
    # Find all running tasks:
    print("Shutdown complete ...")
    #loop.stop()
