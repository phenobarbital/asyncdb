import asyncio
import iso8601
import pytest
import pytest_asyncio
import pandas
import datatable as dt
from asyncdb.meta.record import Record
from asyncdb.meta.recordset import Recordset
from asyncdb import AsyncDB

DRIVER = 'rethink'
params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

params_auth = {
    "host": "localhost",
    "port": "28015",
    "db": "troc",
#     "user": "test",
#     "password": "supersecret"
}


@pytest.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    await db.connection()
    yield db
    await db.close()

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect_by_params(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    assert db.is_connected() is True
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_connect(driver, event_loop):
    db = AsyncDB(driver, params=params_auth, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_auth(driver, event_loop):
    db = AsyncDB(driver, params=params_auth, loop=event_loop)
    await db.connection()
    pytest.assume(db.is_connected() is True)
    result, error = await db.test_connection()
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    await db.close()
    assert db.is_closed() is True


async def test_connection(conn):
    pytest.assume(conn.is_connected() is True)
    result, error = await conn.test_connection()
    pytest.assume(not error)
    pytest.assume(type(result) == list)

scdata = [
    {
        'inserted_at': iso8601.parse_date('2020-12-09 04:23:52.441312+0000'),
        'description': 'TERRE HAUTE-WM - #4235', 'company_id': 1,
        'territory_id': 2.0, 'territory_name': '2-Midwest',
        'region_name': 'William Pipkin', 'district_id': 235.0,
        'district_name': '235-IN/IL Border', 'market_id': 2355.0,
        'market_name': 'IN/IL Border-2355', 'store_id': 4235,
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
        'weight_prepaid': 0.03544981226314575,
        'accessories_sales': 10.0,
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


async def test_operations(conn):
    pytest.assume(conn.is_connected() is True)
    db = await conn.list_databases()
    pytest.assume(type(db) == list)
    await conn.createdb('testing')
    db = await conn.list_databases()
    pytest.assume('testing' in db)
    await conn.use('testing')
    await conn.create_table('scorecard', exists_ok=True)
    inserted = await conn.insert('scorecard', scdata)
    result, error = await conn.query('scorecard', {"company_id": 1})
    pytest.assume(not error)
    pytest.assume(result is not None)
    conn.output_format('record')  # change output format to iter generator
    row, error = await conn.queryrow('scorecard', {"store_id": 4235})
    pytest.assume(not error)
    pytest.assume(type(row) == Record)
    pytest.assume(row.store_id == 4235)
    await conn.drop_table('scorecard')
    tables = await conn.list_tables()
    pytest.assume('scorecard' not in tables)
    # testing between:
    # places table
    created = await conn.create_table('places', pk='place_id')
    pytest.assume(isinstance(created, dict))
    pytest.assume(created['tables_created'] == 1)
    pytest.assume('places' in await conn.list_tables())
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
    conn.output_format('native')  # change output format to iter generator
    # getting one single row:
    site1, error = await conn.queryrow(
        'places',
        columns=['address', 'city', 'state_code', 'name', 'place_id', 'company_id'],
        place_id='3'
    )
    pytest.assume(site1['address'] == "789 Oak St")
    # Create an Index:
    result = await conn.create_index(table='places', field='company_id', name='idxCompany')
    pytest.assume(isinstance(result, dict))
    pytest.assume(result['created'] == 1)
    # Between Query:
    result, error = await conn.between(
        table="places",
        min="1", max="5", idx="place_id"
    )
    pytest.assume(not error)
    pytest.assume(len(result) == 15)
    pytest.assume(result[0]['city'] == 'Springfield')


async def test_formats(conn):
    pytest.assume(conn.is_connected() is True)
    db = await conn.list_databases()
    pytest.assume('testing' in db)
    await conn.use('testing')
    result, error = await conn.query(
        table="places"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    pytest.assume(len(result) == 20)
    conn.output_format('record')  # change output format to list of records
    result, error = await conn.query(
        table="places"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    for row in result:
        pytest.assume(type(row) == Record)
    conn.output_format('recordset')  # change output format to recordset
    result, error = await conn.query(
        table="places"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == Recordset)
    for row in result:
        pytest.assume(type(row) == Record)
    conn.output_format('dt')  # change output format to Datatable Frame
    result, error = await conn.query(
        table="places"
    )
    pytest.assume(not error)
    print(result)
    pytest.assume(type(result) == dt.Frame)
    conn.output_format('pandas')  # change output format to Pandas Dataframe
    result, error = await conn.query(
        table="places"
    )
    pytest.assume(not error)
    print(result)
    pytest.assume(type(result) == pandas.DataFrame)
    pytest.assume(len(result) == 20)

@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_cursors(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    assert db.is_connected() is True
    await db.use('testing')
    async with db.cursor(table="places") as cursor:
        async for row in cursor:
            pytest.assume(type(row) == dict)
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_delete(driver, event_loop):
    rt = AsyncDB(driver, params=params, loop=event_loop)
    async with await rt.connection() as conn:
        await conn.use('testing')
        await conn.drop_database('testing')

def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
