import pytest
from asyncdb import AsyncDB
import asyncio
import pytest
import iso8601
import pytest_asyncio
import pandas
import datatable as dt
from asyncdb.meta import Record, Recordset

DRIVER = 'rethink'
params = {
    "host": "localhost",
    "port": "28015",
    "db": "troc"
}

params_auth = {
    "host": "localhost",
    "port": "28015",
    "db": "epson",
    "user": "test",
    "password": "supersecret"
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
async def test_cursors(driver, event_loop):
    db = AsyncDB(driver, params=params, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    assert db.is_connected() is True
    await db.use('epson')
    async with db.cursor(
            table="epson_api_photo_categories") as cursor:
        async for row in cursor:
            pytest.assume(type(row) == dict)
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

data = [
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


async def test_operations(conn):
    pytest.assume(conn.is_connected() is True)
    db = await conn.list_databases()
    pytest.assume(type(db) == list)
    await conn.createdb('test')
    db = await conn.list_databases()
    pytest.assume('test' in db)
    await conn.use('test')
    await conn.create_table('scorecard')
    inserted = await conn.insert('scorecard', data)
    print(inserted)
    result, error = await conn.query('scorecard', {"company_id": 1})
    pytest.assume(not error)
    pytest.assume(result is not None)
    conn.output_format('record')  # change output format to iter generator
    row, error = await conn.queryrow('scorecard', {"store_id": 4235})
    pytest.assume(not error)
    pytest.assume(type(row) == Record)
    pytest.assume(row.accessories_sales == 10)
    await conn.drop_table('scorecard')
    tables = await conn.list_tables()
    pytest.assume('scorecard' not in tables)
    # testing between:
    await conn.use('epson')
    conn.output_format('native')  # change output format to iter generator
    result, error = await conn.between(
        table="epson_api_photo_categories",
        min=546, max=552, idx="categoryId"
    )
    pytest.assume(not error)
    pytest.assume(len(result) == 5)
    pytest.assume(result[0]['name'] == "Competitor Intel")


async def test_formats(conn):
    pytest.assume(conn.is_connected() is True)
    await conn.use('epson')
    conn.output_format('native')  # change output format to native
    result, error = await conn.query(
        table="epson_api_photo_categories"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    pytest.assume(len(result) == 13)
    conn.output_format('record')  # change output format to list of records
    result, error = await conn.query(
        table="epson_api_photo_categories"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == list)
    for row in result:
        pytest.assume(type(row) == Record)
    conn.output_format('recordset')  # change output format to recordset
    result, error = await conn.query(
        table="epson_api_photo_categories"
    )
    pytest.assume(not error)
    pytest.assume(type(result) == Recordset)
    for row in result:
        pytest.assume(type(row) == Record)
    conn.output_format('datatable')  # change output format to Datatable Frame
    result, error = await conn.query(
        table="epson_api_photo_categories"
    )
    pytest.assume(not error)
    print(result)
    pytest.assume(type(result) == dt.Frame)
    conn.output_format('pandas')  # change output format to Pandas Dataframe
    result, error = await conn.query(
        table="epson_api_photo_categories"
    )
    pytest.assume(not error)
    print(result)
    pytest.assume(type(result) == pandas.core.frame.DataFrame)


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
