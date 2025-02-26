import asyncio
from navconfig import config, BASE_DIR
from asyncdb import AsyncDB
from asyncdb.drivers.mongo import mongo


params = {
    "host": config.get('DOCUMENTDB_HOSTNAME'),
    "port": config.get('DOCUMENTDB_PORT'),
    "username": config.get('DOCUMENTDB_USERNAME'),
    "password": config.get('DOCUMENTDB_PASSWORD'),
    "database": "navigator",
    "dbtype": "documentdb",
    "ssl": True,
    "tlsCAFile": BASE_DIR.joinpath('env', "global-bundle.pem"),
}


async def test_connect(params):
    db = AsyncDB('mongo', params=params)
    async with await db.connection() as conn:
        await conn.use('navigator')
        # count documents:
        print(await conn.count_documents(collection_name='races'))
        result, _ = await conn.queryrow(
            collection_name='races',
        )
        print('Result :', result)
        # query with filtering:
        query = {'race_name': 'Breitenberg, Kihn and Walter Death March'}
        result, _ = await conn.query(
            collection_name='races',
            query=query,
        )
        print('Filtered Result :', result)
        query = {'race_id': 5375}
        result, _ = await conn.query(
            collection_name='races',
            query=query,
        )
        print('Filtered Result :', result)
        # Convert all dates to datetime objects:
        conditions = {
            "race_start_date": "$toDate",
            "race_end_date": "$toDate"
        }
        result = await conn.update_many(
            collection_name='races',
            conditions=conditions,
        )
        print('Updated Result :', result)

async def check_connection():
    async with mongo(
        params=params
    ) as db_driver:
        is_connected = await db_driver.test_connection()
        if is_connected:
            print("Successfully connected to MongoDB.")
        else:
            print("Failed to connect to MongoDB.")


if __name__ == '__main__':
    asyncio.run(check_connection())
    asyncio.run(test_connect(params))
