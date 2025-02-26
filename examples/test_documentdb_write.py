import asyncio
from navconfig import config, BASE_DIR
import pandas as pd
from asyncdb import AsyncDB



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

print('RAW PARAMS: ', params)

async def test_connect(params):
    db = AsyncDB('mongo', params=params)
    async with await db.connection() as conn:
        print('CONNECTED: ', conn.is_connected() is True)
        # First, create a database:
        await conn.use('navigator')
        # load the stores:
        stores = pd.read_csv(BASE_DIR.joinpath('docs', 'calendar-table-data.csv'))
        # Create a collection:
        await conn.create_collection('navigator', 'races', unique_index=True, pk='race_id')
        await conn.write(
            data=stores,
            collection='races',
            database='navigator',
            use_pandas=True,
            if_exists='replace',  # Replace existing documents
            key_field='race_id'  # Use 'race_id' as primary key
        )


if __name__ == '__main__':
    asyncio.run(test_connect(params))
