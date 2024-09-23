import asyncio
from asyncdb.utils import cPrint
from asyncdb import AsyncDB


async def test_connect(driver):
    cPrint('Testing Connection')
    async with await driver.connect() as conn:
        print(f'Is Connected: {conn.is_connected()}')
        result, error = await conn.test_connection()
        print(result, error)
        users = "SELECT * FROM users;"
        result, error = await conn.query(users)
        if error:
            print('ERROR: ', error)
        else:
            for user in result:
                print(user)
        # fetch One:
        jesus = "SELECT * FROM users where firstname='Jesus' and lastname = 'Lara'"
        cPrint('Getting only one Row', level='DEBUG')
        result, error = await conn.queryrow(jesus)
        print(result)


postgresql = {
    "driver": "postgresql",
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": 5432,
    "database": "navigator",
    "jar": [
        'postgresql-42.5.0.jar'
    ],
    "classpath": '/home/jesuslara/proyectos/navigator/asyncdb/bin/jar/'
}

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        driver = AsyncDB("jdbc", params=postgresql, loop=loop)
        loop.run_until_complete(test_connect(driver))
    finally:
        loop.close()
