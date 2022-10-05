import asyncio
from asyncdb.utils import cPrint
from asyncdb import AsyncDB, ABS_PATH
from asyncdb.models import Model, Column

class Airport(Model):
    iata: str = Column(primary_key=True)
    city: str
    country: str
    class Meta:
        name: str = 'airports'


async def test_model(driver):
    cPrint('Testing Model')
    async with await driver.connect() as conn:
        print(f'Is Connected: {conn.is_connected()}')

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
    await driver.close()

sqlserver = {
    "driver": "sqlserver",
    "host": "localhost",
    "port": 1433,
    "database": "AdventureWorks2019",
    "user": 'SA',
    "password": 'P4ssW0rd1.',
    "jar": [
        ABS_PATH.joinpath('bin', 'jar', 'mssql-jdbc-8.4.1.jre8.jar'),
    ],
    "options": {
        # integratedSecurity: 'true',
        # encrypt: 'true'
    }
}

postgresql = {
    "driver": "postgresql",
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": 5432,
    "database": "navigator_dev",
    "jar": [
        'postgresql-42.5.0.jar'
    ],
    "classpath": '/home/jesuslara/proyectos/navigator/asyncdb/bin/jar/'
}

mysql = {
    "driver": "mysql",
    "user": "root",
    "password": "12345678",
    "host": "localhost",
    "port": 3306,
    "database": "navigator_dev",
    "jar": [
        ABS_PATH.joinpath('bin', 'jar', 'mysql-connector-java-8.0.30.jar'),
    ]
}

cassandra = {
    "driver": "cassandra",
    "host": "127.0.0.1",
    "port": 9042,
    "user": 'cassandra',
    "password": 'cassandra',
    "database": 'navigator',
    "jar": [
        ABS_PATH.joinpath('bin', 'jar', 'CassandraJDBC4.jar'),
    ]
}

oracle = {
    "driver": "oracle",
    "host": "127.0.0.1",
    "port": 1521,
    "user": 'system',
    "password": 'oracle',
    "database": 'xe',
    "jar": [
        'ojdbc8.jar'
    ],
    "classpath": '/home/jesuslara/proyectos/navigator/asyncdb/bin/jar/'
}

test2 = {
    "driver": "cassandra",
    "host": "127.0.0.1",
    "port": 9042,
    "user": "cassandra",
    "password": "cassandra",
    "database": "navigator",
    "jar": [
        "CassandraJDBC4.jar"
        # ABS_PATH.joinpath('bin', 'jar', 'CassandraJDBC4.jar'),
    ],
    "classpath": '/home/jesuslara/proyectos/navigator/asyncdb/bin/jar/'
}

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        driver = AsyncDB("jdbc", params=postgresql, loop=loop)
        loop.run_until_complete(test_connect(driver))
        # d = AsyncDB("jdbc", params=mysql, loop=loop)
        # loop.run_until_complete(test_model(d))
        # o = AsyncDB("jdbc", params=oracle, loop=loop)
        # asyncio.run(test_model(o))
    finally:
        loop.stop()
