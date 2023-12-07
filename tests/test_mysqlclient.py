import pytest
from asyncdb import AsyncDB
from asyncdb.drivers.mysqlclient import mysqlclientPool

# Define your database parameters
params = {
    "host": "127.0.0.1",
    "port": "3306",
    "user": "root",
    "password": "12345678",
    "database": "navigator_dev"
}

# Define a fixture for the pool
@pytest.fixture
async def pool():
    p = mysqlclientPool(params=params)
    await p.connect()
    yield p
    await p.close()

@pytest.mark.asyncio
async def test_pooler(pool):
    print(f"Connected: {pool.is_connected()}")
    result = await pool.execute("SELECT 1")
    print(result)
    test, error = await pool.test_connection()
    print('TEST > ', test)
    db = await pool.acquire()
    await pool.release(connection=db.get_connection())
    async with await pool.acquire() as conn:
        result, error = await conn.test_connection()
        print('TEST > ', result, 'Error: ', error)
    print('Is closed: ', {db.is_connected()})

@pytest.mark.asyncio
async def test_mysql():
    db = AsyncDB("mysqlclient", params=params)
    async with await db.connection() as conn:
        print('CONNECTION > ', conn)
        print(f"DB Connected: {conn.is_connected()}")
        await conn.execute("""
            CREATE TABLE `color` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(32) DEFAULT NULL,
                `hex` VARCHAR(7) DEFAULT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
        """)
        data = [
            ("Red", "#ff0000"),
            ("Green", "#00ff00"),
            ("Blue", "#0000ff"),
            # ... other colors
        ]
        sql = "INSERT INTO color (name, hex) VALUES (%s, %s)"
        await conn.executemany(sql, data)
        sql = "select * from color"
        result, error = await conn.query(sql)
        for row in result:
            print(row)
        await conn.execute("TRUNCATE TABLE color")
        await conn.execute("DROP TABLE color;")
