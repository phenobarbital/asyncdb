import asyncio
from asyncdb import AsyncDB
from asyncdb.drivers.mysql import mysqlPool


# create a pool with parameters
params = {
    "user": "root",
    "password": "12345678",
    "host": "localhost",
    "port": "3306",
    "database": "navigator_dev"
}

async def pooler(loop):
    pool = mysqlPool(loop=loop, params=params)
    await pool.connect()
    print(
        f"Connected: {pool.is_connected()}"
    )
    db = await pool.acquire()
    await pool.release(connection=db.get_connection())
    async with await pool.acquire() as conn:
        # execute a sentence
        result, error = await conn.test_connection()
        print(result, 'Error: ', error)
    print('Is closed: ', {db.is_connected()})
    await pool.close()

async def test_mysql(loop):
    db = AsyncDB("mysql", loop=loop, params=params)
    async with await db.connection() as conn:
        result = await conn.execute(
            """
            CREATE TABLE `color` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(32) DEFAULT NULL,
                `hex` VARCHAR(7) DEFAULT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
            """
        )
        print(result)
        data = [
            ("Red", "#ff0000"),
            ("Green", "#00ff00"),
            ("Blue", "#0000ff"),
            ("Cyan", "#00ffff"),
            ("Magenta", "#ff00ff"),
            ("Yellow", "#ffff00"),
            ("Black", "#000000"),
            ("White", "#ffffff"),
        ]
        sql = "INSERT INTO color (name, hex) VALUES (%s, %s)"
        result = await conn.executemany(sql, data)
        print('COLORS INSERTED ', result)
        sql = "select * from color"
        result, error = await conn.query(sql)
        for row in result:
            print(row)
        result, error = await conn.execute("TRUNCATE TABLE color")
        print("-- execute", result, 'errors: ', error)
        # at the end, drop the table:
        result = await conn.execute("DROP TABLE color;")


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(pooler(loop))
        loop.run_until_complete(test_mysql(loop))
    finally:
        loop.stop()
