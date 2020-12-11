import asyncio

from asyncdb import AsyncDB, AsyncPool, asyncORM
from asyncdb.providers.mysql import mysql, mysqlPool

"""
CREATE TABLE 'color' (
    'id' bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    'name' varchar(32) DEFAULT NULL,
    'hex' varchar(7) DEFAULT NULL,
    PRIMARY KEY ('id')
);
"""


def exception_handler(loop, context):
    """Exception Handler for Asyncio Loops."""
    if context:
        msg = context.get("exception", context["message"])
        print("Caught AsyncDB Exception: {}".format(str(msg)))
        # Canceling pending tasks and stopping the loop
        logging.info("Shutting down...")
        loop.run_until_complete(shutdown(loop))


loop = asyncio.get_event_loop()
loop.set_debug(True)
loop.set_exception_handler(exception_handler)

# create a pool with parameters
params = {
    "user": "root",
    "password": "123456",
    "host": "localhost",
    "port": "3306",
    "database": "test",
    "DEBUG": True,
}

# With mysqlPool
pool = mysqlPool(loop=loop, params=params)
loop.run_until_complete(pool.connect())
db = loop.run_until_complete(pool.acquire())
conn = db.get_connection()
loop.run_until_complete(pool.release())
pool.terminate()

# With AsyncDB
pool = AsyncDB("mysql", loop=loop, params=params)
conn = loop.run_until_complete(pool.connection())
cursor = loop.run_until_complete(pool.cursor())

result, error = loop.run_until_complete(pool.execute("TRUNCATE TABLE color"))
print("-- execute", result)

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
result = loop.run_until_complete(pool.executemany(sql, data))

sql = "select * from color"
result, error = loop.run_until_complete(pool.query(sql))
print("-- query", result, error)
result, error = loop.run_until_complete(pool.queryrow(sql))
print("-- queryrow", result, error)
table = pool.table("color")

# TODO: Implement the ORM
db = asyncORM(db=pool, loop=loop)
result = db.table(
    "color", loop=loop
).all()  # .filter(hex='#ffffff').fields(['name', 'hex']).one()
print("-- db.table", result)


async def select():
    conn = pool._connection
    cur = await conn.cursor()
    await cur.execute(sql)
    r = await cur.fetchone()
    print(r)

    async with pool._connection as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            r = await cur.fetchone()
            print(r)


loop.run_until_complete(select())
pool.terminate()
