import asyncio

from asyncdb import AsyncDB, AsyncPool
from asyncdb.providers.sql_alchemy import sql_alchemy


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
# create a pool with parameters
params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

sa = sql_alchemy(params=params)
with sa.connection() as conn:
    result = conn.test_connection()
    table, error = conn.query("SELECT * FROM walmart.stores")
    for row in table:
        print(row)
    store, error = conn.queryrow("SELECT * FROM walmart.stores")
    print(store)

with sa.transaction() as trans:
    r = trans.execute("UPDATE xfinity.stores SET store_designation = 'LTM'")

sa.close()
