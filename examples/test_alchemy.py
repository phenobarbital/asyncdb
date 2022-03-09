import asyncio
import logging
from asyncdb.providers.sql_alchemy import sql_alchemy


def exception_handler(loop, context):
    """Exception Handler for Asyncio Loops."""
    if context:
        msg = context.get("exception", context["message"])
        print("Caught AsyncDB Exception: {}".format(str(msg)))
        # Canceling pending tasks and stopping the loop
        logging.info("Shutting down...")


loop = asyncio.new_event_loop()
loop.set_debug(True)
loop.set_exception_handler(exception_handler)

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
    result = conn.test_connection() # returns SQL Alchemy Row objects
    print(result)
    conn.row_format('dict')  # returns a dictionary
    result = conn.test_connection()
    print(result)
    conn.row_format('record')  
    result = conn.test_connection()
    print(result)
    table, error = conn.query("SELECT * FROM walmart.stores")
    for row in table:
        print(row)
    store, error = conn.queryrow("SELECT * FROM walmart.stores")
    print(store)

with sa.transaction() as trans:
    r = trans.execute("UPDATE xfinity.stores SET store_designation = 'LTM'")

    columns = conn.column_info(
        tablename='stores',
        schema='walmart'    
    )
    print(columns)

with sa.connection() as conn:
    conn.execute('DROP TABLE airports')
    table = """
        CREATE TABLE IF NOT EXISTS airports (
        iata text PRIMARY KEY,
        city text,
        country text
        )
    """
    result, error = conn.execute(table)
    print(result, error)
    data = [
        ("ORD", "Chicago", "United States"),
        ("JFK", "New York City", "United States"),
        ("CDG", "Paris", "France"),
        ("LHR", "London", "United Kingdom"),
        ("DME", "Moscow", "Russia"),
        ("SVO", "Moscow", "Russia"),
    ]
    airports = "INSERT INTO airports VALUES(%s, %s, %s)"
    result, error = conn.execute_many(airports, data)
    print(result, error)
    a_country = "France"
    a_city = "Paris"
    query = "SELECT * FROM airports WHERE country=%s AND city=%s"
    with conn.cursor(query, (a_country, a_city)) as result:
        for row in result:
            print(row)
    a_country = "United States"
    query = "SELECT * FROM airports WHERE country=%s"
    with conn.cursor(query, (a_country,)) as result:
        for row in result:
            print(row)

sa.close()
