# AsyncDB #

AsyncDB is a collection of different Database Drivers using asyncio-based connections, binary-connectors (as asyncpg) but providing an abstraction layer to easily connect to different data sources, a high-level abstraction layer for various non-blocking database connectors,
on other blocking connectors (like MS SQL Server) we are using ThreadPoolExecutors to run in a non-blocking manner.

### Why AsyncDB? ###

The finality of AsyncDB is to provide us a subset of drivers (connectors) for accessing different databases and data sources for data interaction.
The main goal of AsyncDB is using asyncio-based technologies.

### Getting Started ###

## Requirements

Python 3.8+

## Installation

<div class="termy">

```console
$ pip install asyncdb
---> 100%
Successfully installed asyncdb
```

Can also install only drivers required like:
```console
$ pip install asyncdb[pg] # this install only asyncpg
```
Or install all supported drivers as:

```console
$ pip install asyncdb[all]
```

### Requirements ###

* Python >= 3.8
* asyncio (https://pypi.python.org/pypi/asyncio/)

Currently AsyncDB supports the following databases:

* PostgreSQL (supporting two different connectors: asyncpg or aiopg)
* SQLite (requires aiosqlite)
* mySQL/MariaDB (requires aiomysql and mysqlclient)
* ODBC (using aioodbc)
* JDBC(using JayDeBeApi and JPype)
* RethinkDB (requires rethinkdb)
* Redis (requires aioredis)
* Memcache (requires aiomcache)
* MS SQL Server (non-asyncio using freeTDS and pymssql)
* Apache Cassandra (requires official cassandra driver)
* InfluxDB (using influxdb)
* CouchBase (using aiocouch)
* MongoDB (using motor)
* SQLAlchemy (requires sqlalchemy async (+3.14))

### Quick Tutorial ###

```python
from asyncdb import AsyncDB

db = AsyncDB('pg', dsn='postgres://user:password@localhost:5432/database')

# Or you can also passing a dictionary with parameters like:
params = {
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5432",
    "database": "database",
    "DEBUG": True,
}
db = AsyncDB('pg', params=params)

async with await db.connection() as conn:
    result, error = await conn.query('SELECT * FROM test')
```
And that's it!, we are using the same methods on all drivers, maintaining a consistent interface between all of them, facilitating the re-use of the same code for different databases.

Every Driver has a simple name to call it:
* pg: AsyncPG (PostgreSQL)
* postgres: aiopg (PostgreSQL)
* mysql: aiomysql (mySQL)
* influx: influxdb (InfluxDB)
* redis: aioredis (Redis)
* mcache: aiomcache (Memcache)
* odbc: aiodbc (ODBC)

#### Future work: ####

* Prometheus

### Output Support ###

With Output Support results can be returned into a wide-range of variants:

```python
from datamodel import BaseModel

class Point(BaseModel):
    col1: list
    col2: list
    col3: list

db = AsyncDB('pg', dsn='postgres://user:password@localhost:5432/database')
async with await d.connection() as conn:
    # changing output format to Pandas:
    conn.output_format('pandas')  # change output format to pandas
    result, error = await conn.query('SELECT * FROM test')
    conn.output_format('csv')  # change output format to CSV
    result, _ = await conn.query('SELECT TEST')
    conn.output_format('dataclass', model=Point)  # change output format to Dataclass Model
    result, _ = await conn.query('SELECT * FROM test')
```

Currently AsyncDB supports the following Output Formats:

* CSV (comma-separated or parametrized)
* JSON (using orjson)
* iterable (returns a generator)
* Recordset (Internal meta-Object for list of Records)
* Pandas (a pandas Dataframe)
* Datatable (Dt Dataframe)
* Dataclass (exporting data to a dataclass with -optionally- passing Dataclass instance)
* PySpark Dataframe

And others to come:
* Apache Arrow (using pyarrow)
* Polars (Using Python polars)
* Dask Dataframe

### Contribution guidelines ###

Please have a look at the Contribution Guide

* Writing tests
* Code review

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact

### License ###

AsyncDB is copyright of Jesus Lara (https://phenobarbital.info) and is licensed under BSD. I am providing code in this repository under an open source licenses, remember, this is my personal repository; the license that you receive is from me and not from my employeer.
