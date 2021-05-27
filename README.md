# AsyncDB #

AsyncDB is a collection of different Database Drivers using asyncio-based connections, binary-connectors (as asyncpg) but providing an abstraction layer to easily connect to different data sources.

### Why AsyncDB? ###

The finality of AsyncDB is to provide us a subset of drivers (connectors) for accessing different databases and data sources for data interaction.
The main goal of AsyncDB is using asyncio-based technologies.

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### Getting Started ###

* Installation
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Requirements ###

* Python >= 3.8
* asyncio (https://pypi.python.org/pypi/asyncio/)

### Quick Tutorial ###

Currently AsyncDB supports the following databases:

* PostgreSQL (requires asyncpg and aiopg)
* SQLite (requires aiosqlite)
* mySQL (requires aiomysql)
* SQLAlchemy (requires sqlalchemy_aio)
* RethinkDB (requires rethinkdb)
* Redis (requires aioredis)
* Memcache (requires aiomcache)
* MS SQL Server (non-asyncio using freeTDS and pymssql)
* Apache Cassandra
* CouchBase (WIP: using aiocouch)
* MongoDB (WIP: using motor)
* InfluxDB (WIP: using influxdb)

#### Future work: ####

* Prometheus

### Contribution guidelines ###

Please have a look at the Contribution Guide

* Writing tests
* Code review

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact

### License ###

AsyncDB is copyright of Jesus Lara (https://phenobarbital.info) and is dual-licensed under BSD and Apache 2.0 licenses. I am providing code in this repository under an open source licenses, remember, this is my personal repository; the license that you receive is from me and not from my employeer.
