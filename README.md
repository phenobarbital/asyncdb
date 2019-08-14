# AsyncDB #

AsyncDB is a new library for database (and other datasource connections like RESTful services) connections used by T-ROC Navigator App.

### What is AsyncDB? ###

The goal of AsyncDB is to provide a subset of providers (connectors) to access different databases and data sources for data interaction.

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

* Python >= 3.7
* asyncio (https://pypi.python.org/pypi/asyncio/)

### Quick Tutorial ###

Currently AsyncDB supports the following databases:

* PostgreSQL (requires asyncpg)
* SQLite (requires aiosqlite)
* mySQL (requires aiomysql)
* SQLAlchemy (requires sqlalchemy_aio)
* RethinkDB (requires rethinkdb)
* Redis (requires aioredis)
* Memcache (requires aiomcache)

Future work:
* MS SQL Server (non-asyncio using freeTDS)
* Oracle
* Apache Cassandra
* CouchBase

### Contribution guidelines ###

Please have a look at the Contribution Guide

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact

### License ###

AsyncDB is dual-licensed under BSD and Apache 2.0 licenses.
