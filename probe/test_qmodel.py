"""Models.

Models for querysource structure.
"""
import asyncio
from asyncdb.utils.models import Model, Column
from typing import Optional, List, Dict, Union, Tuple, Any, Callable
from dataclasses import InitVar
from datetime import datetime
from asyncdb.exceptions import default_exception_handler
from asyncdb import AsyncDB, AsyncPool

class QueryUtil(Model):
    query_slug: str = Column(required=True, primary_key=True)
    source: str = Column(required=False)
    params: Dict = Column(required=False, db_type='jsonb')
    where_cond: str = Column(required=False)
    conditions: Dict = Column(required=False, db_type='hstore')
    cond_definition: Dict = Column(required=False, db_type='hstore')
    fields: str = Column(required=False, db_type='array')
    ordering: str = Column(required=False, db_type='array')
    query_raw: str = Column(required=False)
    #grouping: str = Column(required=False, db_type='array')
    # created_at: datetime = Column(
    #     required=False,
    #     default=datetime.now(),
    #     db_default='now()'
    # )
    cache_timeout: int = Column(required=False,)
    cache_refresh: int = Column(required=False)
    program_id: int = Column(required=False)
    program_slug: str = Column(required=True)
    is_cached: bool = Column(required=False, default=True)
    row_based: bool = Column(required=False, default=False)
    provider: str = Column(required=False)
    raw_query: bool = Column(required=False, default=False)
    dwh: bool = Column(required=False, default=False)
    dwh_info: Dict = Column(required=False, db_type='jsonb')

    class Meta:
        driver = 'pg'
        name = 'query_util'
        schema = 'troc'
        app_label = 'troc'
        strict = True
        frozen = False


loop = asyncio.new_event_loop()
loop.set_exception_handler(default_exception_handler)

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

pool = AsyncPool('pg', loop=loop, params=params)
loop.run_until_complete(pool.connect())
db = loop.run_until_complete(pool.acquire())

print('Pool Connected: ', pool.is_connected())
db = loop.run_until_complete(pool.acquire())
print('Is Connected: ', db.is_connected())
loop.run_until_complete(db.test_connection())


try:
    print('CREATION of MODEL::')
    mdl = QueryUtil(**{"query_slug": "walmart_stores"})
    mdl.Meta.set_connection(db)
    result = loop.run_until_complete(mdl.fetch(raw_query=False))
    print('RESULT IS: ', result)

    mdl = QueryUtil(**{"program_slug": "walmart", "provider": "db"})
    mdl.Meta.set_connection(loop.run_until_complete(pool.acquire()))
    result = loop.run_until_complete(mdl.select())
    for row in result:
        print(row)
    #print(mdl.schema(type='SQL'))
    # for key in mdl.get_fields():
    #     field = mdl.column(key)
    #     #print(key, field)
    # # try to insert:
    # data = {
    #     "query_slug": "test_query",
    #     "cache_refresh": 3600,
    #     "cache_timeout": 3600,
    #     "params": {
    #         "driver": "pg",
    #         "user": "troc_pgdata",
    #         "password": "12345678",
    #         "host": "127.0.0.1",
    #         "port": "5432",
    #         "database": "navigator_dev",
    #         "DEBUG": True
    #     },
    #     "cond_definition": {
    #         "firstdate": "CURRENT_DATE",
    #         "lastdate": "CURRENT_DATE"
    #     },
    #     "ordering": ["test1", "test2", "test3"]
    # }
    # mdl = QueryUtil(**data)
    # print(mdl)
    # loop.run_until_complete(mdl.save())
finally:
    print("COMPLETED! ========")
    loop.run_until_complete(pool.release(db))
    loop.run_until_complete(pool.wait_close(gracefully=True, timeout=2))
    loop.stop()
