import asyncio
from decimal import Decimal
import datetime
import numpy as np
from typing import Any, List, Optional, get_type_hints, Callable, ClassVar, Union
from asyncdb.models import Model, Column
from asyncdb import AsyncDB


loop = asyncio.get_event_loop()
asyncio.set_event_loop(loop)

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

f = [
    ('sara_order_no', str),
    ('dealer_name', str),
    ('dealer_code', str),
    ('retailer', str),
    ('store_no', int),
    ('account_number', np.int64),
    ('lead_rep_name', Decimal),
    ('activity_date', datetime.datetime)
]

async def create_model():
    args = {
        "server_settings": {
            "application_name": "Testing"
        }
    }
    p = AsyncDB("pg", params=params, **args)
    query = await Model.makeModel(name='query_util', schema='troc', db=p)
    q = await query.filter(query_slug='walmart_stores')
    print(q, q[0].query_raw)
    print(q[0].schema(type='sql'))
    # also, another type:
    act = await Model.makeModel(name='activity_data', schema='att', fields=f, db=p)
    m = act()
    print(m.schema(type='sql'))

if __name__ == '__main__':
    loop.run_until_complete(create_model())
