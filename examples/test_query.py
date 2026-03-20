"""Models.

Models for querysource structure.
"""
import asyncio
from typing import Optional
from datetime import datetime
from asyncdb import AsyncDB
from asyncdb.models import Model, Field

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
    "DEBUG": True,
}

def rigth_now(obj) -> datetime:
    return datetime.now()


class QueryModel(Model):
    query_slug: str = Field(required=True, primary_key=True)
    description: str = Field(required=False, default=None)
    # Source and primary attributes:
    source: Optional[str] = Field(required=False)
    params: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict)
    attributes: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict, comment="Optional Attributes for Query")
    #  main conditions
    conditions: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict)
    cond_definition: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict)
    ## filter and grouping options
    fields: Optional[list] = Field(required=False, db_type='array')
    filtering: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict)
    ordering: Optional[list] = Field(required=False, db_type='array')
    grouping: Optional[list] = Field(required=False, db_type='array')
    qry_options: Optional[dict] = Field(required=False, db_type='jsonb')
    h_filtering: bool = Field(required=False, default=False, comment="filtering based on Hierarchical rules.")
    ### Query Information:
    query_raw: str =  Field(required=False)
    is_raw: bool = Field(required=False, default=False)
    is_cached: bool = Field(required=False, default=True)
    provider: str = Field(required=False, default='db')
    parser: str = Field(required=False, default='SQLParser', comment="Parser to be used for parsing Query.")
    cache_timeout: int = Field(required=True, default=3600)
    cache_refresh: int = Field(required=True, default=0)
    cache_options: Optional[dict] = Field(required=False, db_type='jsonb', default_factory=dict)
    ## Program Information:
    program_id: int = Field(required=True, default=1)
    program_slug: str = Field(required=True, default='default')
    # DWH information
    dwh: bool = Field(required=True, default=False)
    dwh_driver: str = Field(required=False, default=None)
    dwh_info: Optional[dict] = Field(required=False, db_type='jsonb')
    dwh_scheduler: Optional[dict] = Field(required=False, db_type='jsonb')
    # Creation Information:
    created_at: datetime = Field(
        required=False,
        default=datetime.now(),
        db_default='now()'
    )
    created_by: int = Field(required=False) # TODO: validation for valid user
    updated_at: datetime = Field(
        required=False,
        default=datetime.now(),
        encoder=rigth_now
    )
    updated_by: int = Field(required=False) # TODO: validation for valid user

    class Meta:
        driver = 'pg'
        name = 'queries'
        schema = 'public'
        strict = True
        frozen = False
        remove_nulls = True # Auto-remove nullable (with null value) fields


async def test_query(evt):
    db = AsyncDB('pg', params=params, loop=evt)
    async with await db.connection() as conn:
        QueryModel.Meta.connection = conn
        slug = await QueryModel.get(query_slug='walmart_mtd_postpaid_to_goal')
        print(slug)
        print(slug.to_dict())


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        test_query(evt=loop)
    )
