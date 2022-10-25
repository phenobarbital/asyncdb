import asyncio
from uuid import UUID
from typing import Optional
from datetime import datetime
from slugify import slugify
import orjson
from datamodel.exceptions import ValidationError
from asyncdb import AsyncDB
from asyncdb.models import Model, Column

def at_now():
    return datetime.now()

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

# running new multi-threaded async SA (using aiopg)
args = {
    "server_settings": {
        "application_name": "Testing"
    }
}

class WidgetTemplate(Model):

    widget_id: int = Column(required=False, primary_key=True, db_default="auto")
    uid: UUID = Column(required=True, db_default='uuid_generate_v4()')
    widget_name: str = Column(required=False)
    title: str
    description: str
    url: str = Column(required=False)
    active: bool = Column(required=True, default=True)
    params: Optional[dict] = Column(required=False, db_type="jsonb")
    embed: str = Column(required=False)
    attributes: Optional[dict] = Column(required=False, db_type="jsonb")
    conditions: Optional[dict] = Column(required=False, db_type="jsonb")
    cond_definition: Optional[dict] = Column(required=False, db_type="jsonb")
    where_definition: Optional[dict] = Column(required=False, db_type="jsonb")
    format_definition: Optional[dict] = Column(required=False, db_type="jsonb")
    master_filtering: bool = Column(required=True, default=False)
    query_slug: Optional[dict] = Column(required=False, db_type="jsonb")
    widget_slug: str = Column(required=False)
    program_id: int = Column(required=False)
    widget_type_id: str = Column(required=True)
    widgetcat_id: int = Column(required=True)
    allow_filtering: Optional[dict] = Column(required=False, db_type="jsonb")
    filtering_show: Optional[dict] = Column(required=False, db_type="jsonb")
    inserted_at: datetime = Column(required=False, default=at_now)
    inserted_by: int = Column(required=False)
    updated_at: datetime = Column(required=False, default=at_now)
    updated_by: int = Column(required=False)

    def __post_init__(self) -> None:
        if not self.widget_slug:
            self.widget_slug = slugify(self.widget_name, separator='_')
        return super(WidgetTemplate, self).__post_init__()

    class Meta:
        driver = "pg"
        name = "template_widgets"
        schema = "troc"
        strict = True


async def work_with_widgets():
    p = AsyncDB("pg", params=params, **args)
    async with await p.connection() as conn:
        WidgetTemplate.Meta.connection = conn
        try:
            tpl = await WidgetTemplate.get(uid='b13b619a-847e-4734-a3d2-fa198f0531b7')
        except ValidationError as ex:
            print(ex.payload)
        print(tpl)
        # TO JSON:
        print('=== TO JSON ===')
        print(tpl.json(option=orjson.OPT_INDENT_2))
        data = {
            "widget_type_id": "api-card",
            "widget_name": "Nuevo widget Template",
            # "widget_slug": "iframe_new",
            "widgetcat_id": 3
        }
        try:
            tpl = WidgetTemplate(**data)
            print(tpl)
        except ValidationError as ex:
            print(ex.payload)
        print(':: CREATED NEW ::')
        result = await tpl.insert()
        print('=== RESULT == ')
        print(result)
        print('=== TO JSON ===')
        print(result.json(option=orjson.OPT_INDENT_2))

if __name__ == '__main__':
    loop.run_until_complete(work_with_widgets())
