import asyncio
from asyncdb import AsyncDB
from asyncdb.models import Model, Column
from uuid import UUID
from typing import Dict
from datetime import datetime
from slugify import slugify

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

    widget_id: int = Column(required=False, primary_key=True)
    uid: UUID = Column(required=True, db_default='uuid_generate_v4()')
    widget_name: str = Column(required=True)
    title: str
    description: str
    url: str = Column(required=False)
    active: bool = Column(required=True, default=True)
    params: Dict = Column(required=False, db_type="jsonb")
    embed: str = Column(required=False)
    attributes: Dict = Column(required=False, db_type="jsonb")
    conditions: Dict = Column(required=False, db_type="jsonb")
    cond_definition: Dict = Column(required=False, db_type="jsonb")
    where_definition: Dict = Column(required=False, db_type="jsonb")
    format_definition: Dict = Column(required=False, db_type="jsonb")
    query_slug: Dict = Column(required=False, db_type="jsonb")
    widget_slug: str = Column(required=True)
    program_id: int = Column(required=False)
    widget_type_id: str = Column(required=True)
    widgetcat_id: int = Column(required=True)
    allow_filtering: Dict = Column(required=False, db_type="jsonb")
    filtering_show: Dict = Column(required=False, db_type="jsonb")
    inserted_at: datetime = Column(required=False, default=at_now)

    def __post_init__(self) -> None:
        if not self.widget_slug:
            self.widget_slug = slugify(self.widget_name, separator='_')
        return super(WidgetTemplate, self).__post_init__()


    def __getitem__(self, item):
        return getattr(self, item)

    class Meta:
        driver = "pg"
        name = "template_widgets"
        schema = "troc"


async def create_widget():
    p = AsyncDB("pg", params=params, **args)
    async with await p.connection() as conn:
        data = {
            "widget_type_id": "api-card",
            "widget_name": "Nuevo widget Template",
            # "widget_slug": "iframe_new",
            "widgetcat_id": 3
        }
        tpl = WidgetTemplate(**data)
        tpl.set_connection(conn)
        print(tpl)
        await tpl.insert()

if __name__ == '__main__':
    loop.run_until_complete(create_widget())
