from typing import Optional
from uuid import UUID
import asyncio
from asyncdb.models import Column, Model
from asyncdb import AsyncDB

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator",
    "connection_config": {
        "work_mem": "1024MB",
        # "shared_buffers": "4096MB",
        "effective_io_concurrency": "100",
        # "max_worker_processes": "12",
        "max_parallel_workers_per_gather": "6",
    }
    # "ssl": True,
    # "check_hostname": True
}

class Dashboard(Model):

    dashboard_id: UUID = Column(
        required=False, primary_key=True, db_default="auto", repr=False
    )
    name: str = Column(required=True)
    description: str
    params: Optional[dict] = Column(required=False, db_type="jsonb")
    enabled: bool = Column(required=True, default=False)
    shared: bool = Column(required=True, default=False)
    published: bool = Column(required=True, default=False)
    allow_filtering: bool = Column(required=True, default=False)
    allow_widgets: bool = Column(required=True, default=False)
    attributes: Optional[dict] = Column(required=False, db_type="jsonb")
    dashboard_type: str
    slug: str
    position: int = Column(required=True, default=1)
    cond_definition: Optional[dict] = Column(required=False, db_type="jsonb")
    widget_location: Optional[dict] = Column(required=False, db_type="jsonb")
    module_id: int = Column(required=False)
    program_id: int = Column(required=False)
    user_id: int = Column(required=False)
    render_partials: bool = Column(required=True, default=False)
    conditions: Optional[dict] = Column(required=False, db_type="jsonb")
    save_filtering: bool = Column(required=True, default=False)
    filtering_show: Optional[dict] = Column(required=False, db_type="jsonb")
    is_system: bool = Column(required=True, default=False)
    created_by: int = Column(required=False)

    def __getitem__(self, item):
        return getattr(self, item)

    class Meta:
        driver = "pg"
        name = "dashboards"
        schema = "navigator"
        strict = True



async def test_dashboard(db):
    async with await db.connection() as conn:
        args = {
            "program_id": 45,
            "module_id": 307,
            "explorer": False
        }
        Dashboard.Meta.connection = conn
        dashboards = await Dashboard.filter(**args)
        print(dashboards)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    db = AsyncDB("pg", params=params, loop=loop)
    loop.run_until_complete(test_dashboard(db))
    loop.close()
