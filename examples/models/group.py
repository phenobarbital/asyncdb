import asyncio
from datetime import datetime
from datamodel.exceptions import ValidationError
from asyncdb import AsyncDB
from asyncdb.models import Model, Column
from asyncdb.exceptions import DriverError

class Group(Model):
    group_id: int = Column(
        required=False, primary_key=True, db_default='auto', repr=False
    )
    group_name: str = Column(required=True)
    client_id: int = Column(
        required=False, fk="client_id|client", api="clients"
    )
    is_active: bool = Column(required=True, default=True)
    created_at: datetime = Column(
        required=False, default=datetime.now(), repr=False
    )
    updated_at: datetime = Column(
        required=False, default=datetime.now(), repr=False
    )
    created_by: str = Column(required=False, repr=False)

    class Meta:
        name = "groups"
        schema = "auth"
        description = 'Group Management'
        strict = True

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

async def group_management():
    p = AsyncDB("pg", params=params, **args)
    async with await p.connection() as conn:
        Group.Meta.connection = conn
        ### Get All Groups:
        try:
            groups = await Group.all()
            print('GROUPS:', len(groups))
        except ValidationError as ex:
            print(ex.payload)
        ### Get One Group:
        try:
            tpl = await Group.get(group_id=1)
            print(tpl)
        except ValidationError as ex:
            print(ex.payload)
        ### Get Multiple:
        kwargs = {"group_id": [1,2,3,4]}
        try:
            groups = await Group.filter(**kwargs)
            print('GROUPS:', len(groups))
        except ValidationError as ex:
            print(ex.payload)
        ### Filter by Columns:
        kwargs = {"group_name": "superuser"}
        try:
            groups = await Group.filter(**kwargs)
            print('GROUPS:', len(groups))
        except ValidationError as ex:
            print(ex.payload)
        ### Patch a Group:
        try:
            data = {
                "is_active": False,
            }
            result = await Group.get(group_id=13)
            for key, val in data.items():
                if key in result.get_fields():
                    result.set(key, val)
            result = await result.update()
            print('RESULT > ', result)
        except ValidationError as ex:
            print(ex.payload)
        ### Create a Group:
        data = {
            "group_name": "Sample Group",
            "client_id": None,
            "is_active": True
        }
        try:
            resultset = Group(**data)
            if not resultset.is_valid():
                print('ERROR >>')
            inserted = await resultset.insert()
            print('RESULT > ', inserted)
        except ValidationError as ex:
            print(ex.payload)
        except DriverError as ex:
            print(ex)
        inserted_id = inserted.group_id
        ### Edit Group:
        data = {
            "group_id": inserted_id,
            "group_name": "My Sample Group",
            "client_id": 1,
            "is_active": False
        }
        obj = await Group.get(group_id=inserted_id)
        print('OBJ ', obj)
        ## saved with new changes:
        for key, val in data.items():
            if key in obj.get_fields():
                obj.set(key, val)
        try:
            r = await obj.update()
            print('EDITED > ', r)
        except ValidationError as ex:
            print(ex.payload)
        except DriverError as ex:
            print(ex)
        ### Delete Group:
        try:
            obj = await Group.get(group_id=inserted_id)
            print('OBJ ', obj)
            r = await obj.delete()
            print('DELETED > ', r)
        except ValidationError as ex:
            print(ex.payload)
        except DriverError as ex:
            print(ex)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        group_management()
    )
