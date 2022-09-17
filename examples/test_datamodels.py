import uuid
import asyncio
from datetime import datetime
from datamodel import BaseModel, Column
from asyncdb.models import Model
from asyncdb import AsyncDB
from asyncdb.utils import Msg


loop = asyncio.get_event_loop()
asyncpg_url = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"

params = {
    "server_settings": {
        "application_name": "Testing"
    }
}
p = AsyncDB("pg", dsn=asyncpg_url, loop=loop, **params)
loop.run_until_complete(p.connection())

Msg('Working with Data Models: ')

def auto_now_add(*args, **kwargs):
    return uuid.uuid4()

class Contact(BaseModel):
    account: str = ''
    value: str = ''

class User(Model):
    """
    User Basic Structure
    """
    id: uuid.UUID = Column(primary_key=True, required=True, db_default='uuid_generate_v4()')
    firstname: str
    lastname: str
    name: str = Column(required=True, default='John Doe')
    age: int = Column(default=18, required=True)
    signup_ts: datetime = Column(default=datetime.now(), db_default='now()')
    contacts: Contact = Column(required=False)

    class Meta:
        name = 'users'
        schema = 'public'
        driver = 'pg'
        # credentials = {
        #     'user': 'troc_pgdata',
        #     'password': '12345678',
        #     'host': 'localhost',
        #     'port': '5432',
        #     'database': 'navigator_dev',
        # }
        dsn = asyncpg_url
        strict = False

u = User()
print(u)
print(User.model(dialect='sql'))

async def working_model():
    user = User()
    async with user as u:
        u.firstname = 'Jesus'
        u.lastname = 'Lara'
        u.name = 'Jesus Lara'
        u.age = 18
        await user.insert()
        u.age = 42
        await user.save()

loop.run_until_complete(working_model())

#TODO: definition of Operators
# from models.operators import or, not
# or(value) returns OR instead AND
# not if value is an IN, returns NOT IN
async def get_user(age):
    user = await User.get(age=age)
    user.name = 'Jesus Ignacio Jose Lara Gimenez'
    user.age+=1
    print('User is: ', user)
    await user.save()
    user.age = 42
    await user.save()

async def new_user():
    Msg('Inserting and deleting a user: ', 'DEBUG')
    data = {
        "firstname": 'Guillermo',
        "lastname": 'Lara',
        "name": 'Guillermo Daniel',
        "age": 18,
       "contacts": [Contact(**{"account": "email", "value": "jlara@gmail.com"}), Contact(**{"account": "email", "value": "jlara@trocglobal.com"})]
    }
    u = User(**data)
    await u.insert()
    print('USER JSON: ', u.json())
    # also, we can deleting as well
    await u.delete()

# async def get_all_users():
#     users = await User.all()
#     print('get all users: ')
#     for user in users:
#         print(user)

# async def get_users(**kwargs):
#     users = await User.filter(**kwargs)
#     print('get users: ')
#     for user in users:
#         user.age = 48
#         await user.save()
#         print(user)

# async def update_users(filter: list, **kwargs):
#     users = await User.update(filter, **kwargs)
#     print('Users updated:')
#     for user in users:
#         print(user)

async def create_users(users):
    users = await User.create(users)
    print('Users created:')
    for user in users:
        print(user)
# """
# async methods:
#   get
#   filter
#   -
#   fetch
#   fetchone
#   query
# """
users = [
    {"firstname":"Arnoldo","lastname":"Lara Gimenez","name":"Arnoldo Lara","age": 52},
    {"firstname":"Yolanda","lastname":"Lara Gimenez","name":"Yolanda Lara","age": 48},
    {"firstname":"Yolanda","lastname":"Gimenez","name":"Yolanda Gimenez","age": 72}
]
# # asyncio.run(create_users(users))
# # asyncio.run(new_user())
# # asyncio.run(get_user(age=48))
# # asyncio.run(get_all_users())
# # asyncio.run(get_users(age=48, firstname='Román'))
# # asyncio.run(update_users(filter={"age": 72, "lastname": 'Gimenez'}, firstname='Yolanda Ramona'))

loop.run_until_complete(create_users(users))
loop.run_until_complete(new_user())
loop.run_until_complete(get_user(age=48))
# loop.run_until_complete(get_all_users())
# loop.run_until_complete(get_users(age=48, firstname='Román'))
# loop.run_until_complete(update_users(filter={"age": 72, "lastname": 'Gimenez'}, firstname='Yolanda Ramona'))
