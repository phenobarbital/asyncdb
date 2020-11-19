from datetime import datetime
import dataclasses
from dataclasses import dataclass, asdict, fields, InitVar
from typing import Any, List, Optional, get_type_hints, Callable, ClassVar, Union
from asyncdb.utils.models import Model, Column
from asyncdb.utils import Msg
import uuid
import asyncio
import pprint

loop = asyncio.get_event_loop()


Msg('First: Pure-like Dataclasses ')
#@dataclass
class User(Model):
    id: int
    name: str = Column(required=True)
    firstname: str
    lastname: str
    age: int
    directory: InitVar = ''
    class Meta:
        name = 'users'
        schema = 'public'
        app_label = 'troc'
        strict = True
        frozen = False

    def __model_init__(cls, name, attrs) -> None:
        # can you define values before declaring a dataclass (mostly pre-initialization)
        cls.name = 'Jesus Lara'

    def __post_init__(self, directory):
        super(User, self).__post_init__()

u = {
    "id": 1,
    "firstname": 'Super',
    "lastname": 'Sayayin',
    "age": 42
}
u = User(directory='hola', **u)
print('First version of User: ', u)
u.name = 'Jesus Ignacio Lara Gimenez'
print('Compatible with asdict class of dataclass: ', asdict(u))

#@dataclass
class Employee(User):
    associate_id: int
    email: str
    status: int = 0
    chief: User = None

employee = {
    "id": 2,
    "name": 'Rafael David Lara',
    "firstname": 'David',
    "lastname": 'Lara',
    "age": 46,
    "associate_id": 3,
    "email": 'jesuslara@gmail.com'
}
e = Employee(**employee)
e.chief = u
pprint.pprint(e.json(ensure_ascii=True, indent=4))


Msg('==== SECOND METHOD: AsyncDB Model')

class User(Model):
    id: int = Column(required=True)
    name: str = Column(required=True)
    firstname: str
    lastname: str
    age: int = Column(default=42, required=True)
    class Meta:
        name = 'users'
        schema = 'public'
        app_label = 'troc'
        strict = False

class Employee(User):
    status: int = 0
    associate_id: int = Column(required=True)
    email: str = Column(required=False)
    chief: User = Column(required=False)

u = User()
u.id = 1
u.name = 'Admin'
u.firstname = 'Super'
u.lastname = 'Sayayin'
u.ultra = 'Ultra Sayayin'
print(fields(u))
print(u.json())

Msg('Exporting Model Schema: ')
print(u.schema(type='sql'))
print(u.schema(type='json'))

employee = {
    "id": 1,
    "name": 'Jesus Lara',
    "associate_id": 3
}
e = Employee(**employee)
e.email = 'jesuslara@gmail.com'
e.chief = u
print(e.__dataclass_fields__, fields(e))
print(e.dict())


Msg('Working with complex types, as uuid or datetime: ')
class PyUser(Model):
    id: int = Column(default=1, required=True)
    name: str = Column(default='John Doe', required=False)
    signup_ts: datetime = Column(default=datetime.now(), required=False)
    guid: uuid.UUID = Column(default=uuid.uuid4(), required=False)

    class Meta:
        name = 'pyusers'
        schema = 'public'
        app_label = 'troc'
        strict = False

a = PyUser()
a.name = 'Jesus Lara'
a.id = 2
print(a)
print(a.json())

user = PyUser(id='42', signup_ts='2032-06-21T12:00')
user.perolito = True
print(user, user.perolito)


Msg('First version of nested Dataclasses: ')

class NavbarButton(Model):
    href: str

class Navbar(Model):
    button: List[NavbarButton]

navbar = Navbar(
    button=[ NavbarButton(href='http://example.com'), NavbarButton(href='http://example2.com') ]
)
print(navbar)


Msg('Working with Metadata: ')
class Position(Model):
    name: str
    lon: float = Column(default=0.0, metadata={'unit': 'degrees'})
    lat: float = Column(default=0.0, metadata={'unit': 'degrees'})
    country: str

pos = Position(name='Oslo', lon=10.8, lat=59.9)
print(pos)
print(f'{pos.name} is at {pos.lat}°N, {pos.lon}°E')
pos.country = 'Norway'
print(pos)


Msg('Complex Methods, nested DataClasses: ')

person = {
    'name': 'Ivan',
    'age': 30,
    'contact': [
        {
            'phone': '+7-999-000-00-00',
            'email': 'ivan@mail.us',
            'address': 'Miami, 33066',
            'city': 'Miami',
            'zipcode': '33066'
        },
        {
            'phone': '+34-999-000-00-11',
            'email': 'ivan@mail.us',
            'address': 'Florida, 33166',
            'city': 'Orlando',
            'zipcode': '33166'
        }
    ]
}


class Contact(Model):
    phone: str
    email: str
    address: str = Column(default='')
    zipcode: str = Column(default='')
    city: str = Column(required=False)


class Person(Model):
    name: str = Column(default='')
    age: int = Column(default=18, min=0, max=99)
    contact: List[Contact] = Column(required=False)


ivan = Person(**person)
print(ivan.json())


Msg('Using Methods within Dataclass: ')
class InventoryItem(Model):
    """Class for keeping track of an item in inventory."""
    name: str
    unit_price: float
    quantity: int = 0

    def total_cost(self) -> float:
        return self.unit_price * self.quantity


grapes = InventoryItem(name='Grapes', unit_price=2.55)
grapes.quantity = 8
print(f'Total for {grapes.name} is: ', grapes.total_cost())

Msg('Complex Model of Nested and Union Classes: ')


class Foo(Model):
    value: Union[int, List[int]]


class Bar(Model):
    foo: Union[Foo, List[Foo]]


foo = Foo(value=[1, 2])
instance = Bar(foo=foo)
print(instance)
assert instance.is_valid() or 'Not Valid'
assert instance == Bar(foo=Foo(value=[1, 2]))
#
# Msg('Working with Data Models: ')
#
# def auto_now_add(*args, **kwargs):
#     return uuid.uuid4()
#
# class User(Model):
#     """
#     User Basic Structure
#     """
#     id: uuid.UUID = Column(required=True, primary_key=True, default=auto_now_add(), db_default='uuid_generate_v4()')
#     firstname: str
#     lastname: str
#     name: str = Column(required=True, default='John Doe')
#     age: int = Column(default=18, required=True)
#     signup_ts: datetime = Column(default=datetime.now(), db_default='now()')
#     class Meta:
#         name = 'users'
#         schema = 'public'
#         driver = 'pg'
#         credentials = {
#             'user': 'troc_pgdata',
#             'password': '12345678',
#             'host': 'localhost',
#             'port': '5432',
#             'database': 'navigator_dev',
#         }
#         strict = False
#
# u = User()
# print(u.schema(type='sql'))
#
# #TODO: definition of Operators
# # from models.operators import or, not
# # or(value) returns OR instead AND
# # not if value is an IN, returns NOT IN
# async def get_user(age):
#     user = await User.get(age=age)
#     user.name = 'Jesus Ignacio Jose Lara Gimenez'
#     user.age+=1
#     print('User is: ', user)
#     await user.save()
#     user.age = 42
#     await user.save()
#
# async def new_user():
#     Msg('Inserting and deleting a user: ', 'DEBUG')
#     data = {
#         "firstname": 'Román',
#         "lastname": 'Lara',
#         "name": 'Rafael David Lara'
#     }
#     u = User(**data)
#     await u.insert()
#     print(u.json())
#     # also, we can deleting as well
#     await u.delete()
#
# async def get_all_users():
#     users = await User.all()
#     print('get all users: ')
#     for user in users:
#         print(user)
#
# async def get_users(**kwargs):
#     users = await User.filter(**kwargs)
#     print('get users: ')
#     for user in users:
#         user.age = 48
#         await user.save()
#         print(user)
#
# async def update_users(filter: list, **kwargs):
#     users = await User.update(filter, **kwargs)
#     print('Users updated:')
#     for user in users:
#         print(user)
#
# async def create_users(users):
#     users = await User.create(users)
#     print('Users created:')
#     for user in users:
#         print(user)
# """
# async methods:
#   get
#   filter
#   -
#   fetch
#   fetchone
#   query
# """
# # asyncio.run(new_user())
# # asyncio.run(get_user(age=42))
# # asyncio.run(get_all_users())
# # asyncio.run(get_users(age=48, firstname='Román'))
# # asyncio.run(update_users(filter={"age": 42, "lastname": 'Lara'}, firstname='Jesús'))
# users = [
#     {"firstname":"Arnoldo","lastname":"Lara Gimenez","name":"Arnoldo Lara","age": 52},
#     {"firstname":"Yolanda","lastname":"Lara Gimenez","name":"Yolanda Lara","age": 49},
#     {"firstname":"Yolanda","lastname":"Gimenez","name":"Yolanda Gimenez","age": 72}
# ]
# asyncio.run(create_users(users))
