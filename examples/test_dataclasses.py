import uuid
import asyncio
from datetime import datetime
import pprint
from dataclasses import asdict, fields, InitVar
from typing import Any, List, Optional, Callable, ClassVar, Union
from asyncdb.models import Model, Column
from asyncdb.models.sql import SQLModel
from asyncdb.utils import Msg


loop = asyncio.get_event_loop()


Msg('==== FIRST: Pure-like Dataclasses ')


def now():
    return datetime.now()


class User(Model):
    id: int
    name: str
    firstname: str
    lastname: str
    age: Optional[int] = 43
    start_at: datetime = Column(factory=now())
    directory: InitVar = None

    class Meta:
        name = 'users'
        schema = 'public'
        app_label = 'troc'
        strict = True
        frozen = False

    def __model_init__(cls, name, attrs) -> None:
        # can you define values before declaring a dataclass
        # (mostly pre-initialization)
        cls.name = 'Jesus Lara'

    def __post_init__(self, directory):
        super(User, self).__post_init__()
        print('Columns Are: ', self.columns())


u = {
    "id": 1,
    "firstname": 'Super',
    "lastname": 'Sayayin',
    # "age": 42
}
u = User(directory='hola', **u)
print('First version of User: ', u)
u.name = 'Jesus Ignacio Lara Gimenez'
print('Compatible with asdict class of dataclass: ', asdict(u))
print('Is a valid DataClass?: ', u.is_valid())

# Support inheritance


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
pprint.pprint(
    e.json(ensure_ascii=True, indent=4)
)


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

# compatible with "fields" method of dataclasses
print(fields(u))
print(u.json())

employee = {
    "id": 1,
    "name": 'Jesus Lara',
    "associate_id": 3
}
e = Employee(**employee)
e.email = 'jesuslara@gmail.com'
e.chief = u

# Comparing Fields versus fields method of dataclasses
print(e.__dataclass_fields__, fields(e))
print(e.dict())

Msg('Working with complex types, as uuid or datetime: ')


def get_id():
    return uuid.uuid4()


class PyUser(Model):
    id: int = Column(default=1, required=True)
    name: str = Column(default='John Doe', required=False)
    signup_ts: datetime = Column(default=now, required=False)
    guid: uuid.UUID = Column(default=get_id, required=False)

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

b = PyUser()
b.name = 'David Lara'
b.id = 3
print(b)
print(b.json())

user = PyUser(id='42', signup_ts='2032-06-21T12:00')
user.perolito = True
print(user, user.perolito)

Msg('=== Exporting Model Schema: ')
print(PyUser.model(dialect='json'))

Msg('=== Third: Working with nested Dataclasses: ')


class NavbarButton(Model):
    href: str


class Navbar(Model):
    button: List[NavbarButton]


navbar = Navbar(
    button=[NavbarButton(href='http://example.com'),
            NavbarButton(href='http://example2.com')]
)
print(navbar)
# nabar.button[0].href
print(navbar.dict())


Msg('=== Working with Metadata: ')


class Position(Model):
    name: str
    lon: float = Column(default=0.0, metadata={'unit': 'degrees'})
    lat: float = Column(default=0.0, metadata={'unit': 'degrees'})
    country: str
    continent: str = 'Europe'


pos = Position(
    name='Oslo',
    lon=10.8,
    lat=59.9
)
print(pos)
pos.country = 'Norway'
print(f'{pos.name} is in {pos.country} at {pos.lat}°N, {pos.lon}°E')
print(pos)


Msg('=== More Complex Methods, nested DataClasses: ')

person = {
    'name': 'Ivan',
    'age': 30,
    'contacts': [
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
    contacts: List[Contact] = Column(required=False)


ivan = Person(**person)
print(ivan)
print(ivan.json())
print('DATA TYPES> ', ivan.contacts[0], type(ivan.contacts[0]))


Msg('=== Using Methods within Dataclass: ')


class InventoryItem(Model):
    """Class for keeping track of an item in inventory."""
    name: str
    unit_price: float
    quantity: int = 0
    discount: Optional[int] = 0

    def total_cost(self) -> float:
        return self.unit_price * self.quantity

    @property
    def with_discount(self) -> float:
        return self.unit_price - float(self.discount)


grapes = InventoryItem(
    name='Grapes',
    unit_price=2.55
)
grapes.quantity = 8
print(f'Total for {grapes.name} is: ', grapes.total_cost())
grapes.discount = 0.18
print(f'Price of {grapes.name} with discount: {grapes.with_discount}')

Msg('Complex Model of Nested and Union Classes: ')
Msg('=== Nested DataClasses: ')


class Foo(Model):
    value: Union[int, List[int]]


class Bar(Model):
    foo: Union[Foo, List[Foo]]


foo = Foo(value=[1, 2])
instance = Bar(foo=foo)
print(instance)
assert instance.is_valid() or 'Not Valid'
assert instance == Bar(foo=Foo(value=[1, 2]))


Msg('Working with Data Models: ')


def auto_now_add(*args, **kwargs):
    return uuid.uuid4()


class User(SQLModel):
    """
    User Basic Structure
    """
    id: uuid.UUID = Column(
        required=True,
        primary_key=True,
        default=auto_now_add,
        db_default='uuid_generate_v4()'
    )
    firstname: str
    lastname: str
    name: str = Column(required=True, default='John Doe')
    age: int = Column(default=18, required=True)
    signup_ts: datetime = Column(default=now, db_default='now()')

    def __post_init__(self):
        self.name = f"{self.firstname} {self.lastname}"
        super(User, self).__post_init__()
        print('Columns Are: ', self.columns())

    class Meta:
        name = 'users'
        schema = 'public'
        driver = 'pg'
        credentials = {
            'user': 'troc_pgdata',
            'password': '12345678',
            'host': 'localhost',
            'port': '5432',
            'database': 'navigator_dev',
        }
        strict = False


print('Model: ', User)
Msg('Printing Model DDL: ')
print(User.model(dialect='sql'))
data = {
    "firstname": 'Super',
    "lastname": 'Sayayin',
    "age": 9000
}
u = User(**data)

#TODO: definition of Operators
# from models.operators import or, not
# or(value) returns OR instead AND
# not if value is an IN, returns NOT IN


async def get_user(age):
    user = await User.get(age=age)
    user.name = 'Jesus Ignacio Jose Lara Gimenez'
    user.age += 1
    print('User is: ', user)
    await user.save()
    user.age = 42
    await user.save()


async def new_user():
    Msg('Inserting and deleting a user: ', 'DEBUG')
    data = {
        "firstname": 'Román',
        "lastname": 'Lara',
        "name": 'Rafael David Lara'
    }
    u = User(**data)
    await u.insert()
    print(u.json())
    # also, we can deleting as well
    await u.delete()


async def get_all_users():
    users = await User.all()
    print('get all users: ')
    for user in users:
        print(user)


async def get_users(**kwargs):
    users = await User.filter(**kwargs)
    print('get some users: ')
    for user in users:
        user.age = 48
        await user.save()
        print(user)


async def update_users(filter: list, **kwargs):
    users = await User.update(filter, **kwargs)
    print('Users updated:')
    for user in users:
        print(user)


async def create_users(users):
    users = await User.create(users)
    print('Users created:')
    for user in users:
        print(user)
"""
async methods:
  get
  filter
  -
  fetch
  fetchone
  query
"""
users = [
    {"firstname": "Arnoldo", "lastname": "Lara Gimenez",
        "name": "Arnoldo Lara", "age": 52},
    {"firstname": "Yolanda", "lastname": "Lara Gimenez",
        "name": "Yolanda Lara", "age": 49},
    {"firstname": "Yolanda", "lastname": "Gimenez",
        "name": "Yolanda Gimenez", "age": 72}
]
asyncio.run(create_users(users))
asyncio.run(new_user())
asyncio.run(get_user(age=72))
asyncio.run(get_all_users())
asyncio.run(get_users(age=52, firstname='Arnoldo'))
asyncio.run(update_users(
    filter={"age": 42, "lastname": 'Gimenez'}, firstname='Yolanda')
)
