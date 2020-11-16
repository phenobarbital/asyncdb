from datetime import datetime
from typing import Any, List, Optional, get_type_hints, Callable, ClassVar, Union
from asyncdb.utils.models import Model, Column
from dataclasses import dataclass, asdict, fields

#@dataclass
class User(Model):
    id: int
    name: str
    firstname: str
    lastname: str
    age: int
    class Meta:
        name = 'users'
        schema = 'public'
        app_label = 'troc'
        strict = False

#@dataclass
class Employee(User):
    associate_id: int
    email: str
    status: int = 0
    chief: User = None

u = {
    "id": 1,
    "name": 'Admin',
    "firstname": 'Super',
    "lastname": 'Sayayin',
    "age": 42
}
u = User(**u)
print(u)
u.name = 'Jesus Ignacio Lara Gimenez'
print(asdict(u))

employee = {
    "id": 2,
    "name": 'David Lara',
    "firstname": 'David',
    "lastname": 'Lara',
    "age": 46,
    "associate_id": 3,
    "email": 'jesuslara@gmail.com'
}
e = Employee(**employee)
e.chief = u
#print(e.__dataclass_fields__, fields(e))
print(asdict(e))

# class User(Model):
#     id: int = Column(required=True)
#     name: str = Column(required=True)
#     firstname: str
#     lastname: str
#     age: int = Column(default=42, required=True)
#     class Meta:
#         name = 'users'
#         schema = 'public'
#         app_label = 'troc'
#         strict = False
#
# u = User()
# u.id = 1
# u.name = 'Admin'
# u.firstname = 'Super'
# u.lastname = 'Sayayin'
# u.ultra = 'Ultra Sayayin'
#
# print(u.columns())
# print(u)
# cols = u.__slots__
# print(cols)
# print(u.schema(type='sql'))
#
# class PyUser(Model):
#     id: int = Column(default=1, required=True)
#     name: str = Column(default='John Doe', required=False)
#     signup_ts: datetime = Column(default=datetime.now(), required=False)
#
# u = PyUser()
# u.name = 'Jesus Lara'
# u.id = 2
# print(u)
#
#
# class Employee(User):
#     status: int = 0
#     associate_id: int = Column(required=True)
#     email: str = Column(required=False)
#     chief: User = Column(required=False)
#
# u = User(id=1, name='Jesus Lara')
# u.fuerza = True
# print(u)
# u.name = 'Jesus Ignacio Lara Gimenez'
# print(asdict(u))
#
# employee = {
#     "id": 1,
#     "name": 'Jesus Lara',
#     "associate_id": 3
# }
# e = Employee(**employee)
# e.email = 'jesuslara@gmail.com'
# e.chief = u
# print(e.__dataclass_fields__, fields(e))
# print(asdict(e))
#
#
# class PyUser(Model):
#     id: int
#     name: str = 'John Doe'
#     signup_ts: datetime = None
#
#     class Meta:
#         name = 'pyusers'
#         schema = 'public'
#         app_label = 'troc'
#         strict = False
#
#
# user = PyUser(id='42', signup_ts='2032-06-21T12:00')
# user.perolito = True
# print(user, user.perolito)
#
#
# class NavbarButton(Model):
#     href: str
#
# class Navbar(Model):
#     button: List[NavbarButton]
#
# navbar = Navbar(
#     button=[ NavbarButton(href='http://example.com'), NavbarButton(href='http://example2.com') ]
# )
# print(navbar)
#
# class Position(Model):
#     name: str
#     lon: float = Column(default=0.0, metadata={'unit': 'degrees'})
#     lat: float = Column(default=0.0, metadata={'unit': 'degrees'})
#     country: str
#
# pos = Position(name='Oslo', lon=10.8, lat=59.9)
# print(pos)
# print(f'{pos.name} is at {pos.lat}°N, {pos.lon}°E')
# pos.country = 'Norway'
# print(pos)
#
#
# person = {
#     'name': 'Ivan',
#     'age': 30,
#     'contact': [
#         {
#             'phone': '+7-999-000-00-00',
#             'email': 'ivan@mail.us',
#             'address': 'Miami, 33066',
#             'city': 'Miami',
#             'zipcode': '33066'
#         },
#         {
#             'phone': '+34-999-000-00-11',
#             'email': 'ivan@mail.us',
#             'address': 'Florida, 33166',
#             'city': 'Orlando',
#             'zipcode': '33166'
#         }
#     ]
# }
#
#
# class Contact(Model):
#     phone: str
#     email: str
#     address: str = Column(default='')
#     zipcode: str = Column(default='')
#     city: str = Column(required=False)
#
#
# class Person(Model):
#     name: str = Column(default='')
#     age: int = Column(default=18, min=0, max=99)
#     contact: List[Contact] = Column(required=False)
#
#
# ivan = Person(**person)
# print(ivan.json())
