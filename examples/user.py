import asyncio
from datetime import datetime
from typing import Union

from datamodel.exceptions import ValidationError

from asyncdb import AsyncDB
from asyncdb.models import Column, Model

params = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator_dev",
}

class User(Model):
    """Basic User notation."""

    user_id: int = Column(required=False, primary_key=True)
    first_name: str
    last_name: str
    title: str
    email: str = Column(required=False, max=254)
    password: str = Column(required=False, max=128)
    last_login: datetime = Column(required=False)
    username: str = Column(required=False)
    is_superuser: bool = Column(required=True, default=False)
    is_active: bool = Column(required=True, default=True)
    is_new: bool = Column(required=True, default=True)
    is_staff: bool = Column(required=True, default=False)
    title: str = Column(equired=False, max=90)
    registration_key: str = Column(equired=False, max=512)
    reset_pwd_key: str = Column(equired=False, max=512)
    date_joined: datetime = Column(required=False)
    birth_date: Union[datetime, str] = Column(required=False)
    avatar: str = Column(max=512)
    associate_id: str = Column(required=False)
    associateoid: str = Column(required=False)
    company: str
    department: str
    position_id: str
    group_id: list
    groups: list
    programs: list = Column(required=False)

    def __getitem__(self, item):
        return getattr(self, item)

    @property
    def display_name(self):
        return f"{self.first_name} {self.last_name}"

    class Meta:
        driver = "pg"
        name = 'vw_users'
        schema = "public"
        strict = True
        frozen = False
        connection = None

async def get_user(data: dict):
    driver = User.Meta.driver
    db = AsyncDB(driver, params=params)
    try:
        async with await db.connection() as conn:
            User.Meta.connection = conn
            user = await User.get(**data)
            print(user)
    except ValidationError as ex:
        print(ex)
        print(ex.payload)


if __name__ == '__main__':
    asyncio.run(get_user({"username": "Gcanelon@mobileinsight.com"}))
