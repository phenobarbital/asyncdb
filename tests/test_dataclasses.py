import pytest
import uuid
from datetime import datetime
from asyncdb import AsyncDB, AsyncPool
import asyncio
import asyncpg
from io import BytesIO
from pathlib import Path
import pytest_asyncio
from asyncdb.models import Model, Column
from asyncdb.models.sql import SQLModel

DRIVER = 'pg'
CREDENTIALS = {
    'user': 'troc_pgdata',
    'password': '12345678',
    'host': 'localhost',
    'port': '5432',
    'database': 'navigator_dev',
}
DSN = "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator_dev"


@pytest_asyncio.fixture
async def conn(event_loop):
    db = AsyncDB(DRIVER, params=params, loop=event_loop)
    await db.connection()
    yield db
    await db.close()


def auto_now_add(*args, **kwargs):
    return uuid.uuid4()


def now():
    return datetime.now()

class Contact(Model):
    account: str = ''
    value: str = ''

class User(SQLModel):
    """
    User Basic Structure
    """
    id: uuid.UUID = Column(required=True, primary_key=True, default=auto_now_add, db_default='uuid_generate_v4()')
    firstname: str
    lastname: str
    name: str = Column(required=True, default='John Doe')
    age: int = Column(default=18, required=True)
    signup_ts: datetime = Column(default=datetime.now(), db_default='now()')
    contacts: Contact = Column(required=False)

    def __post_init__(self):
        self.name = f"{self.firstname} {self.lastname}"
        super(User, self).__post_init__()
        print('Columns Are: ', self.columns())

    class Meta:
        name = 'users'
        schema = 'public'
        driver = 'pg'
        dsn = None
        credentials = None
        strict = False


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_dataclass_with_dsn(driver, event_loop):
    db = AsyncDB(driver, dsn=DSN, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    pytest.assume(db.is_connected() is True)
    User.Meta.dsn = DSN
    sql_model = User.model(dialect='sql')
    # create table
    result, error = await db.execute(sql_model)
    pytest.assume(not error)
    # let's create some samples:
    data = {
        "firstname": 'Jesus',
        "lastname": 'Lara',
        "age": 43,
        "contacts": [
            Contact(**{"account": "email", "value": "jlara@gmail.com"}),
            Contact(**{"account": "email", "value": "jlara@trocglobal.com"})
        ]
    }
    u = User(**data)
    print('CONN ', u.get_connection())
    jesus = await u.insert()
    pytest.assume(jesus.age == 43)
    # delete the row
    result = await jesus.delete()
    pytest.assume(result == 'DELETE 1')
    # drop table after test:
    drop_table = f'DROP TABLE {User.Meta.schema}.{User.Meta.name}'
    result, error = await db.execute(drop_table)
    pytest.assume(not error)
    pytest.assume(result == 'DROP TABLE')
    await db.close()
    assert db.is_closed() is True


@pytest.mark.parametrize("driver", [
    (DRIVER)
])
async def test_dataclass_with_credentials(driver, event_loop):
    db = AsyncDB(driver, dsn=DSN, loop=event_loop)
    assert db.is_connected() is False
    await db.connection()
    pytest.assume(db.is_connected() is True)
    User.Meta.credentials = CREDENTIALS
    sql_model = User.model(dialect='sql')
    # create table
    result, error = await db.execute(sql_model)
    pytest.assume(not error)
    # let's create some samples:
    users = [
        {"firstname": "Arnoldo", "lastname": "Lara Gimenez",
            "name": "Arnoldo Lara", "age": 52},
        {"firstname": "Yolanda", "lastname": "Lara Gimenez",
            "name": "Yolanda Lara", "age": 49},
        {"firstname": "Yolanda", "lastname": "Gimenez",
            "name": "Yolanda Gimenez", "age": 72}
    ]
    users = await User.create(users)
    for user in users:
        pytest.assume(type(user) == User)
        # change something:
        user.age = 45
        await user.save()
    # check if the data is currently good:
    result, error = await db.query(
        f"SELECT * FROM {User.Meta.schema}.{User.Meta.name}"
    )
    for u in result:
        pytest.assume(u['age'] == 45)
    # making more comparisons
    result, error = await db.queryrow(
        f"SELECT * FROM {User.Meta.schema}.{User.Meta.name} WHERE lastname = 'Gimenez'"
    )
    yolanda = await User.get(age=45, lastname='Gimenez')
    pytest.assume(result['firstname'] == yolanda.firstname)
    # get all users:
    users = await User.all()
    print('get all users: ')
    for u in users:
        pytest.assume(u.age == 45)
        # delete the row
        result = await u.delete()
        pytest.assume(result == 'DELETE 1')
    # drop table after test:
    drop_table = f'DROP TABLE {User.Meta.schema}.{User.Meta.name}'
    result, error = await db.execute(drop_table)
    pytest.assume(not error)
    pytest.assume(result == 'DROP TABLE')
    await db.close()
    assert db.is_closed() is True


def pytest_sessionfinish(session, exitstatus):
    asyncio.get_event_loop().close()
