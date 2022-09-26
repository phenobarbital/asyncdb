"""Dummy Driver.
"""
from datetime import datetime
from asyncdb.exceptions import DriverError
from .abstract import BaseDriver


class dummy(BaseDriver):
    _provider = "dummy"
    _syntax = "sql"

    def __init__(self, dsn="", loop=None, params: dict = None, **kwargs):
        self._test_query = "SELECT 1"
        _starttime = datetime.now()
        self._dsn = 'test:/{host}:{port}/{db}'
        if not params:
            params = {
                "host": "127.0.0.1",
                "port": "0",
                "db": 0
            }
        try:
            super(dummy, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
            self._logger.debug(
                f"Dummy Params are: {params}"
            )
            _generated = datetime.now() - _starttime
            print(f"Started in: {_generated}")
        except Exception as err:
            raise DriverError(
                f"Dummy Error: {err}"
            ) from err

    async def prepare(self):
        pass

    async def connection(self):
        print(
            f'{self._provider}: Connected at {self._params["host"]}'
        )
        self._connected = True
        return self

    async def close(self):
        print("Connection Closed")
        self._connected = False

    async def get_columns(self):
        return {"id": "value"}

    async def use(self, database):
        print(f'Changing Database to {database}')

    async def query(self, sentence="", **kwargs):
        error = None
        print(f"Running Query: {sentence}")
        result = [{'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]}]
        return await self._serializer(result, error)

    fetch_all = query

    async def execute(self, sentence: str, *args, **kwargs):
        print(f"Execute Query {sentence}")
        data = []
        error = None
        result = [data, error]
        return await self._serializer(result, error)

    execute_many = execute

    async def queryrow(self, sentence=""):
        error = None
        print(f"Running Row {sentence}")
        result = {'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]}
        return await self._serializer(result, error)

    fetch_one = queryrow
