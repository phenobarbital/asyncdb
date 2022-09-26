"""Oracle Driver.
"""
import os
import asyncio
from typing import (
    Union,
    Any
)
from collections.abc import Iterable
import time
from datetime import datetime
import oracledb
from asyncdb.utils.encoders import (
    DefaultEncoder
)
from asyncdb.exceptions import (
    DriverError,
    ProviderError,
    NoDataFound
)
from .sql import SQLDriver


class oracle(SQLDriver):
    _provider = "oracle"
    _syntax = "sql"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._test_query = "SELECT 1"
        _starttime = datetime.now()
        self._dsn = '{host}:{port}/{database}'
        self.application_name = os.getenv('APP_NAME', "ASYNCDB")
        try:
            self._lib_dir = params['oracle_client']
        except (KeyError, TypeError):
            try:
                self._lib_dir = kwargs['oracle_client']
            except KeyError:
                self._lib_dir = None
        try:
            super(oracle, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
            _generated = datetime.now() - _starttime
            print(f"Oracle Started in: {_generated}")
        except Exception as err:
            raise DriverError(
                f"Oracle Error: {err}"
            ) from err
        # set the JSON encoder:
        self._encoder = DefaultEncoder()

    async def prepare(self, sentence: Union[str, list]) -> Any:
        raise NotImplementedError

    async def connection(self):
        print(
            f'{self._provider}: Connected at {self._dsn}'
        )
        user = self._params['user']
        password = self._params ['password']
        if self._lib_dir is not None:
            oracledb.init_oracle_client(
                lib_dir=self._lib_dir,
                driver_name=f"{self.application_name} : 1.0"
            )
        self._executor = self.get_executor(executor='thread', max_workers=10)
        try:
            self._connection = await self._thread_func(
                oracledb.connect,
                dsn=self._dsn,
                user=user,
                password=password,
                executor=self._executor
            )
            print('Connection: ', self._connection)
            self._connected = True
            self._initialized_on = time.time()
            if self._init_func is not None and callable(self._init_func):
                await self._init_func(self._connection) # pylint: disable=E1102
            return self
        except Exception as ex:
            self._logger.exception(ex, stack_info=True)
            raise ProviderError(
                f"Oracle Unknown Error: {ex!s}"
            ) from ex

    async def close(self, timeout: int = 10) -> None:
        try:
            if self._connection:
                close = self._thread_func(self._connection.close)
                await asyncio.wait_for(close, timeout)
                print(
                    f'{self._provider}: Closed connection to {self._dsn}'
                )
            self._connected = False
            self._connection = None
        except Exception as e:
            print(e)
            self._logger.exception(e, stack_info=True)
            raise ProviderError(
                f"Oracle Closing Error: {e!s}"
            ) from e

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

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError
