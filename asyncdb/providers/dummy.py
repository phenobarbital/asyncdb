import asyncio
import logging
import os
import sys
import time
from .base import BaseProvider


class dummy(BaseProvider):
    _provider = "dummy"
    _syntax = "sql"

    def __init__(self, params={}, **kwargs):
        self._test_query = "SELECT 1"
        self._dsn = 'test:/{host}:{port}/{db}'
        if not params:
            params = {
                "host": "127.0.0.1",
                "port": "0",
                "db": 0
            }
        try:
            super(dummy, self).__init__(params=params, **kwargs)
            self._logger.debug(" My params are: {}".format(params))
        except Exception as err:
            raise ProviderError(message=str(err), errcode=500)

    async def prepare(self):
        pass

    async def connection(self):
        print(
            '{driver}: Connected at {host}'.format(
                driver=self._provider,
                host=self._params["host"]
            )
        )
        return True

    async def close(self):
        print("Connection Closed")

    disconnect = close

    async def get_columns(self):
        return {"id": "value"}

    async def use(self, db):
        print(f'Changing Database to {db}')

    async def query(self, sentence=""):
        error = None
        print("Running Query {}".format(sentence))
        result = ([], error)
        return result

    fetch_all = query

    async def execute(self, sentence=""):
        print("Execute Query {}".format(sentence))
        data = []
        error = None
        result = [data, error]
        return result

    execute_many = execute

    async def queryrow(self, sentence=""):
        error = None
        print("Running Row {}".format(sentence))
        result = ({"row": []}, error)
        return result

    fetch_one = queryrow
