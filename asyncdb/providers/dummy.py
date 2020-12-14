import asyncio
import logging
import os
import sys
import time
from . import BaseProvider

class dummy(BaseProvider):
    _provider = "dummy"
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, params={}, **kwargs):
        #
        try:
            super(dummy, self).__init__(params, **kwargs)
            self._logger.debug(" My params are: {}".format(params))
        except Exception as err:
            raise ProviderError(str(err), errcode=500)

    async def prepare(self):
        pass

    async def connection(self):
        print(
            "{}: Connected at {} with user {} and password {}".format(
                self._provider,
                self._params["host"],
                self._params["user"],
                self._params["password"],
            )
        )
        return True

    async def close(self):
        print("Connection Closed")

    async def get_columns(self):
        return {"id": "value"}

    async def query(self, sentence=""):
        error = None
        print("Running Query {}".format(sentence))
        result = ({"data": []}, error)
        return result

    async def execute(self, sentence=""):
        print("Execute Query {}".format(sentence))
        data = []
        error = None
        result = [data, error]
        return result

    async def fetchrow(self, sentence=""):
        pass


registerProvider(dummy)
