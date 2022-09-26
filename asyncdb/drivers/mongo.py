#!/usr/bin/env python3

import asyncio
import time
import motor.motor_asyncio
from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
    DriverError
)
from .abstract import BaseDriver


class mongo(BaseDriver):
    _provider = "mongodb"
    _dsn = "'mongodb://{host}:{port}"
    _syntax = "mongo"
    _parameters = ()
    _initialized_on = None
    _timeout: int = 5
    _databases: list = []

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        if "username" in params:
            self._dsn = "mongodb://{username}:{password}@{host}:{port}"
        if "database" in params:
            self._dsn = self._dsn + "/{database}"
        super(mongo, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            if self._dsn:
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(self._dsn)
            else:
                params = {"host": self._params["host"], "port": self._params["port"]}
                if self._params["username"]:
                    params["username"] = self._params["username"]
                    params["password"] = self._params["password"]
                self._connection = motor.motor_asyncio.AsyncIOMotorClient(**params)
            try:
                self._databases = await self._connection.list_database_names()
            except Exception as err:
                raise DriverError(
                    f"Error Connecting to Mongo: {err}"
                ) from err
            if len(self._databases) > 0:
                self._connected = True
                self._initialized_on = time.time()
            return self
        except Exception as err:
            self._connection = None
            self._cursor = None
            print(err)
            raise ProviderError(
                f"connection Error, Terminated: {err}"
            ) from err

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                self._logger.debug("Closing Connection")
                try:
                    self._connection.close()
                except Exception as err:
                    self._connection = None
                    raise ProviderError(
                        "Connection Error, Terminated: {}".format(str(err))
                    )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False

    async def test_connection(self):
        """
        Getting information about Server.
        """
        error = None
        result = None
        if self._connection:
            print("TEST")
            try:
                result = await self._connection.server_info()
            except Exception as err:
                error = err
            finally:
                return [result, error]

    async def execute(self):
        pass

    async def query(self):
        pass

    async def queryrow(self):
        pass
