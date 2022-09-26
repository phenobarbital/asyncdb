"""
SQLProvider.

Abstract class covering all major functionalities for Relational SQL-based databases.
"""
import asyncio
from typing import (
    Any
)
from collections.abc import Iterable
from asyncdb.exceptions import (
    DriverError,
    ProviderError
)
from .abstract import BaseDBDriver, BaseCursor



class SQLCursor(BaseCursor):
    """SQLCursor.

    Base class for all SQL-based Drivers.
    """
    _connection = None

    async def __aenter__(self) -> "BaseCursor":
        try:
            self._cursor = await self._connection.cursor(
                self._sentence, self._params
            )
        except Exception as e:
            raise ProviderError(
                f"SQLCursor Error: {e}"
            ) from e
        return self


class SQLDriver(BaseDBDriver):
    """SQLDriver.

    Driver for SQL-based providers.
    """
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(
            self,
            dsn: str = "",
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
        ) -> None:
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        super(SQLDriver, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs
        )

    async def close(self, timeout: int = 5) -> None:
        """
        Closing Method for any SQL Connector.
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise DriverError(
                message=f"{__name__!s}: Closing Error: {err!s}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    # alias for connection
    disconnect = close

    async def column_info(
            self,
            tablename: str,
            schema: str = ''
    ) -> Iterable[Any]:
        """
        column_info
          get column information about a table
          TODO: rewrite column info using information schema.
        """
