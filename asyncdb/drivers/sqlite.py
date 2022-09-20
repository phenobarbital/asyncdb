#!/usr/bin/env python3
import time
import asyncio
from typing import (
    Any,
    Optional,
    Union
)
from collections.abc import Sequence, Iterable
import aiosqlite
from asyncdb.exceptions import (
    NoDataFound,
    ProviderError
)
from asyncdb.interfaces import DBCursorBackend
from .sql import SQLDriver, SQLCursor



class sqliteCursor(SQLCursor):
    """
    Cursor Object for SQLite.
    """
    _provider: "sqlite"
    _connection: aiosqlite.Connection = None

    async def __aenter__(self) -> "sqliteCursor":
        self._cursor = await self._connection.execute(
            self._sentence, self._params
        )
        return self


class sqlite(SQLDriver, DBCursorBackend):
    _provider: str = 'sqlite'
    _syntax: str = 'sql'
    _dsn: str = "{database}"

    def __init__(
            self,
            dsn: str = "",
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        SQLDriver.__init__(self, dsn, loop, params, **kwargs)
        DBCursorBackend.__init__(self)

    async def prepare(self):
        "Ignoring prepared sentences on SQLite"
        raise NotImplementedError()  # pragma: no cover

    async def __aenter__(self) -> Any:
        if not self._connection:
            await self.connection()
        return self

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await aiosqlite.connect(
                database=self._dsn, **kwargs
            )
            if self._connection:
                if callable(self.init_func):
                    try:
                        await self.init_func(
                            self._connection
                        )
                    except RuntimeError as err:
                        self._logger.exception(
                            f"Error on Init Connection: {err!s}"
                        )
                self._connected = True
                self._initialized_on = time.time()
            return self
        except aiosqlite.OperationalError as e:
            raise ProviderError(
                f"Unable to Open Database: {self._dsn}, {e}"
            ) from e
        except aiosqlite.DatabaseError as e:
            raise ProviderError(
                f"Database Connection Error: {e!s}"
            ) from e
        except aiosqlite.Error as e:
            raise ProviderError(
                f"SQLite Internal Error: {e!s}"
            ) from e
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise ProviderError(
                f"SQLite Unknown Error: {e!s}"
            ) from e

    connect = connection

    async def valid_operation(self, sentence: Any):
        await super(sqlite, self).valid_operation(sentence)
        if self._row_format == 'iterable':
            # converting to a dictionary
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
        else:
            self._connection.row_factory = None

    async def query(self, sentence: Any = None) -> Any:
        """
        Getting a Query from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchall()
            if not self._result:
                return (None, NoDataFound())
        except Exception as err:
            error = f"SQLite Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return await self._serializer(self._result, error)

    async def queryrow(self, sentence: Any = None) -> Iterable[Any]:
        """
        Getting a single Row from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchone()
            if not self._result:
                return (None, NoDataFound())
        except Exception as e:
            error = f"Error on Query: {e}"
            raise ProviderError(
                message=error
            ) from e
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return await self._serializer(self._result, error)

    async def fetch_all(self, sentence: str, **kwargs) -> Sequence:
        """
        Alias for Query, but without error Support.
        """
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = await self._connection.execute(sentence, parameters=kwargs)
            self._result = await cursor.fetchall()
            if not self._result:
                raise NoDataFound(
                    "SQLite Fetch All: Data Not Found"
                )
        except Exception as e:
            error = f"Error on Fetch: {e}"
            raise ProviderError(
                message=error
            ) from e
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    # alias to be compatible with aiosqlite methods.
    fetchall = fetch_all

    async def fetch_many(self, sentence: str, size: int = None):
        """
        Aliases for query, without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    fetchmany = fetch_many

    async def fetch_one(
            self,
            sentence: str
    ) -> Optional[dict]:
        """
        aliases for queryrow, but without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchone()
            if not self._result:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    fetchone = fetch_one
    fetchrow = fetch_one

    async def execute(self, sentence: Any, *args) -> Optional[Any]:
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        try:
            result = await self._connection.execute(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Execute: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return (result, error)

    async def execute_many(
            self,
            sentence: Union[str, list],
            *args
    ) -> Optional[Any]:
        error = None
        await self.valid_operation(sentence)
        try:
            result = await self._connection.executemany(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Execute Many: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return (result, error)

    executemany = execute_many

    async def fetch(
                    self,
                    sentence: str,
                    parameters: Iterable[Any] = None
            ) -> Iterable:
        """Helper to create a cursor and execute the given query."""
        await self.valid_operation(sentence)
        if parameters is None:
            parameters = []
        result = await self._connection.execute(
            sentence, parameters
        )
        return result

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    def use(self, tablename: str):
        raise NotImplementedError(
            'SQLite Error: There is no Database in SQLite'
        )

    async def column_info(
            self,
            table: str,
            **kwargs
    ) -> Iterable[Any]:
        """
        Getting Column info from an existing Table in Provider.
        """
        try:
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r))
            cursor = await self._connection.execute(
                f'PRAGMA table_info({table});', parameters=kwargs
            )
            cols = await cursor.fetchall()
            self._columns = []
            for col in cols:
                d = {
                    "name": col['name'],
                    "type": col['type']
                }
                self._columns.append(d)
            if not self._columns:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Column Info: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return self._columns

    async def create(
        self,
        obj: str = 'table',
        name: str = '',
        fields: Optional[list] = None
    ) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if obj == 'table':
            sql = "CREATE TABLE {name} ({columns});"
            columns = ", ".join(["{name} {type}".format(**e) for e in fields])
            sql = sql.format(name=name, columns=columns)
            try:
                result = await self._connection.execute(sql)
                if result:
                    await self._connection.commit()
                    return True
                else:
                    return False
            except Exception as err:
                raise ProviderError(
                    f"Error in Object Creation: {err!s}"
                ) from err
        else:
            raise RuntimeError(
                f'SQLite: invalid Object type {object!s}'
            )
