from typing import Any, Optional, Union
from collections.abc import Iterable, Sequence
import asyncio
import time
import duckdb as db
from asyncdb.exceptions import (
    NoDataFound,
    DriverError
)
from asyncdb.interfaces import DBCursorBackend
from .sql import SQLCursor, SQLDriver


class duckdbCursor(SQLCursor):
    """
    Cursor Object for SQLite.
    """
    _provider: "duckdb"
    _connection: db.DuckDBPyConnection = None

    async def __aenter__(self) -> "duckdbCursor":
        self._cursor = self._connection.execute(
            self._sentence, parameters=self._params
        )
        return self

    async def __anext__(self):
        """Use `cursor.fetchrow()` to provide an async iterable.
            raise: StopAsyncIteration when done.
        """
        row = self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

### Cursor Methods.
    async def fetch_one(self) -> Optional[Sequence]:
        return self._cursor.fetchone()

    async def fetch_many(self, size: int = None) -> Iterable[Sequence]:
        return self._cursor.fetch(size)

    async def fetch_all(self) -> Iterable[Sequence]:
        return self._cursor.fetchall()

class duckdb(SQLDriver, DBCursorBackend):
    _provider: str = 'duckdb'
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

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = db.connect(
                database=self._dsn, **kwargs
            )
            if self._connection:
                if self._init_func is not None and callable(self._init_func):
                    try:
                        await self._init_func( # pylint: disable=E1102
                            self._connection
                        )
                    except RuntimeError as err:
                        self._logger.exception(
                            f"Error on Init Connection: {err!s}"
                        )
                self._connected = True
                self._initialized_on = time.time()
            return self
        except duckdb.ConnectionException as e:
            raise DriverError(
                f"Unable to Open Database: {self._dsn}, {e}"
            ) from e
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise DriverError(
                f"SQLite Unknown Error: {e!s}"
            ) from e

    connect = connection

    async def prepare(self, sentence: Union[str, list]) -> Any:
        "Ignoring prepared sentences on DuckDB for now"
        raise NotImplementedError()  # pragma: no cover

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    async def use(self, database: str):
        raise NotImplementedError(
            'DuckDB Error: There is no Database in DuckDB'
        )


    async def close(self, timeout: int = 5) -> None:
        """
        Closing the Connection on DuckDB
        """
        try:
            if self._connection:
                self._connection.close()
        except Exception as err:
            raise DriverError(
                message=f"{__name__!s}: Closing Error: {err!s}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    async def query(self, sentence: Any, *args, **kwargs) -> Any:
        """
        Getting a Query from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = self._connection.execute(sentence, *args, **kwargs)
            self._result = cursor.fetchall()
            if not self._result:
                return (None, NoDataFound())
        except Exception as err:
            error = f"DuckDB Error on Query: {err}"
            raise DriverError(
                message=error
            ) from err
        finally:
            return await self._serializer(self._result, error)

    async def queryrow(self, sentence: Any = None) -> Iterable[Any]:
        """
        Getting a single Row from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = self._connection.execute(sentence)
            self._result = cursor.fetchone()
            if not self._result:
                return (None, NoDataFound())
        except Exception as e:
            error = f"Error on Query: {e}"
            raise DriverError(
                message=error
            ) from e
        finally:
            return await self._serializer(self._result, error)

    async def fetch_all(self, sentence: str, *args, **kwargs) -> Sequence:
        """
        Alias for Query, but without error Support.
        """
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = self._connection.execute(sentence, *args, **kwargs)
            self._result = await cursor.fetchall()
            if not self._result:
                raise NoDataFound(
                    "DuckDB Fetch All: Data Not Found"
                )
            return self._result
        except Exception as e:
            error = f"Error on Fetch: {e}"
            raise DriverError(
                message=error
            ) from e

    # alias to be compatible with aiosqlite methods.
    fetchall = fetch_all

    async def fetch_many(self, sentence: str, size: int = None):
        """
        Aliases for query, without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = self._connection.execute(sentence)
            self._result = cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound()
            return self._result
        except Exception as err:
            error = f"Error on Query: {err}"
            raise DriverError(
                message=error
            ) from err

    fetchmany = fetch_many

    async def fetch_one(
            self,
            sentence: str,
            *args,
            **kwargs
    ) -> Optional[dict]:
        """
        aliases for queryrow, but without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = self._connection.execute(sentence, *args, **kwargs)
            self._result = cursor.fetchone()
            return self._result
            if not self._result:
                raise NoDataFound()
        except Exception as err:
            error = f"Error on Query: {err}"
            raise DriverError(
                message=error
            ) from err

    fetchone = fetch_one
    fetchrow = fetch_one

    async def execute(self, sentence: Any, **kwargs) -> Optional[Any]:
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        if kwargs:
            params = kwargs
        else:
            params = None
        await self.valid_operation(sentence)
        try:
            if (result:= self._connection.execute(sentence, parameters=params)):
                self._connection.commit()
        except Exception as err:
            error = f"Error on Execute: {err}"
            raise DriverError(
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
            result = self._connection.executemany(sentence, parameters=args)
            if result:
                self._connection.commit()
        except Exception as err:
            error = f"Error on Execute Many: {err}"
            raise DriverError(
                message=error
            ) from err
        finally:
            return (result, error)

    executemany = execute_many

    async def __aenter__(self) -> Any:
        try:
            await self.connection()
        except Exception as err:
            error = f"Error on Cursor Fetch: {err}"
            raise DriverError(
                message=error
            ) from err
        return self

    async def fetch(
                    self,
                    sentence: str,
                    parameters: Iterable[Any] = None
            ) -> Iterable:
        """Helper to create a cursor and execute the given query."""
        await self.valid_operation(sentence)
        if parameters is None:
            parameters = []
        try:
            result = self._connection.execute(
                sentence, parameters=parameters
            )
        except Exception as err:
            error = f"Error on Cursor Fetch: {err}"
            raise DriverError(
                message=error
            ) from err
        return result

    async def __anext__(self) -> Optional[Any]:
        """_summary_

        Raises:
            StopAsyncIteration: raised when end is reached.

        Returns:
            _type_: Single record for iteration.
        """
        data = self._cursor.fetchone()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

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
                result = self._connection.execute(sql)
                if result:
                    self._connection.commit()
                    return True
                else:
                    return False
            except Exception as err:
                raise DriverError(
                    f"Error in Object Creation: {err!s}"
                ) from err
        else:
            raise RuntimeError(
                f'DuckDB: invalid Object type {object!s}'
            )
