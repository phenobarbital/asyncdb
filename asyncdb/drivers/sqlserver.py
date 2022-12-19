#!/usr/bin/env python3
import asyncio
import time
import logging
from typing import (
    Any,
    Optional
)
from collections.abc import Iterable
import pymssql
from asyncdb.exceptions import (
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    DriverError
)
from .sql import (
    SQLCursor
)
from .mssql import mssql


class sqlserverCursor(SQLCursor):
    _connection = None

    async def __aenter__(self) -> "sqlserverCursor":
        if not self._connection:
            await self.connection()
        self._cursor = self._connection.cursor()
        try:
            self._cursor.execute(self._sentence, self._params)
            return self
        except (pymssql.StandardError, pymssql.Error) as err:
            print(err)
            error = f"SQL Server Error: {err}"
            raise ProviderError(message=error) from err
        except Exception as err:
            print(err)
            raise

    async def __anext__(self):
        """Use `cursor.fetchone()` to provide an async iterable."""
        row = await self.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetchone(self) -> Optional[dict]:
        return self._cursor.fetchone()

    async def fetchmany(self, size: int = None) -> Iterable[list]:
        return self._cursor.fetchmany(size)

    async def fetchall(self) -> Iterable[list]:
        return self._cursor.fetchall()


class sqlserver(mssql):
    """sqlserver.

    Microsoft SQL Server using DB-API connection
    """
    _provider = "sqlserver"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        try:
            self.tds_version = kwargs['tds_version']
            del kwargs["tds_version"]
        except KeyError:
            self.tds_version = "8.0"
        super(sqlserver, self).__init__(
            dsn=dsn,
            loop=loop,
            params=params,
            **kwargs
        )

    async def connection(self) -> Any:
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self.params["appname"] = self.application_name
            self.params["as_dict"] = True
            self.params["timeout"] = self._timeout
            self.params["charset"] = self._charset.upper()
            self.params["tds_version"] = self.tds_version
            self._connection = pymssql.connect(**self.params)
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if 'database' in self.params:
                await self.use(self.params["database"])
            return self
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError(
                f"connection Error, Terminated: {err}"
            ) from err

    async def use(self, database: str):
        try:
            self._cursor = self._connection.cursor()
            self._cursor.execute(f"USE {database!s}")
            return self
        except pymssql.Warning as warn:
            logging.warning(
                f"SQL Server Warning: {warn!s}"
            )
        except (pymssql.StandardError, pymssql.Error) as err:
            raise ProviderError(
                message=f"SQL Server Error: {err}"
            ) from err

    async def execute(self, sentence, *args, **kwargs):
        """
        Execute a sentence
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        # getting a cursor
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._result = self._cursor.execute(sentence, *args)
            print(sentence, self._result)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            logging.debug(error)
            return [self._result, error]  # pylint: disable=W0150

    async def execute_many(self, sentence, *args):
        """
        Execute multiple sentences
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        # getting a cursor
        try:
            self._cursor = self._connection.cursor()
            self._result = self._cursor.executemany(sentence, *args)
        except pymssql.Warning as warn:
            logging.warning(f"SQL Server Warning: {warn!s}")
            error = warn
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            logging.debug(error)
            return [self._result, error]  # pylint: disable=W0150

    executemany = execute_many

    async def query(self, sentence, *args, **kwargs):
        """
        Making a Query and return result
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        if isinstance(sentence, str):
            sentence = sentence.encode(self._charset)
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._cursor.execute(sentence, *args)
            self._result = self._cursor.fetchall()
            if not self._result:
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Query Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def procedure(self, sentence, **kwargs):
        """
        Making a Query and return result based on a Procedure (callproc)
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._cursor = self._connection.cursor()
            params = tuple(
                kwargs.values()
            )
            self._cursor.callproc(
                sentence, params
            )
            self._cursor.nextset()
            self._result = self._cursor.fetchall()
            self._cursor.close()
            try:
                self._connection.commit()
            except pymssql.Error as err:
                logging.error(err)
                self._connection.rollback()
            if not self._result:
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Query Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    callproc = procedure

    async def queryrow(self, sentence, *args, **kwargs):
        error = None
        self._result = None
        await self.valid_operation(sentence)
        if isinstance(sentence, str):
            sentence = sentence.encode(self._charset)
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._cursor.execute(sentence, *args)
            self._result = self._cursor.fetchone()
            if not self._result:
                return [None, NoDataFound("SQL Server: No Data was Found")]
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Query Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def fetch_one(self, sentence, *args, **kwargs):
        self._result = None
        await self.valid_operation(sentence)
        if isinstance(sentence, str):
            sentence = sentence.encode(self._charset)
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._cursor.execute(sentence, args)
            self._result = self._cursor.fetchone()
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
            return self._result
        except (pymssql.StandardError, pymssql.Error) as err:
            raise DataError(
                f"SQL Server Query Error: {err}"
            ) from err
        except RuntimeError as err:
            raise DriverError(
                f"Runtime Error: {err}"
            ) from err
        except Exception as err:  # pylint: disable=W0703
            raise ProviderError(
                f"Error on Query: {err}"
            ) from err

    fetchone = fetch_one

    async def fetch_all(self, sentence, *args, **kwargs):
        self._result = None
        await self.valid_operation(sentence)
        if isinstance(sentence, str):
            sentence = sentence.encode(self._charset)
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._cursor.execute(sentence, args)
            self._result = self._cursor.fetchall()
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
            return self._result
        except (pymssql.StandardError, pymssql.Error) as err:
            raise DataError(
                f"SQL Server Query Error: {err}"
            ) from err
        except RuntimeError as err:
            raise DriverError(
                f"Runtime Error: {err}"
            ) from err
        except Exception as err:  # pylint: disable=W0703
            raise ProviderError(
                f"Error on Query: {err}"
            ) from err

    async def fetch(self, sentence, *args, size: int = 1, **kwargs):
        self._result = None
        if not sentence:
            raise EmptyStatement("Error: Empty Sentence")
        if not self._connection:
            await self.connection()
        try:
            self._cursor = self._connection.cursor(**kwargs)
            self._cursor.execute(sentence, args)
            self._result = self._cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound("SQL Server: No Data was Found")
        except (pymssql.StandardError, pymssql.Error) as err:
            raise DataError(
                f"SQL Server Query Error: {err}"
            ) from err
        except RuntimeError as err:
            raise DriverError(
                f"Runtime Error: {err}"
            ) from err
        except Exception as err:  # pylint: disable=W0703
            raise ProviderError(
                f"Error on Query: {err}"
            ) from err

    async def exec(
        self,
        sentence,
        *args,
        paginated: bool = False,
        page: str = None,
        idx: str = None,
        **kwargs
    ):
        """exec.

        Calling an Stored Function with parameters.
        Args:
            sentence (str): Called Procedure.
            paginated (bool, optional): True if Stored Function is paginated. Defaults to False.
            page (str, optional): Rowset parameter with the number of records. Defaults to None.
            idx (str, optional): Parameter used to declare the Page Index. Defaults to None.
            *args (optional): Any other parameter required by the function (passed on executed).
            **kwargs (str, optional): any parameter required by the stored function.
        Returns:
            Tuple(Any, Exception): tuple with resultset and possible error.
        """
        error = None
        self._result = None
        await self.valid_operation(sentence)
        try:
            self._cursor = self._connection.cursor()
            if idx not in kwargs:
                kwargs[idx] = 1
            if kwargs:
                params = ', '.join([f'{k}={v}' for k, v in kwargs.items()])
            else:
                params = ''
            procedure = f'EXEC {sentence} {params}'
            self._cursor.execute(procedure, *args)
            result = self._cursor.fetchall()
            if not result:
                return [None, NoDataFound("SQL Server: No Data was Found")]
            else:
                # preparing for pagination and other stuff.
                rowlen = len(result)
                if paginated is True:
                    results = []
                    # this procedure is paginated:
                    sample_row = result[0]
                    num_rows = sample_row[page]
                    if num_rows > rowlen:
                        results.extend(result)
                        # there are many more results:
                        index = kwargs[idx]
                        new_index = index + rowlen
                        kwargs[idx] = new_index
                        r, error = await self.exec(sentence, paginated=paginated, page=page, idx=idx, **kwargs)
                        if r and not error:
                            results.extend(r)
                        return await self._serializer(results, error)
                    else:
                        return await self._serializer(result, error)
                else:
                    # is not paginated, return as usual:
                    return await self._serializer(result, error)
        except (pymssql.StandardError, pymssql.Error) as err:
            error = f"SQL Server Query Error: {err}"
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150
