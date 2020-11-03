import importlib
from asyncdb.providers import BaseProvider

from typing import (
    Any,
    List,
    Dict,
    Generator,
    Iterable,
    Optional,
)

from asyncdb.utils import (
    SafeDict,
    _escapeString,
)

class baseCursor:
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """
    _cursor = None
    _connection = None
    _provider: BaseProvider = None
    _result: Any = None
    _sentence: str = ''

    def __init__(
        self,
        provider:BaseProvider,
        sentence:str,
        result: Optional[List] = None,
        parameters: Iterable[Any] = None
    ):
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.get_connection()

    async def __aenter__(self) -> "baseCursor":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(
            self._sentence, self._params
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self._provider.close()

    def __aiter__(self) -> "baseCursor":
        """The cursor is also an async iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchone()` to provide an async iterable."""
        row = await self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetchone(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetchmany(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetchmany(size)

    async def fetchall(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class SQLProvider(BaseProvider):
    """
    SQLProvider.

    Driver methods for SQL-based providers
    """
    _syntax = "sql"
    _test_query = "SELECT 1"
    _prepared = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    __cursor__ = None


    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        try:
            # dynamic loading of Cursor Class
            cls = f'asyncdb.providers.{self._provider}'
            cursor = f'{self._provider}Cursor'
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except ImportError as err:
            print('Error Loading Cursor Class: ', err)
            pass
        super(SQLProvider, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback, *args):
        self.terminate()
        pass

    async def __aenter__(self) -> "sqlite":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.close(timeout=5)

    async def close(self, timeout=5):
        """
        Closing Method for SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError("{}: Closing Error: {}".format(__name__, str(err)))
        finally:
            self._connection = None
            self._connected = False
            return True

    async def connect(self, **kwargs):
        """
        Get a proxy connection, alias of connection
        """
        self._connection = None
        self._connected = False
        return await self.connection(self, **kwargs)

    async def release(self):
        """
        Release a Connection
        """
        await self.close()


    async def valid_operation(self, sentence: str):
        self._result = None
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if not self._connection:
            await self.connection()


    def cursor(self, sentence: str, parameters: Iterable[Any] = None) -> Iterable:
        """ Returns a iterable Cursor Object """
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if parameters is None:
            parameters = []
        try:
            return self.__cursor__(self, sentence=sentence, parameters=parameters)
        except Exception as err:
            print(err)
            return False

    """
    Meta-Operations
    """

    def table(self, table):
        try:
            return self._query_raw.format_map(SafeDict(table=table))
        except Exception as e:
            print(e)
            return False

    def fields(self, sentence, fields=None):
        _sql = False
        if not fields:
            _sql = sentence.format_map(SafeDict(fields="*"))
        elif type(fields) == str:
            _sql = sentence.format_map(SafeDict(fields=fields))
        elif type(fields) == list:
            _sql = sentence.format_map(SafeDict(fields=",".join(fields)))
        return _sql

    """
    where
      add WHERE conditions to SQL
    """

    def where(self, sentence, where):
        sql = ""
        if sentence:
            where_string = ""
            if not where:
                sql = sentence.format_map(SafeDict(where_cond=""))
            elif type(where) == dict:
                where_cond = []
                for key, value in where.items():
                    # print("KEY {}, VAL: {}".format(key, value))
                    if type(value) == str or type(value) == int:
                        if value == "null" or value == "NULL":
                            where_string.append("%s IS NULL" % (key))
                        elif value == "!null" or value == "!NULL":
                            where_string.append("%s IS NOT NULL" % (key))
                        elif key.endswith("!"):
                            where_cond.append("%s != %s" % (key[:-1], value))
                        else:
                            if (
                                type(value) == str and value.startswith("'")
                                and value.endswith("'")
                            ):
                                where_cond.append(
                                    "%s = %s" % (key, "{}".format(value))
                                )
                            elif type(value) == int:
                                where_cond.append(
                                    "%s = %s" % (key, "{}".format(value))
                                )
                            else:
                                where_cond.append(
                                    "%s = %s" % (key, "'{}'".format(value))
                                )
                    elif type(value) == bool:
                        val = str(value)
                        where_cond.append("%s = %s" % (key, val))
                    else:
                        val = ",".join(map(str, value))
                        if type(val) == str and "'" not in val:
                            where_cond.append(
                                "%s IN (%s)" % (key, "'{}'".format(val))
                            )
                        else:
                            where_cond.append("%s IN (%s)" % (key, val))
                # if 'WHERE ' in sentence:
                #    where_string = ' AND %s' % (' AND '.join(where_cond))
                # else:
                where_string = " WHERE %s" % (" AND ".join(where_cond))
                print("WHERE cond is %s" % where_string)
                sql = sentence.format_map(SafeDict(where_cond=where_string))
            elif type(where) == str:
                where_string = where
                if not where.startswith("WHERE"):
                    where_string = " WHERE %s" % where
                sql = sentence.format_map(SafeDict(where_cond=where_string))
            else:
                sql = sentence.format_map(SafeDict(where_cond=""))
            del where
            del where_string
            return sql
        else:
            return False

    def limit(self, sentence, limit=1):
        """
        LIMIT
          add limiting to SQL
        """
        if sentence:
            return "{q} LIMIT {limit}".format(q=sentence, limit=limit)
        return self

    def orderby(self, sentence, ordering=[]):
        """
        LIMIT
          add limiting to SQL
        """
        if sentence:
            if type(ordering) == str:
                return "{q} ORDER BY {ordering}".format(
                    q=sentence, ordering=ordering
                )
            elif type(ordering) == list:
                return "{q} ORDER BY {ordering}".format(
                    q=sentence, ordering=", ".join(ordering)
                )
        return self

    def get_query(self, sentence):
        """
        get_query
          Get formmated query
        """
        sql = sentence
        try:
            # remove fields and where_cond
            sql = sentence.format_map(SafeDict(fields="*", where_cond=""))
            if not self.connected:
                self.connection()
            prepared, error = self._loop.run_until_complete(self.prepare(sql))
            if not error:
                self._columns = self.get_columns()
            else:
                return False
        except (ProviderError, StatementError) as err:
            return False
        except Exception as e:
            print(e)
            return False
        return sql

    def column_info(self, table):
        """
        column_info
          get column information about a table
        """
        discover = "SELECT attname AS column_name, atttypid::regtype AS data_type FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum > 0 AND NOT attisdropped ORDER  BY attnum".format(
            table
        )
        try:
            result, error = self._loop.run_until_complete(self.query(discover))
            if result:
                return result
        except (NoDataFound, ProviderError):
            print(err)
            return False
        except Exception as err:
            print(err)
            return False

    def insert(self, table, data, **kwargs):
        """
        insert
           insert the result onto a table
        """
        sql = "INSERT INTO {table} ({fields}) VALUES ({values})"
        sql = sql.format_map(SafeDict(table=table))
        # set columns
        sql = sql.format_map(SafeDict(fields=",".join(data.keys())))
        values = ",".join(_escapeString(v) for v in data.values())
        sql = sql.format_map(SafeDict(values=values))
        # print(sql)
        try:
            result = self._loop.run_until_complete(
                self._connection.execute(sql)
            )
            if not result:
                print(result)
                return False
            else:
                return result
        except Exception as err:
            print(sql)
            print(err)
            return False
