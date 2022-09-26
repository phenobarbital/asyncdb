#!/usr/bin/env python3

import asyncio
import time
from datetime import datetime
import aiomysql
from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
)
from asyncdb.utils.types import (
    SafeDict,
)
from .abstract import (
    BasePool
)
from .sql import SQLDriver


class mysqlPool(BasePool):
    _max_queries = 300
    # _dsn = 'mysql://{user}:{password}@{host}:{port}/{database}'
    loop = asyncio.get_event_loop()

    def __init__(self, loop=None, params={}):
        self._logger.debug("Ready")
        super(mysqlPool, self).__init__(loop=loop, params=params)

    def get_event_loop(self):
        return self._loop

    """
    __init async db initialization
    """

    # Create a database connection pool
    async def connect(self):
        self._logger.debug("aioMysql: Connecting to {}".format(self._params))
        self._logger.debug("Start connection")
        try:
            # TODO: pass a setup class for set_builtin_type_codec and a setup for add listener
            self._pool = await aiomysql.create_pool(
                host=self._params["host"],
                user=self._params["user"],
                password=self._params["password"],
                db=self._params["database"],
                loop=self._loop,
            )
        except TimeoutError as err:
            raise ConnectionTimeout(
                "Unable to connect to database: {}".format(str(err))
            )
        except ConnectionRefusedError as err:
            raise ProviderError(
                "Unable to connect to database, connection Refused: {}".format(str(err))
            )
        except Exception as err:
            raise ProviderError("Unknown Error: {}".format(str(err)))
            return False
        # is connected
        if self._pool:
            self._connected = True
            self._initialized_on = time.time()

    """
       Take a connection from the pool.
       """

    async def acquire(self):
        self._logger.debug("Acquire")
        db = None
        self._connection = None
        # Take a connection from the pool.
        try:
            self._connection = await self._pool.acquire()
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        if self._connection:
            db = mysql(pool=self)
            db.set_connection(self._connection)
        return db

    """
    Release a connection from the pool
    """

    async def release(self, connection=None, timeout=10):
        self._logger.debug("Release")
        if not connection:
            conn = self._connection
        else:
            conn = connection
        try:
            await self._pool.release(conn)
            # print('r', r)
            # release = asyncio.create_task(r)
            # await self._pool.release(connection, timeout = timeout)
            # release = asyncio.ensure_future(release, loop=self._loop)
            # await asyncio.wait_for(release, timeout=timeout, loop=self._loop)
            # await release
        except Exception as err:
            raise ProviderError("Release Error: {}".format(str(err)))

    """
    close
        Close Pool Connection
    """

    async def wait_close(self, gracefully=True):
        if self._pool:
            self._logger.debug("aioMysql: Closing Pool")
            # try to closing main connection
            try:
                if self._connection:
                    await self._pool.release(self._connection, timeout=2)
            except Exception as err:
                raise ProviderError("Release Error: {}".format(str(err)))
            # at now, try to closing pool
            try:
                await self._pool.close()
                # await self._pool.terminate()
            except Exception as err:
                print("Pool Error: {}".format(str(err)))
                await self._pool.terminate()
                raise ProviderError("Pool Error: {}".format(str(err)))
            finally:
                self._pool = None

    """
    Close Pool
    """

    async def close(self):
        # try:
        #    if self._connection:
        #        print('self._pool', self._pool)
        #        await self._pool.release(self._connection)
        # except Exception as err:
        #    raise ProviderError("Release Error: {}".format(str(err)))
        try:
            await self._pool.close()
        except Exception as err:
            print("Pool Closing Error: {}".format(str(err)))
            self._pool.terminate()

    def terminate(self, gracefully=True):
        self._loop.run_until_complete(asyncio.wait_for(self.close(), timeout=5))

    """
    Execute a connection into the Pool
    """

    async def execute(self, sentence, *args):
        if self._pool:
            try:
                result = await self._pool.execute(sentence, *args)
                return result
            except Exception as err:
                raise ProviderError("Execute Error: {}".format(str(err)))


class mysql(SQLDriver):

    _provider = "mysql"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _dsn = "mysql://{user}:{password}@{host}:{port}/{database}"
    _loop = None
    _pool = None
    _connection = None
    _connected = False
    _prepared = None
    _parameters = ()
    _cursor = None
    _transaction = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"

    def __init__(self, loop=None, pool=None, params={}):
        super(mysql, self).__init__(loop=loop, params=params)
        asyncio.set_event_loop(self._loop)

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                if not self._connection.closed:
                    self._logger.debug("Closing Connection")
                    try:
                        if self._pool:
                            self._pool.close()
                        else:
                            self._connection.close()
                    except Exception as err:
                        self._pool.terminate()
                        self._connection = None
                        raise ProviderError(
                            "Connection Error, Terminated: {}".format(str(err))
                        )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False

    def terminate(self):
        self._loop.run_until_complete(self.close())

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        self._cursor = None
        try:
            if not self._pool:
                self._pool = await aiomysql.create_pool(
                    host=self._params["host"],
                    user=self._params["user"],
                    password=self._params["password"],
                    db=self._params["database"],
                    loop=self._loop,
                )
            self._connection = await self._pool.acquire()
            self._cursor = await self._connection.cursor()
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            self._connection = None
            self._cursor = None
            raise ProviderError("connection Error, Terminated: {}".format(str(err)))
        finally:
            return self._connection

    """
    Release a Connection
    """

    async def release(self):
        try:
            if not await self._connection.closed:
                if self._pool:
                    release = asyncio.create_task(
                        self._pool.release(self._connection, timeout=10)
                    )
                    asyncio.ensure_future(release, loop=self._loop)
                    return await release
                else:
                    await self._connection.close(timeout=5)
        except Exception as err:
            raise ProviderError("Release Interface Error: {}".format(str(err)))
            return False
        finally:
            self._connected = False
            self._connection = None

    def prepared_statement(self):
        return self._prepared

    @property
    def connected(self):
        if self._pool:
            return not self._pool._closed
        elif self._connection:
            return not self._connection.closed

    """
    Preparing a sentence
    """

    async def prepare(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")

        try:
            if not self._connection:
                await self.connection()
            try:
                stmt = await asyncio.shield(self._connection.prepare(sentence))
                try:
                    # print(stmt.get_attributes())
                    self._columns = [a.name for a in stmt.get_attributes()]
                    self._prepared = stmt
                    self._parameters = stmt.get_parameters()
                except TypeError:
                    self._columns = []
            except RuntimeError as err:
                error = "Prepare Runtime Error: {}".format(str(err))
                raise StatementError(message=error)
            except Exception as err:
                error = "Unknown Error: {}".format(str(err))
                raise ProviderError(message=error)
        finally:
            return [self._prepared, error]

    async def query(self, sentence="", size=100000000000):
        # self._logger.debug("Start Query function")
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            await self._cursor.execute(sentence)
            self._result = await self.fetchmany(size)
            if not self._result:
                raise NoDataFound("Mysql: No Data was Found")
                return [None, "Mysql: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            #    self._generated = datetime.now() - startTime
            #    await self.close()
            return [self._result, error]

    async def queryrow(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            # stmt = await self._connection.prepare(sentence)
            # self._columns = [a.name for a in stmt.get_attributes()]
            await self._cursor.execute(sentence)
            self._result = await self.fetchone()
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        # finally:
        # await self.close()
        return [self._result, error]

    async def execute(self, sentence=""):
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            result = await self._cursor.execute(sentence)
            return [result, None]
            return [None, error]
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [result, error]

    async def executemany(self, sentence="", args=[]):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            await self.begin()
            await self._cursor.executemany(sentence, args)
            await self.commit()
            return False
        except Exception as err:
            await self.rollback()
            error = "Error on Execute: {}".format(str(err))
            raise Exception(error)
        finally:
            return error

    """
    Transaction Context
    """

    async def begin(self):
        if not self._connection:
            await self.connection()
        await self._connection.begin()
        return self

    async def commit(self):
        if not self._connection:
            await self.connection()
        await self._connection.commit()
        return self

    async def rollback(self):
        if not self._connection:
            await self.connection()
        await self._connection.rollback()
        return self

    """
    Cursor Context
    """

    async def cursor(self, sentence=""):
        self._logger.debug("Cursor")
        if not self._connection:
            await self.connection()
        return self._cursor

    async def forward(self, number):
        try:
            return await self._cursor.scroll(number)
        except Exception as err:
            error = "Error forward Cursor: {}".format(str(err))
            raise Exception(error)

    async def fetchall(self):
        try:
            return await self._cursor.fetchall()
        except Exception as err:
            error = "Error FetchAll Cursor: {}".format(str(err))
            raise Exception(error)

    async def fetchmany(self, size=None):
        try:
            return await self._cursor.fetchmany(size)
        except Exception as err:
            error = "Error FetchMany Cursor: {}".format(str(err))
            raise Exception(error)

    async def fetchone(self):
        try:
            return await self._cursor.fetchone()
        except Exception as err:
            error = "Error FetchOne Cursor: {}".format(str(err))
            raise Exception(error)

    """
    Cursor Iterator Context
    """

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self._cursor.fetchrow()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

    """
    COPY Functions
    type: [ text, csv, binary ]
    """

    async def copy_from_table(
        self, table="", schema="public", output=None, type="csv", columns=None
    ):
        """table_copy
        get a copy of table data into a file, file-like object or a coroutine passed on "output"
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        try:
            result = await self._connection.copy_from_table(
                table_name=table,
                schema_name=schema,
                columns=columns,
                format=type,
                output=output,
            )
            print(result)
            return result
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

    async def copy_to_table(
        self, table="", schema="public", source=None, type="csv", columns=None
    ):
        """copy_to_table
        get data from a file, file-like object or a coroutine passed on "source" and copy into table
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        try:
            result = await self._connection.copy_to_table(
                table_name=table,
                schema_name=schema,
                columns=columns,
                format=type,
                source=source,
            )
            print(result)
            return result
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

    async def copy_into_table(
        self, table="", schema="public", source=None, columns=None
    ):
        """copy_into_table
        get data from records (any iterable object) and save into table
        returns: num of rows copied.
        example: COPY 1470
        """
        if not self._connection:
            await self.connection()
        try:
            result = await self._connection.copy_records_to_table(
                table_name=table, schema_name=schema, columns=columns, records=source
            )
            print(result)
            return result
        except Exception as err:
            error = "Error on Table Copy: {}".format(str(err))
            raise Exception(error)

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
                                type(value) == str
                                and value.startswith("'")
                                and value.endswith("'")
                            ):
                                where_cond.append("%s = %s" % (key, "{}".format(value)))
                            elif type(value) == int:
                                where_cond.append("%s = %s" % (key, "{}".format(value)))
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
                            where_cond.append("%s IN (%s)" % (key, "'{}'".format(val)))
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
                return "{q} ORDER BY {ordering}".format(q=sentence, ordering=ordering)
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
                self._loop.run_until_complete(self.connection())
            prepared, error = self._loop.run_until_complete(self.prepare(sql))
            if not error:
                self._columns = self.get_columns()
            else:
                print("Error in Get Query", error)
                return False
        except (ProviderError, StatementError) as err:
            print("ProviderError or StatementError Exception in Get Query", e)
            return False
        except Exception as e:
            print("Exception in Get Query", e)
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
        values = ",".join(str(v) for v in data.values())
        sql = sql.format_map(SafeDict(values=values))
        try:
            result = self._loop.run_until_complete(self._connection.execute(sql))
            if not result:
                print(result)
                return False
            else:
                return result
        except Exception as err:
            # print(sql)
            print(err)
            return False
