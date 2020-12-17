#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime

from influxdb import InfluxDBClient
from functools import partial

from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.providers import (
    BasePool,
    BaseProvider,
    registerProvider,
)
from asyncdb.utils import (
    EnumEncoder,
    SafeDict,
)


class influx(BaseProvider):

    _provider = "influxdb"
    _dsn = "influxdb://{user}:{password}@{host}:{port}/{database}"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _parameters = ()
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    _timeout: int = 5
    _version: str = None

    def __init__(self, loop=None, pool=None, params={}, **kwargs):
        super(influx, self).__init__(loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)

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
                            "Connection Error, Terminated: {}".format(
                                str(err)
                            )
                        )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
        finally:
            self._connection = None
            self._connected = False

    async def test_connection(self):
        error = None
        result = None
        if self._connection:
            print('TEST')
            try:
                result = await self.query('SHOW databases')
            except Exception as err:
                error = err
            finally:
                return [result, error]

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            if self._dsn:
                self._connection = InfluxDBClient.from_dsn(
                    self._dsn,
                    timeout=self._timeout
                )
            else:
                params = {
                    "host": self._params["host"],
                    "port": self._params["port"],
                    "database": self._params['database'],
                    "timeout": self._timeout,
                    "pool_size": 10,
                    "gzip": True
                }
                print(params)
                if 'user' in self._params:
                    params['username'] = self._params["user"]
                    params['password'] = self._params["password"]
                self._connection = InfluxDBClient(**params)
            self._version = self._connection.ping()
            if self._version:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            self._connection = None
            self._cursor = None
            print(err)
            raise ProviderError(
                "connection Error, Terminated: {}".format(str(err))
            )
        finally:
            return self

    async def create_database(self, database: str = '', use: bool = False):
        try:
            self._connection.create_database(database)
        except Exception as err:
            raise ProviderError('Error creating Database {}'.format(err))
        if use is True:
            self._connection.switch_database(database)

    async def query(self, sentence="", **kwargs):
        # self._logger.debug("Start Query function")
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            print(startTime)
            fn = partial(self._connection.query, sentence, **kwargs)
            self._result = await asyncio.get_running_loop().run_in_executor(
                None,
                fn
            )
            if not self._result:
                raise NoDataFound("InfluxDB: No Data was Found")
                return [None, "InfluxDB: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    async def queryrow(self, sentence="", **kwargs):
        # self._logger.debug("Start Query function")
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            print(startTime)
            params = {
                "chunked": True,
                "chunk_size": 1
            }
            fn = partial(self._connection.query, sentence, **params, **kwargs)
            self._result = await asyncio.get_running_loop().run_in_executor(
                None,
                fn
            )
            if not self._result:
                raise NoDataFound("InfluxDB: No Data was Found")
                return [None, "InfluxDB: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._result, error]

    async def execute(self, sentence="", method: str = 'GET', **kwargs):
        """Execute a transaction

        returns: results of the execution
        """
        error = None
        result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            result = self._connection.request(
                sentence,
                method,
                **kwargs
            )
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [result, error]


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
            result = self._loop.run_until_complete(
                self._connection.execute(sql)
            )
            if not result:
                print(result)
                return False
            else:
                return result
        except Exception as err:
            # print(sql)
            print(err)
            return False


"""
Registering this Provider
"""

registerProvider(influx)
