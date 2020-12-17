#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime

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

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.cluster import ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

class cassandra(BaseProvider):

    _provider = "cassandra"
    _syntax = "cql"
    _hosts: list = []
    _test_query = "SELECT release_version FROM system.local"
    _cluster = None
    _parameters = ()
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    use_cql: bool = False
    _auth = None

    def __init__(self, loop=None, pool=None, params={}, **kwargs):
        super(cassandra, self).__init__(loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)
        try:
            if 'host' in self._params:
                self._hosts = self._params['host'].split(',')
        except Exception as err:
            self._hosts = ['127.0.0.1']
        print('HOSTS ', self._hosts)
        try:
            self._auth = {
                "username": self._params["username"],
                "password": self._params["password"]
            }
        except KeyError:
            pass

    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                    self._logger.debug("Closing Connection")
                    try:
                        self._connection.shutdown()
                    except Exception as err:
                        self._cluster.shutdown()
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

    async def connection(self, keyspace=None):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        self._cluster = None
        try:
            nodeprofile = ExecutionProfile(
                load_balancing_policy=DCAwareRoundRobinPolicy()
            )
            profiles = {
                "node1": nodeprofile
            }
            params = {
                "port": self._params["port"],
                "compression": True,
                #"connection_class": AsyncioConnection,
            }
            print(params)
            auth_provider = None
            if self._auth:
                auth_provider = PlainTextAuthProvider(
                    **self._auth
                )
            print(self._auth, auth_provider)
            self._cluster = Cluster(
                self._hosts,
                auth_provider=auth_provider,
                execution_profiles=profiles,
                **params
            )
            print(self._cluster)
            if self._cluster:
                self._connection = self._cluster.connect(
                    keyspace=keyspace
                )
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError(
                "connection Error, Terminated: {}".format(str(err))
            )
        finally:
            return self._connection

    async def test_connection(self):
        result = None
        error = None
        try:
            response = self._connection.execute(self._test_query)
            result = [row for row in response]
        except Exception as err:
            error = err
        finally:
            return [result, error]

    def prepared_statement(self):
        return self._prepared

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
                raise StatementError(error)
            except Exception as err:
                error = "Unknown Error: {}".format(str(err))
                raise ProviderError(error)
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
            raise ProviderError(error)
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
            raise ProviderError(error)
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

registerProvider(cassandra)
