#!/usr/bin/env python3

import asyncio
import json
import time
from datetime import datetime
import pytz
from typing import List, Dict, Optional, Any, Union

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

from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import (
    dict_factory,
    named_tuple_factory,
    ordered_dict_factory,
    tuple_factory,
    ConsistencyLevel,
    PreparedStatement,
    SimpleStatement,
)


class cassandra(BaseProvider):

    _provider = "cassandra"
    _syntax = "cql"
    _hosts: list = []
    _test_query = "SELECT release_version FROM system.local"
    _cluster = None
    _parameters = ()
    _prepared: PreparedStatement = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    use_cql: bool = False
    _auth = None
    _timeout = 15

    def __init__(self, loop=None, pool=None, params={}, **kwargs):
        super(cassandra, self).__init__(loop=loop, params=params, **kwargs)
        asyncio.set_event_loop(self._loop)
        try:
            if "host" in self._params:
                self._hosts = self._params["host"].split(",")
        except Exception as err:
            self._hosts = ["127.0.0.1"]
        # print('HOSTS ', self._hosts)
        try:
            self._auth = {
                "username": self._params["username"],
                "password": self._params["password"],
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
                        "Connection Error, Terminated: {}".format(str(err))
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
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                request_timeout=self._timeout,
                row_factory=dict_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            profiles = {EXEC_PROFILE_DEFAULT: nodeprofile}
            params = {
                "port": self._params["port"],
                "compression": True,
                # "connection_class": AsyncioConnection,
                "protocol_version": 3,
            }
            # print(params)
            auth_provider = None
            if self._auth:
                auth_provider = PlainTextAuthProvider(**self._auth)
            # print(self._auth, auth_provider)
            self._cluster = Cluster(
                self._hosts,
                auth_provider=auth_provider,
                execution_profiles=profiles,
                **params,
            )
            # print(self._cluster)
            if self._cluster:
                self._connection = self._cluster.connect(keyspace=keyspace)
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if 'database' in self._params:
                self.use(self._params["database"])
        except Exception as err:
            print(err)
            self._connection = None
            self._cursor = None
            raise ProviderError("connection Error, Terminated: {}".format(str(err)))
        finally:
            return self

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

    def use(self, dbname: str):
        try:
            self._connection.execute(f"USE {dbname!s}")
        except Exception as err:
            print(err)
            logging.error(err)
            raise
        return self

    """
    Preparing a sentence
    """

    def prepared_statement(self):
        return self._prepared

    def prepared_smt(self):
        return self._prepared

    async def prepare(self, sentence=""):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._prepared = self._connection.prepare(sentence)
            self._prepared.consistency_level = ConsistencyLevel.QUORUM
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            print(error)
            return [self._prepared, error]

    def create_query(self, sentence: str):
        return SimpleStatement(sentence, consistency_level=ConsistencyLevel.QUORUM)

    async def query(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
    ):
        error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            startTime = datetime.now()
            self._result = self._connection.execute(sentence, params)
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
                return [None, "Cassandra: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self._generated = datetime.now() - startTime
            return [self._result, error]

    async def fetch(self, sentence, params: List = []):
        return self.query(sentence, params)

    async def queryrow(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
    ):
        error = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
                return [None, "Cassandra: No Data was Found"]
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        return [self._result, error]

    async def fetchrow(self, sentence, params: List = []):
        return self.queryrow(sentence=sentence, params=params)

    async def execute(self, sentence, params: List = []):
        """Execute a transaction
        get a CQL sentence and execute
        returns: results of the execution
        """
        error = None
        self._result = None
        if not sentence:
            raise EmptyStatement("Sentence is an empty string")
        if not self._connection:
            await self.connection()
        try:
            self._result = self._connection.execute(sentence, params)
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
                return [None, "Cassandra: No Data was Found"]
            return [self._result, None]
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [self._result, error]


"""
Registering this Provider
"""

registerProvider(cassandra)
