#!/usr/bin/env python3
import time
import logging
from datetime import datetime
from typing import Dict, List, Union
from asyncdb.exceptions import (
    EmptyStatement,
    NoDataFound,
    ProviderError,
)
from asyncdb.providers import (
    BaseProvider,
)

from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import (
    dict_factory,
    ConsistencyLevel,
    PreparedStatement,
    SimpleStatement,
)


class cassandra(BaseProvider):
    _provider = "cassandra"
    _syntax = "cql"

    def __init__(
            self,
            dsn: str = '',
            loop=None,
            params: Dict = None,
            **kwargs
    ):
        self.hosts: list = []
        self._test_query = "SELECT release_version FROM system.local"
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._cluster = None
        super(cassandra, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        try:
            if "host" in self._params:
                self._hosts = self._params["host"].split(",")
        except KeyError:
            self._hosts = ["127.0.0.1"]
        try:
            self.whitelist = kwargs['whitelist']
        except KeyError:
            self.whitelist = None
        try:
            self._auth = {
                "username": self._params["username"],
                "password": self._params["password"],
            }
        except KeyError:
            self._auth = None

    async def close(self, timeout: int = 10):
        """close.
        Closing a Connection
        """
        try:
            # gracefully closing underlying connection
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
            if self.whitelist:
                policy = WhiteListRoundRobinPolicy(self.whitelist)
            else:
                policy = DCAwareRoundRobinPolicy()
            nodeprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=dict_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            profiles = {EXEC_PROFILE_DEFAULT: nodeprofile}
            params = {
                "port": self._params["port"],
                "compression": True,
                "connection_class": AsyncioConnection,
                "protocol_version": 3,
            }
            print(params)
            auth_provider = None
            if self._auth:
                auth_provider = PlainTextAuthProvider(**self._auth)
            self._cluster = Cluster(
                self._hosts,
                auth_provider=auth_provider,
                execution_profiles=profiles,
                **params,
            )
            print(self._cluster)
            if self._cluster:
                self._connection = self._cluster.connect(keyspace=keyspace)
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if 'database' in self._params:
                self.use(self._params["database"])
        except Exception as err:
            logging.exception(f"connection Error, Terminated: {err}")
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
            logging.exception(err)
            raise
        return self

    """
    Preparing a sentence
    """

    def prepared_statement(self):
        return self._prepared

    def prepared_smt(self):
        return self._prepared

    async def prepare(self, sentence: str = ''):
        error = None
        await self.valid_operation(sentence)
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
        await self.valid_operation(sentence)
        try:
            self.start_timing()
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
            self.generated_at()
            return await self._serializer(self._result, error)

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
